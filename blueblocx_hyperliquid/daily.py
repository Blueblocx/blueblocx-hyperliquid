"""
Scheduled runner: the one command a timer (systemd / cron / dagster) calls, daily
or hourly.

    python -m blueblocx_hyperliquid.daily --wallets-file /etc/hypercore/wallets.txt --iceberg [--s3-backfill]

Order of operations:
  1. load .env + logging; install a SIGTERM handler (a systemd timeout must still
     release the lock and write a summary); take the single-flight lock
     (HL_OUT_DIR/hypercore/.daily.lock)
  2. decide the wallet list (tracked.py): the pinned wallets from --wallets-file
     first, then - with HL_TRACKED_SOURCE=leaderboard - the leaderboard tracked set
     (top whales by account value + smart money, the same definition the TS
     platform-backend sweeps live), refreshed now or taken from the cache, rotated
     by the saved cursor
  3. extract.run(...) over that list - incremental (per-wallet watermark, 1h
     overlap), HIP-3 dexes, today's l4x snapshot (replace-partition), S3 backfill
     for API-capped wallets, lift into Iceberg, staged-CSV retention (once a day);
     with HL_RUN_SOFT_DEADLINE_MIN the tail of the list is deferred to the next run
     instead of the service timeout killing everything
  4. build the run summary (hard problems vs warnings - see ops.build_summary),
     POST the alert webhook (deduplicated: an unchanged problem set repeats only
     every HL_ALERT_REPEAT_HOURS), write _run_summary.json, append _run_history.jsonl,
     ping HL_HEARTBEAT_URL (or its /fail variant)
  Throughout, _run_progress.json records the phase and the wallet being pulled;
  `python -m blueblocx_hyperliquid.status` reads it to answer "running, stuck, or fine?".

Exit codes: 0 clean · 1 problems or error (details in _run_summary.json; includes a
tracked set that could not be refreshed) · 2 bad configuration (wallet list
missing/empty with no tracked source, unknown HL_TRACKED_SOURCE) · 3 another run
holds the lock (also summarised and alerted, so a stuck lock is never silent).
The dagster asset template in deploy/dagster_hypercore_assets.py wraps this.
"""
from __future__ import annotations

import argparse
import datetime as dt
import logging
import signal
import sys
from pathlib import Path

from . import config, extract, ops, tracked
from . import schema as S

log = logging.getLogger("blueblocx_hyperliquid.daily")


class Terminated(BaseException):
    """SIGTERM/SIGINT received: unwind so the lock is released and a summary is written.
    A BaseException (like KeyboardInterrupt) so no `except Exception` on the way - the
    Info API retry loop, the leaderboard refresh, a guarded pull - can swallow it."""


def _raise_terminated(signum, _frame):
    raise Terminated(f"signal {signum}")


def read_wallets(path: Path) -> list[str]:
    out = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.split("#")[0].strip()
        if line:
            out.append(line.lower())
    return out


def _read_previous_summary(ns_dir: Path) -> dict | None:
    p = ns_dir / ops.SUMMARY_FILE
    if not p.exists():
        return None
    try:
        import json
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return None


def main(argv=None) -> int:
    config.load_env()
    config.setup_logging()
    ap = argparse.ArgumentParser(description="HyperCore scheduled extract for a wallet list")
    ap.add_argument("--wallets-file", default=config.env("HL_WALLETS_FILE") or "wallets.txt",
                    help="pinned wallets, always pulled first; optional with HL_TRACKED_SOURCE=leaderboard")
    ap.add_argument("--days", type=int, default=2, help="window for a wallet with no watermark yet")
    ap.add_argument("--iceberg", action="store_true", help="lift into Iceberg (REST / SQL / local catalog)")
    ap.add_argument("--s3-backfill", action="store_true", help="complete API-capped windows from S3 node data")
    ap.add_argument("--with-prices", action="store_true", help="also run the L3 price job inline")
    ap.add_argument("--workers", type=int, default=4, help="concurrent Info API calls per wallet")
    ap.add_argument("--retention-days", type=int, default=config.env_int("HL_RETENTION_DAYS"),
                    help="prune staged l1/l3/l4x CSV rows older than this after a successful lift (0 = never)")
    ap.add_argument("--deadline-min", type=float, default=config.env_float("HL_RUN_SOFT_DEADLINE_MIN"),
                    help="start no new wallet after this many minutes; the rest are deferred to the next run (0 = off)")
    ap.add_argument("--out", default=str(config.out_root()))
    ap.add_argument("--no-alert", action="store_true", help="never POST the webhook")
    args = ap.parse_args(argv)

    source = config.env("HL_TRACKED_SOURCE").strip().lower()
    if source not in ("", "leaderboard"):
        log.error("HL_TRACKED_SOURCE=%r - supported: leaderboard (or empty = wallets file only)", source)
        return 2
    wallets_path = Path(args.wallets_file)
    pins: list[str] = []
    if wallets_path.is_file():
        pins = read_wallets(wallets_path)
    elif not source:
        log.error("wallet list not found: %s", wallets_path)
        return 2
    else:
        log.warning("no pins file at %s - tracked set only", wallets_path)
    if not pins and not source:
        log.error("no wallets in %s", wallets_path)
        return 2
    out_dir = Path(args.out)
    ns_dir = out_dir / S.NS_HYPERCORE
    options = {k: (str(v) if isinstance(v, Path) else v) for k, v in vars(args).items()}

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            signal.signal(sig, _raise_terminated)
        except (ValueError, OSError):      # not the main thread / unsupported platform
            pass

    started = ops.utcnow()
    recon, error, rc = None, None, 1
    lock_held = False
    wallets, auto, cursor, tracked_meta = list(dict.fromkeys(pins)), [], 0, None
    progress: ops.Progress | None = None
    try:
        with ops.RunLock(ns_dir / ".daily.lock"):
            progress = ops.Progress(ns_dir, planned=len(wallets))
            if source == "leaderboard":
                progress.update(phase="tracked-set")
                sets, tracked_meta = tracked.refresh_tracked(ns_dir)
                cursor = tracked.load_cursor(ns_dir)
                wallets, auto = tracked.plan_wallets(pins, sets, cursor)
                log.info("wallet plan: %d pinned + %d tracked (%d whales, %d smart; %s), cursor %d",
                         len(wallets) - len(auto), len(auto), len(sets.get("whales") or []),
                         len(sets.get("smart") or []), tracked_meta.get("source"), cursor)
                if not wallets:
                    raise RuntimeError(f"tracked set unavailable ({tracked_meta.get('error')}) "
                                       f"and no pinned wallets in {wallets_path}")
            deadline_ts = (started.timestamp() + args.deadline_min * 60.0
                           if args.deadline_min and args.deadline_min > 0 else None)
            progress.update(wallets_total=len(wallets),
                            deadline_at=(dt.datetime.fromtimestamp(deadline_ts, tz=dt.timezone.utc).isoformat()
                                         if deadline_ts else None))
            recon = extract.run(wallets, args.days, out_dir, with_prices=args.with_prices,
                                max_workers=args.workers, s3_backfill=args.s3_backfill,
                                iceberg=args.iceberg, incremental=True,
                                retention_days=args.retention_days, deadline_ts=deadline_ts,
                                soft_wallets=set(auto), progress=progress.update)
            if auto:
                tracked.save_cursor(ns_dir, tracked.advance_cursor(auto, cursor, set(recon.get("wallets") or [])))
    except ops.LockHeld as e:
        log.error("%s", e)
        error, lock_held, rc = f"LockHeld: {str(e)[:300]}", True, 3
    except Terminated as e:
        log.error("run terminated (%s) - lock released, nothing advanced", e)
        error = f"Terminated: {e}"
    except Exception as e:  # noqa: BLE001 — summarise and alert; never die silently
        log.exception("run failed")
        error = f"{type(e).__name__}: {str(e)[:300]}"

    summary = ops.build_summary(recon, wallets, started, ops.utcnow(), error=error, options=options,
                                deferred=(recon or {}).get("deferred"), soft_wallets=set(auto))
    if tracked_meta is not None:
        summary["tracked"] = {**tracked_meta, "pins": len(wallets) - len(auto), "auto": len(auto),
                              "planned": len(wallets), "cursor": cursor}
        if tracked_meta.get("source") == "cache":
            summary["warnings"].append(f"tracked_stale={tracked_meta.get('age_h')}h")
        elif tracked_meta.get("source") == "none" and recon is not None:
            summary["problems"].append("tracked_unavailable")
            summary["ok"] = False
    if lock_held:
        summary["lock_held"] = True
    previous = _read_previous_summary(ns_dir)
    if not args.no_alert and ops.should_alert(summary, previous):
        summary["alert_sent"] = ops.send_alert(summary)
        summary["alert_sent_at"] = ops.utcnow().isoformat()
    elif previous and not summary["ok"] and previous.get("alert_sent"):
        # carry the last alert time forward so the dedupe window keeps counting
        summary["alert_sent"] = False
        summary["alert_sent_at"] = previous.get("alert_sent_at")
        summary["alert_suppressed"] = True
    path = ops.write_summary(ns_dir, summary)
    if progress is not None:
        progress.finish(ok=summary["ok"], problems=len(summary["problems"]), error=error)
    hb_url = config.env("HL_HEARTBEAT_URL")
    if hb_url:
        log.info("heartbeat %s: %s", "ok" if summary["ok"] else "fail", ops.heartbeat(hb_url, summary["ok"]))
    log.info("run done: ok=%s wallets ok=%d/%d deferred=%d problems=%s warnings=%d summary=%s",
             summary["ok"], summary["wallets_ok"], summary["wallets_total"], summary["wallets_deferred"],
             summary["problems"], len(summary["warnings"]), path)
    if summary["ok"]:
        return 0
    return rc


if __name__ == "__main__":
    sys.exit(main())
