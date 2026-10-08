"""
Daily runner: the one command a scheduler (systemd timer / cron / dagster) calls.

    python -m hl_poc.daily --wallets-file /etc/hypercore/wallets.txt --iceberg --s3-backfill

Order of operations:
  1. load .env + logging; take the single-flight lock (HL_OUT_DIR/hypercore/.daily.lock)
  2. extract.run(...) over the wallet list — incremental (per-wallet watermark, 1h
     overlap), HIP-3 dexes, today's l4x snapshot (replace-partition), S3 backfill
     for API-capped wallets, lift into Iceberg, staged-CSV retention after a
     successful lift
  3. build the run summary (hard problems vs warnings — see ops.build_summary),
     write _run_summary.json, append _run_history.jsonl, POST the alert webhook

Exit codes: 0 clean · 1 problems or error (details in _run_summary.json) · 2 bad
configuration (wallet list missing/empty) · 3 another run holds the lock.
The dagster asset template in deploy/dagster_hypercore_assets.py wraps this.
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from . import config, extract, ops
from . import schema as S

log = logging.getLogger("hl_poc.daily")


def read_wallets(path: Path) -> list[str]:
    out = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.split("#")[0].strip()
        if line:
            out.append(line.lower())
    return out


def main(argv=None) -> int:
    config.load_env()
    config.setup_logging()
    ap = argparse.ArgumentParser(description="HyperCore daily extract for a wallet list")
    ap.add_argument("--wallets-file", default=config.env("HL_WALLETS_FILE") or "wallets.txt")
    ap.add_argument("--days", type=int, default=2, help="window for a wallet with no watermark yet")
    ap.add_argument("--iceberg", action="store_true", help="lift into Iceberg (NESSIE_URI or local catalog)")
    ap.add_argument("--s3-backfill", action="store_true", help="complete API-capped windows from S3 node data")
    ap.add_argument("--with-prices", action="store_true", help="also run the L3 price job inline")
    ap.add_argument("--workers", type=int, default=4, help="concurrent Info API calls per wallet")
    ap.add_argument("--retention-days", type=int, default=config.env_int("HL_RETENTION_DAYS"),
                    help="prune staged l1/l3 CSV rows older than this after a successful lift (0 = never)")
    ap.add_argument("--out", default=str(config.out_root()))
    ap.add_argument("--no-alert", action="store_true", help="never POST the webhook")
    args = ap.parse_args(argv)

    wallets_path = Path(args.wallets_file)
    if not wallets_path.is_file():
        log.error("wallet list not found: %s", wallets_path)
        return 2
    wallets = read_wallets(wallets_path)
    if not wallets:
        log.error("no wallets in %s", wallets_path)
        return 2
    out_dir = Path(args.out)
    ns_dir = out_dir / S.NS_HYPERCORE
    options = {k: (str(v) if isinstance(v, Path) else v) for k, v in vars(args).items()}

    started = ops.utcnow()
    recon, error = None, None
    try:
        with ops.RunLock(ns_dir / ".daily.lock"):
            recon = extract.run(wallets, args.days, out_dir, with_prices=args.with_prices,
                                max_workers=args.workers, s3_backfill=args.s3_backfill,
                                iceberg=args.iceberg, incremental=True,
                                retention_days=args.retention_days)
    except ops.LockHeld as e:
        log.error("%s", e)
        return 3
    except Exception as e:  # noqa: BLE001 — summarise and alert; never die silently
        log.exception("daily run failed")
        error = f"{type(e).__name__}: {str(e)[:300]}"

    summary = ops.build_summary(recon, wallets, started, ops.utcnow(), error=error, options=options)
    if not args.no_alert and (not summary["ok"] or config.env_bool("HL_ALERT_ON_SUCCESS")):
        summary["alert_sent"] = ops.send_alert(summary)
    path = ops.write_summary(ns_dir, summary)
    log.info("daily done: ok=%s wallets ok=%d/%d problems=%s warnings=%d summary=%s",
             summary["ok"], summary["wallets_ok"], summary["wallets_total"],
             summary["problems"], len(summary["warnings"]), path)
    return 0 if summary["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
