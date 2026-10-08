"""
Operational plumbing around the scheduled run: atomic state writes, a
single-flight lock, the run summary (hard problems vs warnings) and the alert
webhook. No business logic lives here — daily.py wraps extract.run with these.
"""
from __future__ import annotations

import datetime as dt
import json
import logging
import os
import socket
import time
from pathlib import Path

from . import __version__, config

log = logging.getLogger("hl_poc.ops")

SUMMARY_FILE = "_run_summary.json"      # latest run, full detail
HISTORY_FILE = "_run_history.jsonl"     # one compact line per run, append-only


def utcnow() -> dt.datetime:
    return dt.datetime.now(tz=dt.timezone.utc)


def atomic_write_text(path: Path, text: str) -> None:
    """Sibling temp file + os.replace: a crash mid-write never leaves a half-written
    state file (staged CSV, watermarks, summaries) behind."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


# --- single-flight lock ------------------------------------------------------------

class LockHeld(RuntimeError):
    """Another run holds the lock."""


class RunLock:
    """Single-flight guard for the daily run.

    Atomic O_EXCL create of a small JSON lock file. A lock older than
    HL_LOCK_STALE_HOURS is treated as left behind by a crashed run and replaced.
    PID liveness is deliberately NOT probed (os.kill(pid, 0) terminates the target
    on Windows); the stale-age rule is enough for a once-a-day job, and systemd
    timers never overlap a running service anyway — cron does, hence the lock.
    """

    def __init__(self, path: Path, stale_hours: float | None = None):
        self.path = Path(path)
        hours = float(config.env("HL_LOCK_STALE_HOURS")) if stale_hours is None else stale_hours
        self.stale_s = hours * 3600.0

    def _holder(self) -> str:
        try:
            return self.path.read_text(encoding="utf-8").strip()[:200]
        except OSError:
            return "?"

    def __enter__(self) -> "RunLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        for attempt in (1, 2):
            try:
                fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            except FileExistsError:
                try:
                    age = time.time() - self.path.stat().st_mtime
                except FileNotFoundError:
                    continue                         # released between our calls: retry once
                if attempt == 1 and age > self.stale_s:
                    log.warning("stale lock %s (%.1f h old) removed", self.path, age / 3600)
                    self.path.unlink(missing_ok=True)
                    continue
                raise LockHeld(f"another run holds {self.path}: {self._holder()}")
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump({"pid": os.getpid(), "host": socket.gethostname(),
                           "started_at": utcnow().isoformat()}, fh)
            return self
        raise LockHeld(f"could not acquire {self.path}")

    def __exit__(self, *exc) -> bool:
        self.path.unlink(missing_ok=True)
        return False


# --- run summary --------------------------------------------------------------------

# HARD problems fail the run (exit 1 + alert):
#   natural_key        chain-wide (tx_hash, event_index, direction) collision — nothing written
#   skipped / missing  a wallet's core pulls failed, or it never produced a result
#   fidelity           mapped rows != raw API input (dropped / duplicated / sign-flipped rows)
#   perp_oracle        perp equity != HL portfolio.perpAllTime (or the oracle could not be read)
#   networth_oracle    computed net worth != HL portfolio.allTime
#   fills_truncated    the window is incomplete (API cap without a complete S3 backfill)
#   fetch_errors       a non-core pull failed (staking / vaults / open orders / a HIP-3 dex)
#   error              the run raised
# WARNINGS are reported and alerted with the summary but do not fail the run:
#   pnl_oracle         HL perpDay.pnlHistory Δ24h != realized + Δunrealized (still being
#                      calibrated on live data; promote to hard once it is stable)
#   unmapped_ledger_types   a ledger delta.type the mapper does not know (rows skipped)
#   unpriced           a held non-stable symbol with no spot price (net worth understated)


def build_summary(recon: dict | None, wallets: list[str], started_at: dt.datetime,
                  finished_at: dt.datetime, *, error: str | None = None,
                  options: dict | None = None) -> dict:
    """Turn extract.run's reconciliation into the operator-facing verdict."""
    by = (recon or {}).get("by_wallet", {}) or {}
    problems: list[str] = []
    warnings: list[str] = []
    per_wallet: dict[str, dict] = {}
    if error:
        problems.append(f"error:{error}")
    if recon is not None and not recon.get("natural_key_ok", True):
        problems.append("natural_key")

    for w in wallets:
        r = by.get(w) or {}
        short = w[:10]
        if not r:
            per_wallet[w] = {"status": "missing"}
            if recon is not None:
                problems.append(f"{short}:missing")
            continue
        if r.get("skipped"):
            per_wallet[w] = {"status": "skipped", "errors": r.get("errors")}
            problems.append(f"{short}:skipped")
            continue
        oracle = r.get("oracle") or {}
        pnl = oracle.get("pnl") or {}
        entry = {
            "status": "ok",
            "fidelity_ok": bool(r.get("fidelity_ok")),
            "perp_oracle": oracle.get("account_value_matches"),
            "networth_oracle": oracle.get("networth_matches"),
            "pnl_oracle": (pnl.get("pnl_matches") if pnl.get("checked") else None),
            "fills_truncated": bool(r.get("fills_truncated_at_10k_cap")),
            "fetch_errors": sorted((r.get("fetch_errors") or {}).keys()),
            "l1_rows": r.get("l1_ledger_rows"),
            "raw_fills": r.get("raw_fills"),
            "open_positions": r.get("open_positions"),
            "lending_lazy_pull": bool(r.get("lending_lazy_pull")),
            "unmapped_ledger_types": r.get("unmapped_ledger_types") or {},
            "unpriced_held_symbols": r.get("unpriced_held_symbols") or [],
        }
        hard: list[str] = []
        if not entry["fidelity_ok"]:
            hard.append("fidelity")
        if entry["perp_oracle"] is not True:
            hard.append("perp_oracle")
        if entry["networth_oracle"] is False:
            hard.append("networth_oracle")
        if entry["fills_truncated"]:
            hard.append("fills_truncated")
        if entry["fetch_errors"]:
            hard.append("fetch_errors=" + ",".join(entry["fetch_errors"]))
        if entry["pnl_oracle"] is False:
            warnings.append(f"{short}:pnl_oracle")
        if entry["unmapped_ledger_types"]:
            warnings.append(f"{short}:unmapped_ledger_types=" + ",".join(sorted(entry["unmapped_ledger_types"])))
        if entry["unpriced_held_symbols"]:
            warnings.append(f"{short}:unpriced=" + ",".join(entry["unpriced_held_symbols"]))
        if hard:
            entry["status"] = "problem"
            problems += [f"{short}:{p}" for p in hard]
        per_wallet[w] = entry

    statuses = [e["status"] for e in per_wallet.values()]
    ok = recon is not None and not problems
    return {
        "version": __version__,
        "host": socket.gethostname(),
        "run_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "duration_s": round((finished_at - started_at).total_seconds(), 1),
        "ok": ok,
        "wallets_total": len(wallets),
        "wallets_ok": statuses.count("ok"),
        "wallets_problem": statuses.count("problem"),
        "wallets_skipped": statuses.count("skipped") + statuses.count("missing"),
        "problems": problems,
        "warnings": warnings,
        "error": error,
        "natural_key_ok": (recon or {}).get("natural_key_ok"),
        "iceberg": (recon or {}).get("iceberg"),
        "retention": (recon or {}).get("retention"),
        "hip3_dexes": (recon or {}).get("hip3_dexes_checked"),
        "unmapped_ledger_types": (recon or {}).get("unmapped_ledger_types") or {},
        "options": options or {},
        "per_wallet": per_wallet,
    }


def compact_summary(summary: dict) -> dict:
    """The history line: everything but the per-wallet detail (statuses only)."""
    out = {k: v for k, v in summary.items() if k not in ("per_wallet", "iceberg", "options")}
    out["per_wallet_status"] = {w: e.get("status") for w, e in (summary.get("per_wallet") or {}).items()}
    return out


def write_summary(ns_dir: Path, summary: dict) -> Path:
    ns_dir = Path(ns_dir)
    latest = ns_dir / SUMMARY_FILE
    atomic_write_text(latest, json.dumps(summary, indent=2, default=str))
    with (ns_dir / HISTORY_FILE).open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(compact_summary(summary), default=str) + "\n")
    return latest


# --- alerting ------------------------------------------------------------------------

def format_alert_text(summary: dict) -> str:
    state = "OK" if summary.get("ok") else "PROBLEMS"
    lines = [f"[hypercore daily] {state} — {summary.get('wallets_ok', 0)}/{summary.get('wallets_total', 0)} "
             f"wallets clean in {summary.get('duration_s', 0)}s on {summary.get('host', '?')} "
             f"(run {summary.get('run_at', '?')})"]
    probs = summary.get("problems") or []
    warns = summary.get("warnings") or []
    if probs:
        lines.append("problems: " + ", ".join(probs[:20]) + (f" … +{len(probs) - 20}" if len(probs) > 20 else ""))
    if warns:
        lines.append("warnings: " + ", ".join(warns[:20]) + (f" … +{len(warns) - 20}" if len(warns) > 20 else ""))
    return "\n".join(lines)


def send_alert(summary: dict, url: str | None = None, timeout: float = 10.0, post=None) -> bool:
    """POST the summary to a Slack/Discord-style incoming webhook. Returns True when
    the hook accepted it. Never logs the URL (it is a secret)."""
    url = url or config.env("HL_ALERT_WEBHOOK_URL")
    if not url:
        return False
    text = format_alert_text(summary)
    payload = {
        "text": text,                 # Slack
        "content": text[:1900],       # Discord (2000-char cap)
        "hypercore": {"ok": summary.get("ok"), "run_at": summary.get("run_at"),
                      "problems": (summary.get("problems") or [])[:50],
                      "warnings": (summary.get("warnings") or [])[:50]},
    }
    if post is None:
        import requests
        post = requests.post
    try:
        resp = post(url, json=payload, timeout=timeout)
        ok = 200 <= int(getattr(resp, "status_code", 0)) < 300
        if not ok:
            log.warning("alert webhook returned HTTP %s", getattr(resp, "status_code", "?"))
        return ok
    except Exception as e:  # noqa: BLE001 — an alert failure must not fail the run
        log.warning("alert webhook failed: %s", type(e).__name__)
        return False
