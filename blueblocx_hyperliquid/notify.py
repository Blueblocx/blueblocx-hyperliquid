"""
systemd OnFailure hook: alert when a unit failed WITHOUT the run reporting itself.

    OnFailure=hypercore-failure@%n.service          (in hypercore-daily.service etc.)
    ExecStart=... python -m blueblocx_hyperliquid.notify --failed-unit %i

daily.py writes _run_summary.json and POSTs the webhook on every outcome it can
see, including problems (exit 1) and SIGTERM. The cases it cannot see are what
this hook is for: preflight FAIL in ExecStartPre, a crash before the summary, a
SIGKILL after the stop timeout, a venv whose pipeline dependencies (pyarrow,
pyiceberg, boto3) no longer import. If the failed unit is the data run and its
summary was finished within --grace-minutes, the run did report itself and this
hook stays quiet, so a problem run never produces two messages. Any other unit
(hypercore-maintenance.service) writes no summary, so its failure always posts;
the heartbeat /fail ping is sent only for the data run, because the heartbeat
answers "did the data run happen". Always exits 0 (a notifier must never fail
the unit it reports on). A broken interpreter or `requests` takes this hook down
too - the heartbeat's missed ping is the signal for that.
"""
from __future__ import annotations

import argparse
import datetime as dt
import logging
import socket
import sys
from pathlib import Path

from . import config, ops
from . import schema as S

log = logging.getLogger("blueblocx_hyperliquid.notify")

# the units whose run writes _run_summary.json and alerts on its own
SELF_REPORTING_UNITS = ("hypercore-daily.service",)


def summary_age_s(summary: dict | None, now: dt.datetime) -> float | None:
    if not summary:
        return None
    try:
        finished = dt.datetime.fromisoformat(str(summary.get("finished_at")))
    except (TypeError, ValueError):
        return None
    if finished.tzinfo is None:
        finished = finished.replace(tzinfo=dt.timezone.utc)
    return (now - finished).total_seconds()


def failure_text(unit: str, host: str, now: dt.datetime, summary: dict | None, age_s: float | None, *,
                 self_reporting: bool = True) -> str:
    when = f"on {host} at {now.isoformat(timespec='seconds')}"
    if not self_reporting:
        return (f"[hypercore] systemd unit {unit} FAILED {when} (this unit writes no run summary). "
                f"Check: journalctl -u {unit} -n 50")
    last = ("no run summary exists yet" if summary is None or age_s is None
            else f"last summary {age_s / 60:.0f} min ago (ok={summary.get('ok')})")
    return (f"[hypercore] systemd unit {unit} FAILED {when} without a run summary - preflight FAIL, "
            f"crash or kill; {last}. Check: journalctl -u {unit} -n 50")


def main(argv=None, *, now: dt.datetime | None = None, post=None, get=None) -> int:
    config.load_env()
    config.setup_logging()
    ap = argparse.ArgumentParser(description="OnFailure notifier for the hypercore units")
    ap.add_argument("--failed-unit", required=True)
    ap.add_argument("--grace-minutes", type=float, default=5.0,
                    help="a data-run summary finished within this window means the run reported itself: stay quiet")
    ap.add_argument("--out", default=str(config.out_root()))
    args = ap.parse_args(argv)
    now = now or ops.utcnow()
    ns_dir = Path(args.out) / S.NS_HYPERCORE
    self_reporting = args.failed_unit in SELF_REPORTING_UNITS
    summary = ops.read_json(ns_dir / ops.SUMMARY_FILE) if self_reporting else None
    age_s = summary_age_s(summary, now)
    if self_reporting and age_s is not None and 0 <= age_s <= args.grace_minutes * 60:
        log.info("%s failed but the run wrote its summary %.0f s ago (ok=%s) - it already alerted; nothing to do",
                 args.failed_unit, age_s, summary.get("ok") if summary else None)
        return 0
    text = failure_text(args.failed_unit, socket.gethostname(), now, summary, age_s, self_reporting=self_reporting)
    log.error("%s", text)
    sent = ops.post_text(text, post=post)
    hb = ops.heartbeat(config.env("HL_HEARTBEAT_URL"), ok=False, get=get) if self_reporting else False
    log.info("notified: webhook=%s heartbeat_fail=%s", sent, hb)
    return 0


if __name__ == "__main__":
    sys.exit(main())
