"""
One command for "is it running, is it stuck, was the last run fine?":

    python -m blueblocx_hyperliquid.status                 # human-readable
    python -m blueblocx_hyperliquid.status --json          # for scripts / an external monitor
    exit 0 = healthy, 1 = something to look at

Reads HL_OUT_DIR/hypercore/_run_progress.json (live: phase, wallet i/N, last
update), _run_summary.json (last verdict), _run_history.jsonl (recent verdicts)
and, where systemctl exists, the unit and timer state.

Liveness of a run: the pid in the progress file is checked via /proc (same host),
falling back to the systemd state. NB a Type=oneshot unit is ActiveState=activating
(SubState start-pre / start) for its whole run and is never "active". A run is
STUCK when alive but the progress file has not moved for HL_STUCK_MINUTES (15 by
default: one first-time full-ledger lookup on an old whale is ~9 min, one S3 hour
file ~90 s, the lift a few minutes). The last run is STALE when it finished more
than --max-age-hours ago: default 2 h (two missed hourly ticks), 26 h when only
hypercore-daily.timer is active; HL_STATUS_MAX_AGE_HOURS overrides (cron/Docker
schedules, where no timer can be read).
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import shutil
import socket
import subprocess
import sys
from pathlib import Path

from . import config, ops
from . import schema as S

UNIT = "hypercore-daily.service"
TIMERS = ("hypercore-hourly.timer", "hypercore-daily.timer", "hypercore-maintenance.timer")
DATA_TIMERS = ("hypercore-hourly.timer", "hypercore-daily.timer")
_ALIVE_STATES = ("active", "activating", "deactivating", "reloading")
_DEAD_STATES = ("inactive", "failed")


def _parse_iso(value) -> dt.datetime | None:
    try:
        d = dt.datetime.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None
    return d if d.tzinfo else d.replace(tzinfo=dt.timezone.utc)


def _age_min(value, now: dt.datetime) -> float | None:
    d = _parse_iso(value)
    return None if d is None else (now - d).total_seconds() / 60.0


def systemd_state(names=(UNIT, *TIMERS), run=None) -> dict[str, dict]:
    """{unit: {ActiveState, SubState, Result, ExecMainStartTimestamp, NextElapseUSecRealtime}}
    via `systemctl show`; {} when systemctl is unavailable (Windows, containers)."""
    if run is None:
        if not shutil.which("systemctl"):
            return {}
        run = subprocess.run
    out: dict[str, dict] = {}
    for name in names:
        try:
            r = run(["systemctl", "show", name, "-p",
                     "ActiveState,SubState,Result,ExecMainStartTimestamp,NextElapseUSecRealtime,LoadState"],
                    capture_output=True, text=True, timeout=5)
        except Exception:  # noqa: BLE001
            continue
        props = dict(line.split("=", 1) for line in (r.stdout or "").splitlines() if "=" in line)
        if props.get("LoadState") not in (None, "not-found"):
            out[name] = props
    return out


def process_alive(progress: dict | None) -> bool | None:
    """True/False via /proc for a progress file written on this host, else None."""
    if not progress or progress.get("state") != "running":
        return None
    if progress.get("host") not in (None, socket.gethostname()):      # same rule as RunLock._abandoned
        return None
    return ops.pid_alive(progress.get("pid"))


def read_history(ns_dir: Path, last: int = 24) -> list[dict]:
    p = ns_dir / ops.HISTORY_FILE
    try:
        lines = p.read_text(encoding="utf-8").splitlines()
    except OSError:
        return []
    rows = []
    for line in lines[-last:]:
        try:
            rows.append(json.loads(line))
        except ValueError:
            continue
    return rows


def default_max_age_h(units: dict | None) -> float:
    """2 h on the hourly timer; 26 h (24 h period + 2 h grace) when only the daily timer is active."""
    units = units or {}

    def active(name):
        return (units.get(name) or {}).get("ActiveState") == "active"
    return 26.0 if active("hypercore-daily.timer") and not active("hypercore-hourly.timer") else 2.0


def assess(progress: dict | None, summary: dict | None, history: list[dict], now: dt.datetime, *,
           units: dict | None = None, max_age_h: float | None = None, stuck_min: float = 15.0,
           alive: bool | None = None) -> dict:
    """The verdict, as data. `issues` non-empty = unhealthy.

    alive: process_alive(progress) - True/False when /proc could judge the pid, else
    None; the systemd ActiveState is the fallback signal. Only a run whose process is
    known to be gone is reported as died; an alive run that stopped updating the
    progress file for stuck_min is STUCK."""
    issues: list[str] = []
    units = units or {}
    unit = units.get(UNIT) or {}
    if max_age_h is None:
        max_age_h = default_max_age_h(units)
    running = bool(progress) and progress.get("state") == "running"
    report: dict = {"now": now.isoformat(timespec="seconds"), "running": running, "unit": unit or None,
                    "max_age_h": max_age_h, "stuck_min": stuck_min}

    if running:
        age = _age_min(progress.get("updated_at"), now)
        started_age = _age_min(progress.get("started_at"), now)
        report["progress"] = {k: progress.get(k) for k in
                              ("phase", "detail", "wallets_done", "wallets_total", "wallets_deferred", "analysed",
                               "analysed_total", "current", "started_at", "updated_at", "deadline_at", "pid")}
        report["progress"]["minutes_since_update"] = None if age is None else round(age, 1)
        report["progress"]["minutes_running"] = None if started_age is None else round(started_age, 1)
        unit_alive = (unit.get("ActiveState") in _ALIVE_STATES) if unit.get("ActiveState") else None
        if unit.get("ActiveState") in _DEAD_STATES:
            unit_alive = False
        is_alive = alive if alive is not None else unit_alive
        report["progress"]["alive"] = is_alive
        if is_alive is False:
            where = f" and {UNIT} is {unit.get('ActiveState')}" if unit.get("ActiveState") else ""
            issues.append(f"progress file says running but pid {progress.get('pid')} is gone{where}: "
                          f"the run died without a summary")
        elif age is not None and age > stuck_min:
            detail = f", {progress.get('detail')}" if progress.get("detail") else ""
            issues.append(f"STUCK? no progress for {age:.0f} min in phase {progress.get('phase')}{detail} "
                          f"(wallet {progress.get('wallets_done')}/{progress.get('wallets_total')}, "
                          f"current {str(progress.get('current'))[:10]}); the service timeout kills it if it stays quiet")
    elif progress:
        report["progress"] = {"state": progress.get("state"), "phase": progress.get("phase"),
                              "finished_at": progress.get("finished_at")}

    if summary:
        age = _age_min(summary.get("finished_at"), now)
        report["last_run"] = {k: summary.get(k) for k in
                              ("run_at", "finished_at", "duration_s", "ok", "wallets_total", "wallets_ok",
                               "wallets_deferred", "problems", "warnings", "version")}
        report["last_run"]["tracked"] = (summary.get("tracked") or {}).get("source")
        report["last_run"]["minutes_ago"] = None if age is None else round(age, 1)
        if not summary.get("ok"):
            probs = summary.get("problems") or []
            issues.append(f"last run had {len(probs)} problem(s): " + ", ".join(probs[:5])
                          + (" ..." if len(probs) > 5 else ""))
        if age is not None and age > max_age_h * 60 and not running:
            issues.append(f"STALE: last run finished {age / 60:.1f} h ago (limit {max_age_h:g} h) - "
                          f"timer disabled, preflight failing, or the host was down")
    elif not running:
        issues.append("no run summary yet - nothing has completed")

    if history:
        oks = sum(1 for r in history if r.get("ok"))
        report["history"] = {"runs": len(history), "ok": oks,
                             "last_failed_at": next((r.get("finished_at") for r in reversed(history)
                                                     if not r.get("ok")), None)}
    timers = {n: {"active": p.get("ActiveState"), "next": p.get("NextElapseUSecRealtime")}
              for n, p in units.items() if n.endswith(".timer")}
    if timers:
        report["timers"] = timers
        if not any(t["active"] == "active" for n, t in timers.items() if n in DATA_TIMERS):
            issues.append("no data timer is active (hypercore-hourly.timer / hypercore-daily.timer)")
    report["issues"] = issues
    report["healthy"] = not issues
    return report


def format_report(r: dict) -> str:
    lines = [f"hypercore status at {r['now']}: {'HEALTHY' if r['healthy'] else 'ATTENTION'}"]
    p = r.get("progress")
    if r.get("running") and p:
        extra = ""
        if p.get("phase") == "analyse" and p.get("analysed") is not None:
            extra = f", analysed {p.get('analysed')}/{p.get('analysed_total')}"
        if p.get("detail"):
            extra += f", {p['detail']}"
        lines.append(f"  running : phase {p.get('phase')}, wallet {p.get('wallets_done')}/{p.get('wallets_total')}"
                     f"{' (deferred ' + str(p.get('wallets_deferred')) + ')' if p.get('wallets_deferred') else ''}{extra}, "
                     f"{p.get('minutes_running')} min in, last update {p.get('minutes_since_update')} min ago"
                     f"{', deadline ' + str(p.get('deadline_at')) if p.get('deadline_at') else ''}")
    else:
        lines.append("  running : no")
    lr = r.get("last_run")
    if lr:
        lines.append(f"  last run: {'ok' if lr.get('ok') else 'PROBLEMS'} {lr.get('minutes_ago')} min ago, "
                     f"{lr.get('duration_s')} s, wallets ok {lr.get('wallets_ok')}/{lr.get('wallets_total')}"
                     f"{', deferred ' + str(lr.get('wallets_deferred')) if lr.get('wallets_deferred') else ''}, "
                     f"tracked {lr.get('tracked')}, v{lr.get('version')}")
        if lr.get("problems"):
            lines.append("            problems: " + ", ".join(lr["problems"][:8]))
        if lr.get("warnings"):
            lines.append("            warnings: " + ", ".join(lr["warnings"][:8]))
    else:
        lines.append("  last run: none yet")
    h = r.get("history")
    if h:
        lines.append(f"  history : {h['ok']}/{h['runs']} recent runs ok"
                     + (f", last failed {h['last_failed_at']}" if h.get("last_failed_at") else ""))
    for n, t in (r.get("timers") or {}).items():
        lines.append(f"  timer   : {n} {t['active']}" + (f", next {t['next']}" if t.get("next") else ""))
    for issue in r["issues"]:
        lines.append(f"  !! {issue}")
    return "\n".join(lines)


def main(argv=None) -> int:
    config.load_env()
    ap = argparse.ArgumentParser(description="HyperCore pipeline status: running / stuck / last verdict")
    ap.add_argument("--out", default=str(config.out_root()))
    ap.add_argument("--max-age-hours", type=float, default=None,
                    help="last run older than this = STALE (default: HL_STATUS_MAX_AGE_HOURS, else 2 on the hourly "
                         "timer / 26 when only hypercore-daily.timer is active)")
    ap.add_argument("--stuck-minutes", type=float, default=config.env_float("HL_STUCK_MINUTES"))
    ap.add_argument("--no-systemd", action="store_true", help="do not query systemctl")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args(argv)
    ns_dir = Path(args.out) / S.NS_HYPERCORE
    units = {} if args.no_systemd else systemd_state()
    max_age = args.max_age_hours
    if max_age is None and config.env("HL_STATUS_MAX_AGE_HOURS"):
        max_age = config.env_float("HL_STATUS_MAX_AGE_HOURS")
    progress = ops.read_progress(ns_dir)
    report = assess(progress, ops.read_json(ns_dir / ops.SUMMARY_FILE), read_history(ns_dir), ops.utcnow(),
                    units=units, max_age_h=max_age, stuck_min=args.stuck_minutes, alive=process_alive(progress))
    print(json.dumps(report, indent=2, default=str) if args.json else format_report(report))
    return 0 if report["healthy"] else 1


if __name__ == "__main__":
    sys.exit(main())
