"""
Web monitor: one page that answers "is it running, is it stuck, was the last run fine?"

    python -m blueblocx_hyperliquid.web                 # http://0.0.0.0:8787/
    HL_WEB_BIND / HL_WEB_PORT (or --bind / --port)

Standard library only (http.server), read-only, no authentication: serve it on the
LAN or a VPN address and keep the port off the internet. Routes:
    /               the page (auto-refreshes every 30 s)
    /api/status     the verdict as JSON (same data as `status --json`)
    /api/summary    the last run summary, /api/progress the live progress file,
    /api/history    the recent run history
    /healthz        200 "ok" when healthy, 503 with the issues otherwise - for any
                    external uptime checker you may add later
The same assess() as the CLI decides the verdict; the page is only a rendering of
_run_progress.json, _run_summary.json, _run_history.jsonl and the systemd state
(cached 10 s).
"""
from __future__ import annotations

import argparse
import html
import json
import logging
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from . import config, ops, status
from . import schema as S

log = logging.getLogger("blueblocx_hyperliquid.web")
REFRESH_S = 30
SYSTEMD_CACHE_S = 10.0


class Monitor:
    """Reads the small state files on every request; caches `systemctl show` briefly."""

    def __init__(self, ns_dir: Path, *, use_systemd: bool = True, max_age_h: float | None = None,
                 stuck_min: float = 15.0):
        self.ns_dir = Path(ns_dir)
        self.use_systemd = use_systemd
        self.max_age_h = max_age_h
        self.stuck_min = stuck_min
        self._units: dict = {}
        self._units_at = 0.0
        self._lock = threading.Lock()

    def units(self) -> dict:
        if not self.use_systemd:
            return {}
        with self._lock:
            if time.monotonic() - self._units_at > SYSTEMD_CACHE_S:
                self._units = status.systemd_state()
                self._units_at = time.monotonic()
            return self._units

    def progress(self) -> dict | None:
        return ops.read_progress(self.ns_dir)

    def summary(self) -> dict | None:
        return ops.read_json(self.ns_dir / ops.SUMMARY_FILE)

    def history(self, last: int = 24) -> list[dict]:
        return status.read_history(self.ns_dir, last=last)

    def report(self) -> dict:
        progress = self.progress()
        return status.assess(progress, self.summary(), self.history(), ops.utcnow(), units=self.units(),
                             max_age_h=self.max_age_h, stuck_min=self.stuck_min,
                             alive=status.process_alive(progress))


# --- rendering --------------------------------------------------------------------------

_CSS = """
body{font-family:system-ui,Segoe UI,Arial,sans-serif;margin:0;background:#f5f6f8;color:#1b1f23}
header{padding:14px 22px;color:#fff;font-size:20px;font-weight:600}
header.ok{background:#1f8a4c} header.bad{background:#b42318}
header small{font-weight:400;font-size:13px;opacity:.9;margin-left:14px}
main{padding:18px 22px;max-width:1200px}
section{background:#fff;border:1px solid #e3e6ea;border-radius:8px;padding:14px 18px;margin-bottom:16px}
h2{font-size:15px;margin:0 0 10px;color:#444;text-transform:uppercase;letter-spacing:.04em}
table{border-collapse:collapse;width:100%;font-size:13.5px}
th,td{text-align:left;padding:5px 8px;border-bottom:1px solid #eef0f2;white-space:nowrap}
th{color:#666;font-weight:600} td.num{text-align:right;font-variant-numeric:tabular-nums}
.bar{height:10px;background:#e9ecef;border-radius:5px;overflow:hidden;margin:6px 0 8px}
.bar>div{height:100%;background:#2f6fed}
.issue{color:#b42318;font-weight:600;margin:4px 0}
.tag{display:inline-block;padding:1px 7px;border-radius:10px;font-size:12px;font-weight:600}
.tag.ok{background:#e6f4ea;color:#1f8a4c} .tag.problem{background:#fde8e6;color:#b42318}
.tag.deferred,.tag.skipped,.tag.missing{background:#fff4d6;color:#8a5a00}
.muted{color:#777} code{background:#f0f2f4;padding:1px 5px;border-radius:4px}
"""


def _e(v) -> str:
    return html.escape("" if v is None else str(v))


def _tag(status_name: str) -> str:
    cls = status_name if status_name in ("ok", "problem", "deferred", "skipped", "missing") else "muted"
    return f'<span class="tag {cls}">{_e(status_name)}</span>'


def render_html(report: dict, summary: dict | None, history: list[dict]) -> str:
    healthy = report.get("healthy")
    parts = [f"<!doctype html><html><head><meta charset='utf-8'><title>hypercore monitor</title>"
             f"<meta http-equiv='refresh' content='{REFRESH_S}'><style>{_CSS}</style></head><body>",
             f"<header class='{'ok' if healthy else 'bad'}'>hypercore {'HEALTHY' if healthy else 'ATTENTION'}"
             f"<small>{_e(report.get('now'))} UTC &middot; refreshes every {REFRESH_S} s</small></header><main>"]
    if report.get("issues"):
        parts.append("<section><h2>Attention</h2>" + "".join(f"<div class='issue'>{_e(i)}</div>"
                                                              for i in report["issues"]) + "</section>")
    p = report.get("progress") or {}
    parts.append("<section><h2>Current run</h2>")
    if report.get("running"):
        total = p.get("wallets_total") or 0
        done = p.get("wallets_done") or 0
        pct = int(100 * done / total) if total else 0
        parts.append(f"<div>phase <b>{_e(p.get('phase'))}</b>"
                     + (f" &middot; {_e(p.get('detail'))}" if p.get("detail") else "")
                     + f" &middot; wallet {done}/{total}"
                     + (f" (deferred {p.get('wallets_deferred')})" if p.get("wallets_deferred") else "")
                     + (f" &middot; analysed {p.get('analysed')}/{p.get('analysed_total')}"
                        if p.get("phase") == "analyse" and p.get("analysed") is not None else "")
                     + f"</div><div class='bar'><div style='width:{pct}%'></div></div>"
                     f"<div class='muted'>running {_e(p.get('minutes_running'))} min, last progress "
                     f"{_e(p.get('minutes_since_update'))} min ago, current {_e(p.get('current'))}"
                     + (f", deadline {_e(p.get('deadline_at'))}" if p.get("deadline_at") else "")
                     + f", pid {_e(p.get('pid'))}</div>")
    else:
        parts.append("<div class='muted'>not running"
                     + (f" &middot; last finished {_e(p.get('finished_at'))}" if p.get("finished_at") else "")
                     + "</div>")
    parts.append("</section>")

    lr = report.get("last_run")
    parts.append("<section><h2>Last run</h2>")
    if lr:
        parts.append(f"<div>{_tag('ok' if lr.get('ok') else 'problem')} {_e(lr.get('minutes_ago'))} min ago &middot; "
                     f"{_e(lr.get('duration_s'))} s &middot; wallets ok {_e(lr.get('wallets_ok'))}/{_e(lr.get('wallets_total'))}"
                     + (f" &middot; deferred {_e(lr.get('wallets_deferred'))}" if lr.get("wallets_deferred") else "")
                     + f" &middot; tracked {_e(lr.get('tracked'))} &middot; v{_e(lr.get('version'))}</div>")
        if lr.get("problems"):
            parts.append("<div class='issue'>problems: " + _e(", ".join(lr["problems"][:20])) + "</div>")
        if lr.get("warnings"):
            parts.append("<div class='muted'>warnings: " + _e(", ".join(lr["warnings"][:20])) + "</div>")
    else:
        parts.append("<div class='muted'>no run has completed yet</div>")
    parts.append("</section>")

    timers = report.get("timers") or {}
    if timers:
        parts.append("<section><h2>Timers</h2><table><tr><th>timer</th><th>state</th><th>next</th></tr>")
        for name, t in timers.items():
            parts.append(f"<tr><td>{_e(name)}</td><td>{_e(t.get('active'))}</td><td>{_e(t.get('next') or '')}</td></tr>")
        parts.append("</table></section>")

    if history:
        parts.append("<section><h2>Recent runs</h2><table><tr><th>run at</th><th>ok</th><th class='num'>duration s</th>"
                     "<th class='num'>wallets ok</th><th class='num'>deferred</th><th>problems</th><th>warnings</th></tr>")
        for r in reversed(history):
            probs = r.get("problems") or []
            warns = r.get("warnings") or []
            parts.append(f"<tr><td>{_e(r.get('run_at'))}</td><td>{_tag('ok' if r.get('ok') else 'problem')}</td>"
                         f"<td class='num'>{_e(r.get('duration_s'))}</td>"
                         f"<td class='num'>{_e(r.get('wallets_ok'))}/{_e(r.get('wallets_total'))}</td>"
                         f"<td class='num'>{_e(r.get('wallets_deferred') or 0)}</td>"
                         f"<td>{_e(', '.join(probs[:3]) + (' …' if len(probs) > 3 else ''))}</td>"
                         f"<td>{_e(', '.join(warns[:3]) + (' …' if len(warns) > 3 else ''))}</td></tr>")
        parts.append("</table></section>")

    per_wallet = (summary or {}).get("per_wallet") or {}
    if per_wallet:
        order = {"problem": 0, "skipped": 1, "missing": 1, "deferred": 2, "ok": 3}
        rows = sorted(per_wallet.items(), key=lambda kv: (order.get(kv[1].get("status"), 9), kv[0]))
        parts.append(f"<section><h2>Wallets in the last run ({len(rows)})</h2><table><tr><th>wallet</th><th>status</th>"
                     "<th>fidelity</th><th>perp oracle</th><th>net worth</th><th>capped</th>"
                     "<th class='num'>l1 rows</th><th class='num'>fills</th><th class='num'>positions</th></tr>")
        for w, e in rows:
            parts.append(f"<tr><td><code>{_e(w)}</code></td><td>{_tag(str(e.get('status')))}</td>"
                         f"<td>{_e(e.get('fidelity_ok'))}</td><td>{_e(e.get('perp_oracle'))}</td>"
                         f"<td>{_e(e.get('networth_oracle'))}</td><td>{_e(e.get('fills_truncated'))}</td>"
                         f"<td class='num'>{_e(e.get('l1_rows'))}</td><td class='num'>{_e(e.get('raw_fills'))}</td>"
                         f"<td class='num'>{_e(e.get('open_positions'))}</td></tr>")
        parts.append("</table></section>")

    parts.append("<section class='muted'>JSON: <a href='/api/status'>/api/status</a> &middot; "
                 "<a href='/api/summary'>/api/summary</a> &middot; <a href='/api/progress'>/api/progress</a> &middot; "
                 "<a href='/api/history'>/api/history</a> &middot; <a href='/healthz'>/healthz</a></section>")
    parts.append("</main></body></html>")
    return "".join(parts)


# --- server -----------------------------------------------------------------------------

class Handler(BaseHTTPRequestHandler):
    monitor: Monitor | None = None       # set by serve()
    server_version = "hypercore-monitor"
    sys_version = ""

    def log_message(self, fmt, *args):   # keep journald quiet; errors still surface via log_error
        log.debug("%s - " + fmt, self.address_string(), *args)

    def _send(self, code: int, body: str, ctype: str) -> None:
        data = body.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(data)

    def _json(self, code: int, obj) -> None:
        self._send(code, json.dumps(obj, indent=2, default=str), "application/json; charset=utf-8")

    def do_HEAD(self):  # noqa: N802
        self.do_GET()

    def do_GET(self):  # noqa: N802
        m = self.monitor
        path = self.path.split("?", 1)[0].rstrip("/") or "/"
        try:
            if path == "/":
                report = m.report()
                self._send(200, render_html(report, m.summary(), m.history()), "text/html; charset=utf-8")
            elif path == "/api/status":
                self._json(200, m.report())
            elif path == "/api/summary":
                self._json(200, m.summary() or {})
            elif path == "/api/progress":
                self._json(200, m.progress() or {})
            elif path == "/api/history":
                self._json(200, m.history(last=200))
            elif path == "/healthz":
                report = m.report()
                self._send(200 if report["healthy"] else 503,
                           "ok\n" if report["healthy"] else "\n".join(report["issues"]) + "\n", "text/plain; charset=utf-8")
            else:
                self._send(404, "not found\n", "text/plain; charset=utf-8")
        except Exception as e:  # noqa: BLE001 - a rendering bug must not take the monitor down
            log.exception("request failed")
            self._send(500, f"error: {type(e).__name__}\n", "text/plain; charset=utf-8")


def serve(bind: str, port: int, monitor: Monitor) -> ThreadingHTTPServer:
    Handler.monitor = monitor
    srv = ThreadingHTTPServer((bind, port), Handler)
    srv.daemon_threads = True
    return srv


def main(argv=None) -> int:
    config.load_env()
    config.setup_logging()
    ap = argparse.ArgumentParser(description="HyperCore web monitor (read-only status page)")
    ap.add_argument("--bind", default=config.env("HL_WEB_BIND"))
    ap.add_argument("--port", type=int, default=config.env_int("HL_WEB_PORT"))
    ap.add_argument("--out", default=str(config.out_root()))
    ap.add_argument("--no-systemd", action="store_true")
    ap.add_argument("--stuck-minutes", type=float, default=config.env_float("HL_STUCK_MINUTES"))
    ap.add_argument("--max-age-hours", type=float,
                    default=(config.env_float("HL_STATUS_MAX_AGE_HOURS") if config.env("HL_STATUS_MAX_AGE_HOURS") else None))
    args = ap.parse_args(argv)
    monitor = Monitor(Path(args.out) / S.NS_HYPERCORE, use_systemd=not args.no_systemd,
                      max_age_h=args.max_age_hours, stuck_min=args.stuck_minutes)
    srv = serve(args.bind, args.port, monitor)
    log.info("hypercore web monitor on http://%s:%d/ (state dir %s)", args.bind, args.port, monitor.ns_dir)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        srv.server_close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
