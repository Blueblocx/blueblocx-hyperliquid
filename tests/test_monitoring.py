"""Observability layer: live progress file, status verdict, the web monitor. No
network beyond 127.0.0.1, no systemd - everything external is faked."""
from __future__ import annotations

import datetime as dt
import http.client
import io
import json
import os
import tempfile
import threading
import time
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

from blueblocx_hyperliquid import extract, ops, s3_backfill, status, web
from blueblocx_hyperliquid import schema as S
from tests.test_tracked import W1, W2, _ClientDown, _FakeClient, _FakeTime

NOW = dt.datetime(2026, 10, 13, 8, 0, tzinfo=dt.timezone.utc)


def _iso(minutes_ago: float) -> str:
    return (NOW - dt.timedelta(minutes=minutes_ago)).isoformat()


class ProgressTests(unittest.TestCase):
    def test_progress_file_lifecycle(self):
        with tempfile.TemporaryDirectory() as d:
            ns = Path(d) / "hypercore"
            p = ops.Progress(ns, planned=3)
            first = ops.read_progress(ns)
            self.assertEqual((first["state"], first["phase"], first["wallets_total"], first["pid"]),
                             ("running", "starting", 3, os.getpid()))
            p.update(phase="pull", current=W1, wallets_done=1)
            cur = ops.read_progress(ns)
            self.assertEqual((cur["phase"], cur["current"], cur["wallets_done"]), ("pull", W1, 1))
            self.assertGreaterEqual(cur["updated_at"], first["updated_at"])
            p.finish(ok=True, problems=0)
            done = ops.read_progress(ns)
            self.assertEqual((done["state"], done["phase"], done["current"], done["ok"]), ("finished", "done", None, True))
            self.assertIn("finished_at", done)

    def test_progress_write_failure_never_raises(self):
        with tempfile.TemporaryDirectory() as d:
            p = ops.Progress(Path(d) / "hypercore")
            with mock.patch.object(ops, "atomic_write_text", side_effect=OSError("disk full")):
                p.update(phase="pull")          # logged, not raised
        self.assertIsNone(ops.read_progress(Path(d) / "nope"))

    def test_extract_run_reports_phases_and_deferral(self):
        calls: list[dict] = []
        with tempfile.TemporaryDirectory() as d, mock.patch.object(extract, "InfoClient", _ClientDown):
            extract.run([W1, W2], 2, Path(d), hip3=False, deadline_ts=time.time() - 1,
                        progress=lambda **kw: calls.append(kw))
        phases = [c["phase"] for c in calls if "phase" in c]
        self.assertEqual(phases, ["pull", "analyse", "write", "finalise"])
        self.assertEqual(calls[0], {"phase": "pull", "wallets_total": 2, "wallets_done": 0, "current": None})
        after_pull = next(c for c in calls if "wallets_deferred" in c)
        self.assertEqual((after_pull["wallets_done"], after_pull["wallets_deferred"]), (0, 2))

    def test_extract_run_reports_the_wallet_in_flight_in_both_phases(self):
        calls: list[dict] = []
        fake = _FakeClient
        fake.clock, fake.bump, fake.fills = _FakeTime(1_760_000_000.0), 0.0, {}
        with tempfile.TemporaryDirectory() as d, mock.patch.object(extract, "InfoClient", fake), \
                mock.patch.object(extract, "time", fake.clock):
            extract.run([W1], 2, Path(d), hip3=False, progress=lambda **kw: calls.append(kw))
        self.assertIn({"current": W1, "wallets_done": 0}, calls)                      # phase 1
        self.assertIn({"wallets_done": 1, "wallets_deferred": 0, "current": None}, calls)
        analyse_at = calls.index({"phase": "analyse"})
        self.assertIn({"current": W1, "analysed": 0, "analysed_total": 1}, calls[analyse_at:])   # phase 2 tick
        self.assertIn({"current": None, "detail": None}, calls[analyse_at:])

    def test_s3_pass_reports_each_hour_file(self):
        seen = []
        start = int(dt.datetime(2026, 10, 6, 8, 15, tzinfo=dt.timezone.utc).timestamp() * 1000)
        with mock.patch.object(s3_backfill, "_download_compressed", return_value=("missing", None)):
            res = s3_backfill.fetch_fills_s3([W1], start, start + 2 * 3_600_000, on_file=lambda d, t: seen.append((d, t)))
        self.assertEqual(seen, [(1, 3), (2, 3), (3, 3)])
        self.assertEqual(len(res.keys_missing), 3)


class StatusTests(unittest.TestCase):
    OK_SUMMARY = {"ok": True, "run_at": _iso(15), "finished_at": _iso(10), "duration_s": 300.0,
                  "wallets_total": 149, "wallets_ok": 147, "wallets_deferred": 2, "problems": [],
                  "warnings": ["deferred=2"], "version": "1.3.0", "tracked": {"source": "fresh"}}
    RUNNING = {"state": "running", "pid": 123, "host": "h", "phase": "pull", "wallets_done": 40, "wallets_total": 149,
               "wallets_deferred": 0, "current": W1, "started_at": _iso(12), "updated_at": _iso(1)}
    # what `systemctl show` reports for a LIVE Type=oneshot run: activating/start, never active
    LIVE = {status.UNIT: {"ActiveState": "activating", "SubState": "start", "LoadState": "loaded"},
            "hypercore-hourly.timer": {"ActiveState": "active", "NextElapseUSecRealtime": "Tue 2026-10-13 09:10:00 UTC",
                                       "LoadState": "loaded"}}
    IDLE = {status.UNIT: {"ActiveState": "inactive", "SubState": "dead", "LoadState": "loaded"},
            "hypercore-hourly.timer": {"ActiveState": "active", "LoadState": "loaded"}}

    def test_healthy_running_under_systemd_and_without_it(self):
        for units, alive in ((self.LIVE, True), (self.LIVE, None), ({}, True), ({}, None)):
            r = status.assess(self.RUNNING, self.OK_SUMMARY, [], NOW, units=units, alive=alive)
            self.assertTrue(r["healthy"], (units, alive, r["issues"]))
            self.assertTrue(r["running"])
        r = status.assess(self.RUNNING, self.OK_SUMMARY, [], NOW, units=self.LIVE, alive=True)
        self.assertEqual(r["progress"]["minutes_since_update"], 1.0)
        text = status.format_report(r)
        self.assertIn("HEALTHY", text)
        self.assertIn("wallet 40/149", text)

    def test_stuck_when_the_process_is_alive_but_quiet(self):
        quiet = dict(self.RUNNING, updated_at=_iso(25), phase="analyse", detail="ledger_all pull")
        r = status.assess(quiet, self.OK_SUMMARY, [], NOW, units=self.LIVE, alive=True, stuck_min=15)
        self.assertFalse(r["healthy"])
        self.assertTrue(r["issues"][0].startswith("STUCK? no progress for 25 min in phase analyse, ledger_all pull"))
        r = status.assess(quiet, self.OK_SUMMARY, [], NOW, units=self.LIVE, alive=True, stuck_min=30)
        self.assertTrue(r["healthy"])
        r = status.assess(quiet, self.OK_SUMMARY, [], NOW, units={}, alive=None, stuck_min=15)   # no signal at all
        self.assertTrue(r["issues"][0].startswith("STUCK?"))
        stopping = {status.UNIT: {"ActiveState": "deactivating", "SubState": "stop-sigterm", "LoadState": "loaded"}}
        r = status.assess(quiet, self.OK_SUMMARY, [], NOW, units=stopping, alive=None, stuck_min=15)
        self.assertTrue(r["issues"][0].startswith("STUCK?"))                                     # not "died"

    def test_dead_process_means_died_without_summary(self):
        for units, alive in ((self.IDLE, None), (self.IDLE, False), ({}, False),
                             ({status.UNIT: {"ActiveState": "failed", "LoadState": "loaded"}}, None)):
            r = status.assess(self.RUNNING, self.OK_SUMMARY, [], NOW, units=units, alive=alive)
            self.assertIn("died without a summary", r["issues"][0], (units, alive))
        # /proc says alive even though systemd thinks the unit is idle (manual CLI run): trust /proc
        r = status.assess(self.RUNNING, self.OK_SUMMARY, [], NOW, units=self.IDLE, alive=True)
        self.assertTrue(r["healthy"])

    def test_last_run_verdicts(self):
        r = status.assess(None, self.OK_SUMMARY, [{"ok": True}, {"ok": False, "finished_at": "x"}], NOW,
                          units=self.IDLE)
        self.assertTrue(r["healthy"])
        self.assertEqual(r["history"], {"runs": 2, "ok": 1, "last_failed_at": "x"})
        bad = dict(self.OK_SUMMARY, ok=False, problems=["0xaaaa:fidelity", "tracked_unavailable"])
        r = status.assess(None, bad, [], NOW, units=self.IDLE)
        self.assertIn("last run had 2 problem(s): 0xaaaa:fidelity, tracked_unavailable", r["issues"])
        stale = dict(self.OK_SUMMARY, finished_at=_iso(200))
        r = status.assess(None, stale, [], NOW, units=self.IDLE, max_age_h=2)
        self.assertTrue(any(i.startswith("STALE") for i in r["issues"]))
        r = status.assess(None, None, [], NOW, units=self.IDLE)
        self.assertEqual(r["issues"], ["no run summary yet - nothing has completed"])

    def test_stale_threshold_follows_the_active_timer(self):
        daily = {status.UNIT: {"ActiveState": "inactive", "LoadState": "loaded"},
                 "hypercore-daily.timer": {"ActiveState": "active", "LoadState": "loaded"},
                 "hypercore-hourly.timer": {"ActiveState": "inactive", "LoadState": "loaded"}}
        nine_h_old = dict(self.OK_SUMMARY, finished_at=_iso(9 * 60))
        r = status.assess(None, nine_h_old, [], NOW, units=daily)
        self.assertEqual(r["max_age_h"], 26.0)
        self.assertTrue(r["healthy"], r["issues"])
        r = status.assess(None, nine_h_old, [], NOW, units=self.IDLE)        # hourly timer -> 2 h
        self.assertEqual(r["max_age_h"], 2.0)
        self.assertTrue(any(i.startswith("STALE") for i in r["issues"]))
        r = status.assess(None, nine_h_old, [], NOW, units={})               # no systemd: 2 h unless overridden
        self.assertEqual(r["max_age_h"], 2.0)
        r = status.assess(None, nine_h_old, [], NOW, units={}, max_age_h=26)
        self.assertTrue(r["healthy"])

    def test_timer_disabled_is_an_issue_and_without_systemd_it_is_not(self):
        off = {status.UNIT: {"ActiveState": "inactive", "LoadState": "loaded"},
               "hypercore-hourly.timer": {"ActiveState": "inactive", "LoadState": "loaded"},
               "hypercore-maintenance.timer": {"ActiveState": "active", "LoadState": "loaded"}}
        r = status.assess(None, self.OK_SUMMARY, [], NOW, units=off)
        self.assertIn("no data timer is active (hypercore-hourly.timer / hypercore-daily.timer)", r["issues"])
        r = status.assess(None, self.OK_SUMMARY, [], NOW, units={})
        self.assertTrue(r["healthy"])

    def test_systemd_state_parses_show_output(self):
        def run(cmd, capture_output, text, timeout):
            name = cmd[2]
            out = ("ActiveState=activating\nSubState=start\nLoadState=loaded\n" if name == status.UNIT
                   else "LoadState=not-found\nActiveState=inactive\n")

            class R:
                stdout = out
            return R()
        st = status.systemd_state(run=run)
        self.assertEqual(list(st), [status.UNIT])
        self.assertEqual(st[status.UNIT]["SubState"], "start")

    def test_process_alive_uses_proc_only_for_this_host(self):
        self.assertIsNone(status.process_alive(None))
        self.assertIsNone(status.process_alive({"state": "finished", "pid": os.getpid()}))
        self.assertIsNone(status.process_alive({"state": "running", "pid": os.getpid(), "host": "elsewhere"}))
        here = status.process_alive({"state": "running", "pid": os.getpid()})
        self.assertIn(here, (True, None))                 # True on Linux (/proc), None where it cannot be told

    def test_main_json_and_exit_codes(self):
        # status.main calls config.load_env(): the patch reverts whatever a developer's <repo>/.env adds
        with tempfile.TemporaryDirectory() as d, \
                mock.patch.dict(os.environ, {"HL_STUCK_MINUTES": "15", "HL_STATUS_MAX_AGE_HOURS": ""}, clear=False):
            ns = Path(d) / S.NS_HYPERCORE
            ns.mkdir(parents=True)
            buf = io.StringIO()
            with redirect_stdout(buf):
                rc = status.main(["--out", d, "--no-systemd", "--json"])
            self.assertEqual(rc, 1)
            self.assertEqual(json.loads(buf.getvalue())["issues"], ["no run summary yet - nothing has completed"])
            ten_min_ago = (ops.utcnow() - dt.timedelta(minutes=10)).isoformat()
            summary = dict(self.OK_SUMMARY, finished_at=ten_min_ago, run_at=ten_min_ago)
            (ns / ops.SUMMARY_FILE).write_text(json.dumps(summary), encoding="utf-8")
            buf = io.StringIO()
            with redirect_stdout(buf):
                rc = status.main(["--out", d, "--no-systemd"])
            self.assertEqual(rc, 0)                                                   # default 2 h: fine
            self.assertIn("HEALTHY", buf.getvalue())
            (ns / ops.PROGRESS_FILE).write_text("{not json", encoding="utf-8")       # corrupt progress: ignored
            with redirect_stdout(io.StringIO()):
                self.assertEqual(status.main(["--out", d, "--no-systemd"]), 0)
            with mock.patch.dict(os.environ, {"HL_STATUS_MAX_AGE_HOURS": "0.1"}):     # 6 min: env override -> STALE
                with redirect_stdout(io.StringIO()):
                    self.assertEqual(status.main(["--out", d, "--no-systemd"]), 1)


class WebTests(unittest.TestCase):
    def test_render_html_shows_the_verdict_tables_and_escapes(self):
        history = [{"ok": True, "run_at": "2026-10-13T06:10:00+00:00", "duration_s": 420.0, "wallets_ok": 149,
                    "wallets_total": 149, "wallets_deferred": 0, "problems": [], "warnings": []},
                   {"ok": False, "run_at": "2026-10-13T07:10:00+00:00", "duration_s": 2100.0, "wallets_ok": 70,
                    "wallets_total": 149, "wallets_deferred": 78, "problems": ["a", "b", "c", "d"], "warnings": ["w"]}]
        report = status.assess(StatusTests.RUNNING, StatusTests.OK_SUMMARY, history, NOW, units=StatusTests.LIVE,
                               alive=True)
        summary = dict(StatusTests.OK_SUMMARY, per_wallet={
            W1: {"status": "ok", "fidelity_ok": True, "perp_oracle": True, "networth_oracle": True,
                 "fills_truncated": False, "l1_rows": 12, "raw_fills": 5, "open_positions": 1},
            W2: {"status": "problem", "fidelity_ok": False}})
        page = web.render_html(report, summary, history)
        self.assertIn("hypercore HEALTHY", page)
        self.assertIn("wallet 40/149", page)
        self.assertIn("Recent runs", page)
        self.assertIn("a, b, c …", page)
        self.assertIn("hypercore-hourly.timer", page)
        table_at = page.index("Wallets in the last run")        # (W1 also appears earlier as the wallet in flight)
        self.assertLess(page.index(W2, table_at), page.index(W1, table_at))   # problem wallets are listed first
        bad = status.assess(None, dict(StatusTests.OK_SUMMARY, ok=False, problems=["<script>x</script>"]), [], NOW,
                            units=StatusTests.IDLE)
        page = web.render_html(bad, None, [])
        self.assertIn("hypercore ATTENTION", page)
        self.assertIn("&lt;script&gt;x&lt;/script&gt;", page)
        self.assertNotIn("<script>x", page)
        empty = status.assess(None, None, [], NOW, units=StatusTests.IDLE)
        self.assertIn("no run has completed yet", web.render_html(empty, None, []))

    def test_server_routes(self):
        with tempfile.TemporaryDirectory() as d:
            ns = Path(d) / S.NS_HYPERCORE
            ns.mkdir(parents=True)
            srv = web.serve("127.0.0.1", 0, web.Monitor(ns, use_systemd=False))
            port = srv.server_address[1]
            threading.Thread(target=srv.serve_forever, daemon=True).start()

            def get(path):
                c = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
                c.request("GET", path)
                r = c.getresponse()
                body = r.read().decode("utf-8")
                ctype = r.getheader("Content-Type") or ""
                c.close()
                return r.status, body, ctype
            try:
                st, body, _ = get("/healthz")
                self.assertEqual((st, body), (503, "no run summary yet - nothing has completed\n"))
                st, body, ctype = get("/api/status")
                self.assertEqual(st, 200)
                self.assertIn("json", ctype)
                self.assertFalse(json.loads(body)["healthy"])
                st, body, ctype = get("/")
                self.assertEqual(st, 200)
                self.assertIn("text/html", ctype)
                self.assertIn("hypercore ATTENTION", body)
                fresh = dict(StatusTests.OK_SUMMARY, finished_at=ops.utcnow().isoformat(), run_at=ops.utcnow().isoformat())
                (ns / ops.SUMMARY_FILE).write_text(json.dumps(fresh), encoding="utf-8")
                (ns / ops.HISTORY_FILE).write_text(json.dumps({"ok": True, "run_at": "r"}) + "\n", encoding="utf-8")
                st, body, _ = get("/healthz")
                self.assertEqual((st, body), (200, "ok\n"))
                st, body, _ = get("/api/summary")
                self.assertTrue(json.loads(body)["ok"])
                st, body, _ = get("/api/progress")
                self.assertEqual(json.loads(body), {})
                st, body, _ = get("/api/history")
                self.assertEqual(json.loads(body), [{"ok": True, "run_at": "r"}])
                st, body, _ = get("/?refresh=1")
                self.assertIn("hypercore HEALTHY", body)
                st, _, _ = get("/nope")
                self.assertEqual(st, 404)
            finally:
                srv.shutdown()
                srv.server_close()


if __name__ == "__main__":
    unittest.main()
