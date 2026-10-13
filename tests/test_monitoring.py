"""Observability layer: live progress file, status verdict, heartbeat, webhook text,
the OnFailure notifier. No network, no systemd - everything external is faked."""
from __future__ import annotations

import datetime as dt
import io
import json
import os
import tempfile
import time
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

from blueblocx_hyperliquid import extract, notify, ops, s3_backfill, status
from blueblocx_hyperliquid import schema as S
from tests.test_tracked import W1, W2, _ClientDown, _FakeClient, _FakeTime

NOW = dt.datetime(2026, 10, 13, 8, 0, tzinfo=dt.timezone.utc)


def _iso(minutes_ago: float) -> str:
    return (NOW - dt.timedelta(minutes=minutes_ago)).isoformat()


class _Resp:
    def __init__(self, code=200):
        self.status_code = code


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


class HeartbeatAndWebhookTests(unittest.TestCase):
    def test_heartbeat_targets_and_outcomes(self):
        seen = []

        def get(url, timeout):
            seen.append(url)
            return _Resp(200)
        self.assertTrue(ops.heartbeat("https://hc.example/ping/abc", True, get=get))
        self.assertTrue(ops.heartbeat("https://hc.example/ping/abc/", False, get=get))
        self.assertEqual(seen, ["https://hc.example/ping/abc", "https://hc.example/ping/abc/fail"])
        self.assertFalse(ops.heartbeat("", True, get=get))                         # off: no call
        self.assertEqual(len(seen), 2)
        self.assertFalse(ops.heartbeat("https://hc.example/p", True, get=lambda url, timeout: _Resp(503)))

        def boom(url, timeout):
            raise ConnectionError("down")
        self.assertFalse(ops.heartbeat("https://hc.example/p", True, get=boom))

    def test_post_text_and_send_alert_payloads(self):
        posts = []

        def post(url, json, timeout):
            posts.append((url, json))
            return _Resp(204)
        self.assertTrue(ops.post_text("hello", url="https://hook", post=post, extra={"k": 1}))
        self.assertEqual(posts[-1][1], {"text": "hello", "content": "hello", "k": 1})
        summary = {"ok": False, "wallets_ok": 1, "wallets_total": 2, "duration_s": 5, "host": "h",
                   "run_at": "2026-10-13T00:00:00+00:00", "problems": ["0xaaaa:fidelity"], "warnings": ["deferred=1"]}
        self.assertTrue(ops.send_alert(summary, url="https://hook", post=post))
        payload = posts[-1][1]
        self.assertIn("PROBLEMS", payload["text"])
        self.assertIn("0xaaaa:fidelity", payload["text"])
        self.assertEqual(payload["hypercore"]["problems"], ["0xaaaa:fidelity"])
        with mock.patch.dict(os.environ, {"HL_ALERT_WEBHOOK_URL": ""}):
            self.assertFalse(ops.post_text("x", post=post))                        # unset: no call
        self.assertEqual(len(posts), 2)


class NotifyTests(unittest.TestCase):
    def _run(self, d, summary, env=None, unit="hypercore-daily.service", extra=()):
        ns = Path(d) / S.NS_HYPERCORE
        ns.mkdir(parents=True, exist_ok=True)
        if summary is not None:
            (ns / ops.SUMMARY_FILE).write_text(json.dumps(summary), encoding="utf-8")
        posts, gets = [], []

        def post(url, json, timeout):
            posts.append(json)
            return _Resp(200)

        def get(url, timeout):
            gets.append(url)
            return _Resp(200)
        environ = {"HL_ALERT_WEBHOOK_URL": "https://hook", "HL_HEARTBEAT_URL": "https://hc/ping/x"}
        environ.update(env or {})
        with mock.patch.dict(os.environ, environ):
            rc = notify.main(["--failed-unit", unit, "--out", d, *extra], now=NOW, post=post, get=get)
        return rc, posts, gets

    def test_quiet_when_the_data_run_reported_itself(self):
        with tempfile.TemporaryDirectory() as d:
            rc, posts, gets = self._run(d, {"ok": False, "finished_at": _iso(2)})
            self.assertEqual((rc, posts, gets), (0, [], []))

    def test_alerts_when_no_fresh_summary(self):
        with tempfile.TemporaryDirectory() as d:
            rc, posts, gets = self._run(d, None)
            self.assertEqual(rc, 0)
            self.assertIn("hypercore-daily.service FAILED", posts[0]["text"])
            self.assertIn("no run summary exists yet", posts[0]["text"])
            self.assertEqual(gets, ["https://hc/ping/x/fail"])
        with tempfile.TemporaryDirectory() as d:
            rc, posts, _ = self._run(d, {"ok": True, "finished_at": _iso(90)})
            self.assertIn("last summary 90 min ago (ok=True)", posts[0]["text"])
        with tempfile.TemporaryDirectory() as d:          # --grace-minutes 0 defeats the quiet window (test command)
            rc, posts, _ = self._run(d, {"ok": True, "finished_at": _iso(1)}, extra=["--grace-minutes", "0"])
            self.assertEqual(len(posts), 1)

    def test_maintenance_failure_posts_despite_a_fresh_daily_summary_and_skips_the_heartbeat(self):
        with tempfile.TemporaryDirectory() as d:
            rc, posts, gets = self._run(d, {"ok": True, "finished_at": _iso(3)}, unit="hypercore-maintenance.service")
            self.assertEqual(rc, 0)
            self.assertEqual(len(posts), 1)
            self.assertIn("hypercore-maintenance.service FAILED", posts[0]["text"])
            self.assertIn("writes no run summary", posts[0]["text"])
            self.assertEqual(gets, [])                        # the heartbeat answers "did the data run happen"

    def test_no_urls_configured_is_still_exit_zero(self):
        with tempfile.TemporaryDirectory() as d:
            rc, posts, gets = self._run(d, None, env={"HL_ALERT_WEBHOOK_URL": "", "HL_HEARTBEAT_URL": ""})
            self.assertEqual((rc, posts, gets), (0, [], []))


class StatusTests(unittest.TestCase):
    OK_SUMMARY = {"ok": True, "run_at": _iso(15), "finished_at": _iso(10), "duration_s": 300.0,
                  "wallets_total": 149, "wallets_ok": 147, "wallets_deferred": 2, "problems": [],
                  "warnings": ["deferred=2"], "version": "1.2.0", "tracked": {"source": "fresh"}}
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


if __name__ == "__main__":
    unittest.main()
