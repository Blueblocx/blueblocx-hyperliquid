"""Production plumbing: env/config, lock, run summary, alerting, retention, daily
exit codes, preflight. No network — everything external is faked."""
from __future__ import annotations

import csv
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

from hl_poc import config, daily, extract, ops, preflight
from hl_poc.info_client import InfoClient


def _clean_recon(wallets, **overrides):
    """A reconciliation shaped like extract.run's return for clean wallets."""
    by = {}
    for w in wallets:
        by[w] = {
            "fidelity_ok": True, "l1_ledger_rows": 12, "raw_fills": 5, "open_positions": 1,
            "fills_truncated_at_10k_cap": False, "fetch_errors": {}, "unmapped_ledger_types": {},
            "unpriced_held_symbols": [], "lending_lazy_pull": False,
            "oracle": {"checked": True, "account_value_matches": True, "networth_matches": True,
                       "pnl": {"checked": False, "reason": "no prior snapshot (~24h) yet"}},
        }
    recon = {"natural_key_ok": True, "by_wallet": by, "iceberg": None, "retention": None,
             "hip3_dexes_checked": ["xyz"], "unmapped_ledger_types": {}}
    recon.update(overrides)
    return recon


class ConfigTests(unittest.TestCase):
    def test_load_env_sets_defaults_without_overriding(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "x.env"
            p.write_text('A_TEST_KEY="quoted"\nexport B_TEST_KEY=val # trailing comment\n'
                         '# comment\nC_TEST_KEY=from_file\nbroken line\n', encoding="utf-8")
            with mock.patch.dict(os.environ, {"C_TEST_KEY": "preset"}, clear=False):
                for k in ("A_TEST_KEY", "B_TEST_KEY"):
                    os.environ.pop(k, None)
                self.assertEqual(config.load_env(p), p)
                self.assertEqual(os.environ["A_TEST_KEY"], "quoted")
                self.assertEqual(os.environ["B_TEST_KEY"], "val")
                self.assertEqual(os.environ["C_TEST_KEY"], "preset")     # environment wins

    def test_hl_env_file_variable_is_honoured(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "server.env"
            p.write_text("D_TEST_KEY=1\n", encoding="utf-8")
            with mock.patch.dict(os.environ, {"HL_ENV_FILE": str(p)}, clear=False):
                os.environ.pop("D_TEST_KEY", None)
                self.assertEqual(config.load_env(), p)
                self.assertEqual(os.environ["D_TEST_KEY"], "1")

    def test_typed_accessors_and_defaults(self):
        with mock.patch.dict(os.environ, {"HL_OUT_DIR": "/srv/hc", "HL_WEIGHT_BUDGET": "abc"}, clear=False):
            self.assertEqual(config.out_root(), Path("/srv/hc"))
            with self.assertRaises(ValueError):
                config.env_int("HL_WEIGHT_BUDGET")
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("HL_RETENTION_DAYS", None)
            self.assertEqual(config.env_int("HL_RETENTION_DAYS"), 45)        # documented default
            os.environ["HL_ALERT_ON_SUCCESS"] = "yes"
            self.assertTrue(config.env_bool("HL_ALERT_ON_SUCCESS"))

    def test_describe_env_never_exposes_secret_values(self):
        with mock.patch.dict(os.environ, {"AWS_SECRET_ACCESS_KEY": "s3cr3t", "HL_INFO_URL": "http://x"}, clear=False):
            d = config.describe_env()
            self.assertEqual(d["AWS_SECRET_ACCESS_KEY"], {"set": True, "default": "", "value": "<redacted>"})
            self.assertEqual(d["HL_INFO_URL"]["value"], "http://x")
            self.assertNotIn("s3cr3t", json.dumps(d))

    def test_info_client_reads_env_at_construction(self):
        with mock.patch.dict(os.environ, {"HL_WEIGHT_BUDGET": "123", "HL_INFO_URL": "http://hl.test/info",
                                          "HL_HTTP_TIMEOUT": "7"}, clear=False):
            c = InfoClient()
            self.assertEqual((c._budget.budget, c.url, c.timeout), (123, "http://hl.test/info", 7))
            c2 = InfoClient(url="http://other", timeout=3, weight_budget=50)
            self.assertEqual((c2._budget.budget, c2.url, c2.timeout), (50, "http://other", 3))
        with mock.patch.dict(os.environ, {"HL_WEIGHT_BUDGET": ""}, clear=False):
            self.assertEqual(InfoClient()._budget.budget, 900)

    def test_json_logging_format(self):
        buf = io.StringIO()
        config.setup_logging(level="INFO", json_lines=True, stream=buf)
        import logging
        logging.getLogger("hl_poc.test").info("hello %s", "world")
        rec = json.loads(buf.getvalue().strip().splitlines()[-1])
        self.assertEqual((rec["level"], rec["logger"], rec["msg"]), ("INFO", "hl_poc.test", "hello world"))
        config.setup_logging(level="WARNING", json_lines=False, stream=io.StringIO())


class OpsTests(unittest.TestCase):
    def test_atomic_write_leaves_no_temp_file(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "sub" / "state.json"
            ops.atomic_write_text(p, "1")
            ops.atomic_write_text(p, "2")
            self.assertEqual(p.read_text(encoding="utf-8"), "2")
            self.assertEqual([x.name for x in p.parent.iterdir()], ["state.json"])

    def test_run_lock_is_exclusive_and_released(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "hypercore" / ".daily.lock"
            with ops.RunLock(p, stale_hours=12):
                self.assertTrue(p.exists())
                with self.assertRaises(ops.LockHeld):
                    with ops.RunLock(p, stale_hours=12):
                        pass
            self.assertFalse(p.exists())
            with ops.RunLock(p, stale_hours=12):
                pass

    def test_run_lock_replaces_stale_lock(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / ".daily.lock"
            p.write_text('{"pid": 1}', encoding="utf-8")
            old = time.time() - 3 * 3600
            os.utime(p, (old, old))
            with ops.RunLock(p, stale_hours=1):
                self.assertNotEqual(p.read_text(encoding="utf-8"), '{"pid": 1}')
            self.assertFalse(p.exists())

    def test_build_summary_classifies_hard_problems_and_warnings(self):
        wallets = ["0xaaaa0000000000000000000000000000000000aa",
                   "0xbbbb0000000000000000000000000000000000bb",
                   "0xcccc0000000000000000000000000000000000cc",
                   "0xdddd0000000000000000000000000000000000dd"]
        recon = _clean_recon(wallets[:3])
        a, b, c = (recon["by_wallet"][w] for w in wallets[:3])
        a["oracle"]["pnl"] = {"checked": True, "pnl_matches": False}          # warning only
        a["unpriced_held_symbols"] = ["PURR"]
        b["fidelity_ok"] = False
        b["oracle"]["networth_matches"] = False
        b["fetch_errors"] = {"staking": "boom"}
        recon["by_wallet"][wallets[2]] = {"skipped": True, "errors": {"fills": "timeout"}}
        t0 = dt.datetime(2026, 10, 9, 0, 20, tzinfo=dt.timezone.utc)
        s = ops.build_summary(recon, wallets, t0, t0 + dt.timedelta(seconds=90))
        self.assertFalse(s["ok"])
        self.assertEqual(s["duration_s"], 90.0)
        self.assertEqual((s["wallets_ok"], s["wallets_problem"], s["wallets_skipped"]), (1, 1, 2))
        self.assertEqual(s["problems"], ["0xbbbb0000:fidelity", "0xbbbb0000:networth_oracle",
                                         "0xbbbb0000:fetch_errors=staking", "0xcccc0000:skipped",
                                         "0xdddd0000:missing"])
        self.assertEqual(s["warnings"], ["0xaaaa0000:pnl_oracle", "0xaaaa0000:unpriced=PURR"])
        self.assertEqual(s["per_wallet"][wallets[0]]["status"], "ok")
        self.assertEqual(s["per_wallet"][wallets[3]], {"status": "missing"})

    def test_build_summary_clean_and_error(self):
        w = ["0xaaaa0000000000000000000000000000000000aa"]
        t0 = ops.utcnow()
        clean = ops.build_summary(_clean_recon(w), w, t0, t0)
        self.assertTrue(clean["ok"])
        self.assertEqual(clean["problems"], [])
        err = ops.build_summary(None, w, t0, t0, error="RuntimeError: natural key violation")
        self.assertFalse(err["ok"])
        self.assertEqual(err["problems"], ["error:RuntimeError: natural key violation"])
        self.assertEqual(err["per_wallet"][w[0]], {"status": "missing"})   # not double-counted as a problem

    def test_perp_oracle_unreadable_is_a_problem(self):
        w = ["0xaaaa0000000000000000000000000000000000aa"]
        recon = _clean_recon(w)
        recon["by_wallet"][w[0]]["oracle"] = {"checked": False}
        s = ops.build_summary(recon, w, ops.utcnow(), ops.utcnow())
        self.assertEqual(s["problems"], ["0xaaaa0000:perp_oracle"])

    def test_write_summary_and_history(self):
        w = ["0xaaaa0000000000000000000000000000000000aa"]
        s = ops.build_summary(_clean_recon(w), w, ops.utcnow(), ops.utcnow())
        with tempfile.TemporaryDirectory() as d:
            ns = Path(d) / "hypercore"
            ops.write_summary(ns, s)
            ops.write_summary(ns, s)
            latest = json.loads((ns / ops.SUMMARY_FILE).read_text(encoding="utf-8"))
            self.assertTrue(latest["ok"])
            lines = (ns / ops.HISTORY_FILE).read_text(encoding="utf-8").strip().splitlines()
            self.assertEqual(len(lines), 2)
            compact = json.loads(lines[0])
            self.assertNotIn("per_wallet", compact)
            self.assertEqual(compact["per_wallet_status"], {w[0]: "ok"})

    def test_send_alert(self):
        w = ["0xaaaa0000000000000000000000000000000000aa"]
        s = ops.build_summary(_clean_recon(w), w, ops.utcnow(), ops.utcnow())
        s["problems"] = ["0xaaaa0000:fidelity"]
        s["ok"] = False
        calls = []

        class Resp:
            status_code = 200

        def fake_post(url, json=None, timeout=None):
            calls.append((url, json, timeout))
            return Resp()

        with mock.patch.dict(os.environ, {"HL_ALERT_WEBHOOK_URL": ""}, clear=False):
            self.assertFalse(ops.send_alert(s, post=fake_post))
            self.assertEqual(calls, [])
        self.assertTrue(ops.send_alert(s, url="https://hooks.example/abc", post=fake_post))
        url, payload, timeout = calls[0]
        self.assertEqual(url, "https://hooks.example/abc")
        self.assertIn("PROBLEMS", payload["text"])
        self.assertIn("0xaaaa0000:fidelity", payload["text"])
        self.assertLessEqual(len(payload["content"]), 1900)
        self.assertEqual(payload["hypercore"]["problems"], ["0xaaaa0000:fidelity"])

        def boom(*a, **k):
            raise ConnectionError("down")
        self.assertFalse(ops.send_alert(s, url="https://hooks.example/abc", post=boom))


class RetentionTests(unittest.TestCase):
    def _write(self, path, rows):
        with path.open("w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=["tx_hash", "block_timestamp", "amount_signed"])
            w.writeheader()
            w.writerows(rows)

    def test_prune_csv_drops_old_keeps_recent_and_unparseable(self):
        now = dt.datetime(2026, 10, 9, tzinfo=dt.timezone.utc)
        cutoff_ms = int((now - dt.timedelta(days=45)).timestamp() * 1000)
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "l1_wallet_ledger.csv"
            self._write(p, [
                {"tx_hash": "old", "block_timestamp": (now - dt.timedelta(days=60)).isoformat(), "amount_signed": "1"},
                {"tx_hash": "new", "block_timestamp": (now - dt.timedelta(days=10)).isoformat(), "amount_signed": "2"},
                {"tx_hash": "edge", "block_timestamp": (now - dt.timedelta(days=45)).isoformat(), "amount_signed": "3"},
                {"tx_hash": "garbage", "block_timestamp": "not-a-date", "amount_signed": "4"},
            ])
            rep = extract.prune_csv(p, "block_timestamp", cutoff_ms)
            self.assertEqual(rep, {"kept": 3, "dropped": 1})
            with p.open(newline="", encoding="utf-8") as fh:
                left = [r["tx_hash"] for r in csv.DictReader(fh)]
            self.assertEqual(left, ["new", "edge", "garbage"])
            self.assertEqual([x.name for x in Path(d).iterdir()], ["l1_wallet_ledger.csv"])
            self.assertEqual(extract.prune_csv(p, "block_timestamp", cutoff_ms), {"kept": 3, "dropped": 0})

    def test_prune_missing_file(self):
        self.assertEqual(extract.prune_csv(Path("does/not/exist.csv"), "block_timestamp", 0), {"kept": 0, "dropped": 0})


class DailyMainTests(unittest.TestCase):
    W = ["0xaaaa0000000000000000000000000000000000aa", "0xbbbb0000000000000000000000000000000000bb"]

    def _run(self, d, recon=None, raise_exc=None, wallets=None, extra=()):
        out = Path(d) / "out"
        wf = Path(d) / "wallets.txt"
        wf.write_text("\n".join(wallets if wallets is not None else self.W) + "\n# comment\n", encoding="utf-8")
        captured = {}

        def fake_run(wallets, days, out_dir, **kw):
            captured.update(wallets=wallets, days=days, out_dir=out_dir, **kw)
            if raise_exc:
                raise raise_exc
            return recon

        with mock.patch.object(daily.extract, "run", side_effect=fake_run):
            code = daily.main(["--wallets-file", str(wf), "--out", str(out), "--no-alert",
                               "--iceberg", "--retention-days", "60", *extra])
        summary_path = out / "hypercore" / ops.SUMMARY_FILE
        summary = json.loads(summary_path.read_text(encoding="utf-8")) if summary_path.exists() else None
        return code, summary, captured, out

    def test_clean_run_exits_zero_and_passes_options(self):
        with tempfile.TemporaryDirectory() as d:
            code, summary, captured, out = self._run(d, recon=_clean_recon(self.W))
            self.assertEqual(code, 0)
            self.assertTrue(summary["ok"])
            self.assertEqual(captured["wallets"], self.W)
            self.assertEqual((captured["iceberg"], captured["incremental"], captured["retention_days"]),
                             (True, True, 60))
            self.assertFalse((out / "hypercore" / ".daily.lock").exists())
            self.assertTrue((out / "hypercore" / ops.HISTORY_FILE).exists())

    def test_problem_run_exits_one(self):
        recon = _clean_recon(self.W)
        recon["by_wallet"][self.W[1]]["fills_truncated_at_10k_cap"] = True
        with tempfile.TemporaryDirectory() as d:
            code, summary, _, _ = self._run(d, recon=recon)
            self.assertEqual(code, 1)
            self.assertEqual(summary["problems"], ["0xbbbb0000:fills_truncated"])

    def test_exception_is_summarised_not_fatal(self):
        with tempfile.TemporaryDirectory() as d:
            code, summary, _, out = self._run(d, raise_exc=RuntimeError("natural key violation: 1 collision"))
            self.assertEqual(code, 1)
            self.assertTrue(summary["problems"][0].startswith("error:RuntimeError: natural key violation"))
            self.assertFalse((out / "hypercore" / ".daily.lock").exists())   # released on failure

    def test_bad_config_exit_two(self):
        with tempfile.TemporaryDirectory() as d:
            code = daily.main(["--wallets-file", str(Path(d) / "nope.txt"), "--out", d, "--no-alert"])
            self.assertEqual(code, 2)
            code, _, _, _ = self._run(d, recon=_clean_recon([]), wallets=[])
            self.assertEqual(code, 2)

    def test_lock_held_exit_three(self):
        with tempfile.TemporaryDirectory() as d:
            lock = Path(d) / "out" / "hypercore" / ".daily.lock"
            lock.parent.mkdir(parents=True)
            lock.write_text('{"pid": 999}', encoding="utf-8")
            code, summary, captured, _ = self._run(d, recon=_clean_recon(self.W))
            self.assertEqual(code, 3)
            self.assertIsNone(summary)
            self.assertEqual(captured, {})           # extract.run never called


class PreflightTests(unittest.TestCase):
    def test_offline_checks_have_no_failures_here(self):
        with tempfile.TemporaryDirectory() as d, \
                mock.patch.dict(os.environ, {"HL_WEIGHT_BUDGET": "600", "HL_RETENTION_DAYS": "45"}, clear=False):
            os.environ.pop("NESSIE_URI", None)
            checks = preflight.run_checks(offline=True, out_dir=Path(d))
            by = {c.name: c for c in checks}
            self.assertEqual([c.name for c in checks if c.status == "FAIL"], [])
            self.assertEqual(by["hl_api"].status, "SKIP")
            self.assertEqual(by["iceberg"].status, "WARN")          # local catalog is dev-only
            self.assertEqual(by["wallets"].status, "SKIP")

    def test_env_sanity_failures(self):
        with mock.patch.dict(os.environ, {"HL_WEIGHT_BUDGET": "5000", "HL_RETENTION_DAYS": "10"}, clear=False):
            by = {c.name: c for c in preflight.check_env(None)}
            self.assertEqual(by["weight_budget"].status, "FAIL")
            self.assertEqual(by["retention"].status, "FAIL")
        with mock.patch.dict(os.environ, {"HL_WEIGHT_BUDGET": "1000", "HL_RETENTION_DAYS": "0"}, clear=False):
            by = {c.name: c for c in preflight.check_env(None)}
            self.assertEqual(by["weight_budget"].status, "WARN")
            self.assertEqual(by["retention"].status, "OK")

    def test_wallet_file_validation(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "w.txt"
            p.write_text("0xzz\n", encoding="utf-8")
            self.assertEqual(preflight.check_wallets(p).status, "FAIL")
            p.write_text("0xaaaa0000000000000000000000000000000000aa\n0xAAAA0000000000000000000000000000000000AA\n", encoding="utf-8")
            c = preflight.check_wallets(p)
            self.assertEqual(c.status, "WARN")                      # duplicate after lowercasing
            self.assertEqual(preflight.check_wallets(Path(d) / "missing.txt").status, "FAIL")

    def test_iceberg_required_without_rest_fails(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("NESSIE_URI", None)
            self.assertEqual(preflight.check_iceberg(required=True).status, "FAIL")
            self.assertEqual(preflight.check_iceberg(required=False).status, "WARN")

    def test_main_json_output(self):
        buf = io.StringIO()
        with tempfile.TemporaryDirectory() as d, redirect_stdout(buf), \
                mock.patch.dict(os.environ, {"HL_WEIGHT_BUDGET": "600"}, clear=False):
            code = preflight.main(["--offline", "--json", "--out", d])
        doc = json.loads(buf.getvalue())
        self.assertEqual(code, 0)
        self.assertTrue(doc["ok"])
        self.assertIn("deps", {c["name"] for c in doc["checks"]})


if __name__ == "__main__":
    unittest.main()
