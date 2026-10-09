"""Iceberg lift plumbing for a REST (Nessie) deployment: bounded incremental lifts,
metadata-only row counts, REST client properties, lift-state marker, and the
preflight checks that catch an unreachable advertised S3 endpoint or a missing
FileIO dependency. No network — catalogs are faked."""
from __future__ import annotations

import csv
import datetime as dt
import json
import os
import socket
import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

from hl_poc import iceberg_load as il
from hl_poc import preflight


def _write_csv(path: Path, rows: list[dict]) -> None:
    cols = ["tx_hash", "updated_at", "amount_signed"]
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=cols)
        w.writeheader()
        w.writerows(rows)


class BoundedLiftTests(unittest.TestCase):
    def test_read_csv_rows_since_filter(self):
        t0 = dt.datetime(2026, 10, 9, 0, 20, tzinfo=dt.timezone.utc)
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "l1_wallet_ledger.csv"
            _write_csv(p, [
                {"tx_hash": "old", "updated_at": (t0 - dt.timedelta(days=3)).isoformat(), "amount_signed": "1"},
                {"tx_hash": "edge", "updated_at": t0.isoformat(), "amount_signed": "2"},          # == since: excluded
                {"tx_hash": "new", "updated_at": (t0 + dt.timedelta(minutes=1)).isoformat(), "amount_signed": "3"},
                {"tx_hash": "nots", "updated_at": "", "amount_signed": "4"},                         # unparseable: kept
                {"tx_hash": "naive", "updated_at": (t0 + dt.timedelta(hours=1)).replace(tzinfo=None).isoformat(),
                 "amount_signed": "5"},                                                             # naive = UTC
            ])
            self.assertEqual([r["tx_hash"] for r in il.read_csv_rows(p)], ["old", "edge", "new", "nots", "naive"])
            self.assertEqual([r["tx_hash"] for r in il.read_csv_rows(p, t0.isoformat())], ["new", "nots", "naive"])

    def test_fallback_timestamp_column_for_tables_without_updated_at(self):
        t0 = dt.datetime(2026, 10, 9, 0, 20, tzinfo=dt.timezone.utc)
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "l4x_perp_position_snapshot.csv"
            with p.open("w", newline="", encoding="utf-8") as fh:
                w = csv.DictWriter(fh, fieldnames=["wallet_address", "coin", "as_of_ts"])
                w.writeheader()
                w.writerows([
                    {"wallet_address": "0xa", "coin": "BTC", "as_of_ts": (t0 - dt.timedelta(days=1)).isoformat()},
                    {"wallet_address": "0xa", "coin": "ETH", "as_of_ts": (t0 + dt.timedelta(hours=2)).isoformat()},
                ])
            rows = il.read_csv_rows(p, t0.isoformat(), fallback_ts_col="as_of_ts")
            self.assertEqual([r["coin"] for r in rows], ["ETH"])
            # no usable timestamp column at all -> nothing is filtered out
            rows = il.read_csv_rows(p, t0.isoformat(), fallback_ts_col="nope")
            self.assertEqual(len(rows), 2)

    def test_since_with_margin(self):
        self.assertIsNone(il.since_with_margin(None))
        self.assertIsNone(il.since_with_margin("garbage"))
        out = il.since_with_margin("2026-10-09T00:20:00+00:00")
        self.assertEqual(out, "2026-10-08T23:20:00+00:00")

    def test_lift_state_round_trip(self):
        with tempfile.TemporaryDirectory() as d:
            self.assertEqual(il.read_lift_state(Path(d)), {})
            il.write_lift_state(Path(d), "2026-10-09T00:20:00+00:00",
                                {"l1_wallet_ledger": {"incoming": 5, "written": 3}, "_since": None,
                                 "_watermarks": {"l1_wallet_ledger": "written"}})
            st = il.read_lift_state(Path(d))
            self.assertEqual(st["last_success_run_iso"], "2026-10-09T00:20:00+00:00")
            self.assertEqual(st["tables"], {"l1_wallet_ledger": 3})
            (Path(d) / il.LIFT_STATE_FILE).write_text("{not json", encoding="utf-8")
            self.assertEqual(il.read_lift_state(Path(d)), {})

    def test_table_row_count_uses_metadata_only(self):
        class Snap:
            def __init__(self, summary):
                self.summary = summary

        class File:
            def __init__(self, n):
                self.record_count = n

        class Task:
            def __init__(self, n):
                self.file = File(n)

        class Scan:
            def __init__(self, counts):
                self._counts = counts

            def plan_files(self):
                return [Task(n) for n in self._counts]

            def to_arrow(self):
                raise AssertionError("row count must never read data")

        class Table:
            def __init__(self, snap, counts=()):
                self._snap, self._counts = snap, counts

            def current_snapshot(self):
                return self._snap

            def scan(self):
                return Scan(self._counts)

        self.assertEqual(il._table_row_count(Table(None)), 0)
        self.assertEqual(il._table_row_count(Table(Snap({"total-records": "42"}))), 42)
        self.assertEqual(il._table_row_count(Table(Snap({}), counts=(10, 32))), 42)     # manifest fallback
        self.assertEqual(il._table_row_count(Table(Snap(None), counts=(1,))), 1)


class RestPropsTests(unittest.TestCase):
    def test_rest_props_defaults(self):
        env = {"NESSIE_URI": "http://nessie:19120/iceberg/", "NESSIE_BRANCH": "", "WAREHOUSE": "",
               "S3_ENDPOINT": "http://10.0.0.5:9000", "S3_ACCESS_KEY": "k", "S3_SECRET_KEY": "s",
               "S3_REGION": "", "AWS_REGION": "", "HL_PY_IO_IMPL": "", "NESSIE_TOKEN": ""}
        with mock.patch.dict(os.environ, env, clear=False):
            p = il._rest_props(os.environ["NESSIE_URI"])
        self.assertEqual(p["uri"], "http://nessie:19120/iceberg")
        self.assertEqual(p["prefix"], "main")
        self.assertEqual(p["warehouse"], "s3://warehouse")
        self.assertEqual(p["py-io-impl"], "pyiceberg.io.pyarrow.PyArrowFileIO")
        self.assertEqual(p["s3.region"], "us-east-1")
        self.assertEqual((p["s3.endpoint"], p["s3.access-key-id"], p["s3.secret-access-key"]),
                         ("http://10.0.0.5:9000", "k", "s"))
        self.assertNotIn("token", p)

    def test_rest_props_overrides(self):
        env = {"NESSIE_BRANCH": "dev", "WAREHOUSE": "s3://lake", "S3_REGION": "eu-west-1",
               "HL_PY_IO_IMPL": "pyiceberg.io.fsspec.FsspecFileIO", "NESSIE_TOKEN": "t0k"}
        with mock.patch.dict(os.environ, env, clear=False):
            p = il._rest_props("http://n:19120/iceberg")
        self.assertEqual((p["prefix"], p["warehouse"], p["s3.region"], p["py-io-impl"], p["token"]),
                         ("dev", "s3://lake", "eu-west-1", "pyiceberg.io.fsspec.FsspecFileIO", "t0k"))


class PreflightIcebergTests(unittest.TestCase):
    def test_tcp_reachable(self):
        srv = socket.socket()
        srv.bind(("127.0.0.1", 0))
        srv.listen(1)
        port = srv.getsockname()[1]
        try:
            self.assertTrue(preflight._tcp_reachable(f"http://127.0.0.1:{port}", timeout=2))
        finally:
            srv.close()
        self.assertFalse(preflight._tcp_reachable(f"http://127.0.0.1:{port}", timeout=1))   # closed now
        self.assertFalse(preflight._tcp_reachable("http://", timeout=1))

    def _fake_catalog(self, props, tables=()):
        class Cat:
            properties = props

            def list_namespaces(self):
                return [("hypercore",), ("control",)]

            def list_tables(self, ns):
                return [("hypercore", t) for t in tables]
        return Cat()

    def test_unreachable_advertised_endpoint_fails(self):
        cat = self._fake_catalog({"s3.endpoint": "http://172.17.0.1:9000",
                                  "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO"})
        with mock.patch.dict(os.environ, {"NESSIE_URI": "http://olympus:19120/iceberg", "WAREHOUSE": "s3://warehouse"}), \
                mock.patch.object(il, "load_catalog_from_env", return_value=(cat, "rest")), \
                mock.patch.object(preflight, "_tcp_reachable", return_value=False):
            c = preflight.check_iceberg(required=True)
        self.assertEqual(c.status, "FAIL")
        self.assertIn("172.17.0.1:9000", c.detail)
        self.assertIn("external-endpoint", c.detail)

    def test_fsspec_without_s3fs_fails_and_pyarrow_passes(self):
        tables = list(il.TABLE_PLAN)
        cat = self._fake_catalog({"s3.endpoint": "http://10.0.0.7:9000",
                                  "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO"}, tables)
        import builtins
        real_import = builtins.__import__

        def no_s3fs(name, *a, **k):
            if name == "s3fs":
                raise ModuleNotFoundError("s3fs")
            return real_import(name, *a, **k)

        with mock.patch.dict(os.environ, {"NESSIE_URI": "http://olympus:19120/iceberg", "WAREHOUSE": "s3://warehouse"}), \
                mock.patch.object(il, "load_catalog_from_env", return_value=(cat, "rest")), \
                mock.patch.object(preflight, "_tcp_reachable", return_value=True), \
                mock.patch.object(builtins, "__import__", side_effect=no_s3fs):
            c = preflight.check_iceberg(required=True)
        self.assertEqual(c.status, "FAIL")
        self.assertIn("s3fs", c.detail)

        cat_ok = self._fake_catalog({"s3.endpoint": "http://10.0.0.7:9000",
                                     "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO"}, tables)
        with mock.patch.dict(os.environ, {"NESSIE_URI": "http://olympus:19120/iceberg", "WAREHOUSE": "s3://warehouse"}), \
                mock.patch.object(il, "load_catalog_from_env", return_value=(cat_ok, "rest")), \
                mock.patch.object(preflight, "_tcp_reachable", return_value=True):
            c = preflight.check_iceberg(required=True)
        self.assertEqual(c.status, "OK")
        self.assertIn(f"hypercore tables {len(tables)}/{len(tables)}", c.detail)


if __name__ == "__main__":
    unittest.main()
