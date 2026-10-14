"""Iceberg lift plumbing for a REST (Nessie) deployment: bounded incremental lifts,
metadata-only row counts, REST client properties, lift-state marker, and the
preflight checks that catch an unreachable advertised S3 endpoint or a missing
FileIO dependency. No network — catalogs are faked."""
from __future__ import annotations

import csv
import datetime as dt
import io
import json
import os
import socket
import tempfile
import threading
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

from blueblocx_hyperliquid import duckdb_smoke, iceberg_load as il
from blueblocx_hyperliquid import preflight


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

    def test_lift_since_never_excludes_the_current_runs_rows(self):
        t0 = "2026-10-09T00:20:00+00:00"
        # a scheduled lift finished 3 h after this (long, hand-run) run started: the
        # cut-off must still be this run's own stamp minus the margin
        self.assertEqual(il.lift_since("2026-10-09T03:20:00+00:00", t0), "2026-10-08T23:20:00+00:00")
        # the normal case: the previous lift is older than this run
        self.assertEqual(il.lift_since("2026-10-08T23:00:00+00:00", t0), il.since_with_margin("2026-10-08T23:00:00+00:00"))
        self.assertIsNone(il.lift_since(None, t0))
        # datetimes are compared, not strings: a microsecond-bearing marker vs a plain one
        self.assertEqual(il.lift_since("2026-10-09T00:20:00.000000+00:00", "2026-10-09T00:20:00+00:00"),
                         "2026-10-08T23:20:00+00:00")

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


class CatalogModeTests(unittest.TestCase):
    def _env(self, **kw):
        base = {"NESSIE_URI": "", "HL_CATALOG_URI": "", "WAREHOUSE": "", "S3_ENDPOINT": "", "S3_ACCESS_KEY": "",
                "S3_SECRET_KEY": "", "S3_REGION": "", "AWS_REGION": "", "HL_PY_IO_IMPL": ""}
        base.update(kw)
        return mock.patch.dict(os.environ, base, clear=False)

    def test_catalog_mode_precedence(self):
        with self._env():
            self.assertEqual(il.catalog_mode(), "local")
        with self._env(HL_CATALOG_URI="sqlite:///x.db"):
            self.assertEqual(il.catalog_mode(), "sql")
        with self._env(HL_CATALOG_URI="sqlite:///x.db", NESSIE_URI="http://n:19120/iceberg"):
            self.assertEqual(il.catalog_mode(), "rest")

    def test_warehouse_local_path(self):
        self.assertEqual(il.warehouse_local_path("file:///mnt/blueblocx/hypercore/warehouse"),
                         Path("/mnt/blueblocx/hypercore/warehouse"))
        self.assertEqual(il.warehouse_local_path("file://localhost/mnt/x"), Path("/mnt/x"))
        self.assertEqual(il.warehouse_local_path("file://D:/BlueBlocx/out/iceberg/warehouse"),
                         Path("D:/BlueBlocx/out/iceberg/warehouse"))
        self.assertEqual(il.warehouse_local_path("file:///D:/x/wh"), Path("D:/x/wh"))
        self.assertIsNone(il.warehouse_local_path("file://nas/share/wh"))
        self.assertIsNone(il.warehouse_local_path("s3://warehouse"))

    def test_sql_props(self):
        with self._env(HL_CATALOG_URI="postgresql+psycopg2://hl:pw@localhost:5432/hl_catalog",
                       WAREHOUSE="file:///mnt/blueblocx/hypercore/warehouse"):
            p = il._sql_props(os.environ["HL_CATALOG_URI"])
        self.assertEqual((p["type"], p["warehouse"], p["py-io-impl"]),
                         ("sql", "file:///mnt/blueblocx/hypercore/warehouse", il.DEFAULT_PY_IO_IMPL))
        self.assertNotIn("s3.region", p)                        # no S3 props for a file:// warehouse
        with self._env(HL_CATALOG_URI="sqlite:///c.db", WAREHOUSE="s3://hl-lake", S3_ENDPOINT="http://10.0.0.9:9000",
                       S3_ACCESS_KEY="k", S3_SECRET_KEY="s"):
            p = il._sql_props(os.environ["HL_CATALOG_URI"])
        self.assertEqual((p["s3.endpoint"], p["s3.region"], p["s3.access-key-id"]), ("http://10.0.0.9:9000", "us-east-1", "k"))
        with tempfile.TemporaryDirectory() as d, self._env(HL_CATALOG_URI="sqlite:///c.db"):
            p = il._sql_props(os.environ["HL_CATALOG_URI"], local_dir=Path(d))
        self.assertTrue(p["warehouse"].startswith("file://") and p["warehouse"].endswith("/warehouse"))

    def test_redacted_uri_hides_password(self):
        out = il._redacted_uri("postgresql+psycopg2://hl:s3cr3t@db.local:5432/hl_catalog")
        self.assertNotIn("s3cr3t", out)
        self.assertIn("db.local", out)

    def test_sql_mode_lift_writes_data_to_warehouse_dir(self):
        """SQL catalog (SQLite stands in for PostgreSQL) + a file:// warehouse that is NOT
        under HL_OUT_DIR: the data files must land in the warehouse directory."""
        from blueblocx_hyperliquid import schema as S
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            wh = root / "nas" / "warehouse"
            cat = root / "cat"
            cat.mkdir()
            src = root / "staging"
            src.mkdir()
            now = dt.datetime(2026, 10, 12, 1, 0, tzinfo=dt.timezone.utc).isoformat()
            with (src / "token_whitelist.csv").open("w", newline="", encoding="utf-8") as fh:
                w = csv.DictWriter(fh, fieldnames=S.TOKEN_WHITELIST_COLUMNS)
                w.writeheader()
                w.writerow({"token_address": "hlcore:token:1", "token_group": 7, "is_active": True,
                            "whitelist_reason": "hypercore_token", "created_at": now, "updated_at": now})
            env = {"HL_CATALOG_URI": f"sqlite:///{(cat / 'catalog.db').as_posix()}",
                   "WAREHOUSE": f"file://{wh.as_posix()}", "HL_OUT_DIR": str(root / "out")}
            with self._env(**env):
                self.assertEqual(il.catalog_mode(), "sql")
                catalog, mode = il.load_catalog_from_env()
                self.assertEqual(mode, "sql")
                try:
                    rep = il.lift_directory(src, catalog=catalog)
                finally:
                    catalog.engine.dispose()          # SQLite keeps the file open on Windows
            self.assertEqual(rep["token_whitelist"]["written"], 1)
            parquet = list(wh.rglob("*.parquet"))
            self.assertTrue(parquet, "data files must be under the WAREHOUSE directory")
            self.assertFalse(list((root / "out").rglob("*.parquet")) if (root / "out").exists() else [])

    def test_local_mode_warehouse_override_and_s3_ignored(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            with self._env(WAREHOUSE=f"file://{(root / 'wh2').as_posix()}"):
                catalog, mode = il.load_catalog_from_env(local_dir=root / "cat")
            catalog.engine.dispose()
            self.assertEqual(mode, "local")
            self.assertTrue(catalog.properties["warehouse"].endswith("/wh2"))
            self.assertTrue((root / "wh2").is_dir())
            with self._env(WAREHOUSE="s3://warehouse"):
                catalog, mode = il.load_catalog_from_env(local_dir=root / "cat2")
            catalog.engine.dispose()
            self.assertEqual(mode, "local")
            self.assertIn("/cat2/warehouse", catalog.properties["warehouse"])


def _sqlite_env(root: Path) -> dict:
    return {"NESSIE_URI": "", "HL_CATALOG_URI": f"sqlite:///{(root / 'c.db').as_posix()}",
            "WAREHOUSE": f"file://{(root / 'wh').as_posix()}", "HL_WAREHOUSE_MOUNT": ""}


def _write_rows(path: Path, cols: list[str], rows: list[dict]) -> None:
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=cols)
        w.writeheader()
        w.writerows(rows)


def _committed_states(table) -> list[tuple[str, int]]:
    """(operation, current snapshot id) of every metadata.json this table ever had,
    oldest first - i.e. every state a reader of the catalog pointer could observe."""
    import json as _json
    states = []
    locations = [e.metadata_file for e in table.metadata.metadata_log] + [table.metadata_location]
    for loc in locations:
        p = il.warehouse_local_path(loc)
        meta = _json.loads(Path(p).read_text(encoding="utf-8"))
        sid = meta.get("current-snapshot-id")
        if sid is None or sid == -1:
            states.append(("empty", None))
            continue
        snap = next(s for s in meta["snapshots"] if s["snapshot-id"] == sid)
        states.append((snap["summary"]["operation"], sid))
    return states


class CommitFanOutTests(unittest.TestCase):
    """Hourly cadence lives or dies by the number of Iceberg commits per lift, and the
    ETL's DuckDB reads the catalog pointer: every committed state must be complete."""

    def _l4x_rows(self, wallets, as_of_date, as_of_ts):
        from blueblocx_hyperliquid import schema as S
        rows = []
        for w in wallets:
            for coin in ("BTC", "ETH"):
                r = {c: "" for c in S.L4X_PERP_POSITION_SNAPSHOT_COLUMNS}
                r.update({"as_of_date": as_of_date, "wallet_address": w, "coin": coin, "as_of_ts": as_of_ts,
                          "dex": "main", "unrealized_pnl_usd": "1.5", "created_at": as_of_ts})
                rows.append(r)
        return rows

    def test_replace_scope_is_one_commit_per_lift_and_reader_never_sees_missing_rows(self):
        from blueblocx_hyperliquid import schema as S
        wallets = [f"0x{i:040x}" for i in range(1, 8)]          # 7 wallets, same day
        day = "2026-10-12"
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            src = root / "staging"
            src.mkdir()
            cols = S.L4X_PERP_POSITION_SNAPSHOT_COLUMNS
            _write_rows(src / "l4x_perp_position_snapshot.csv", cols,
                        self._l4x_rows(wallets, day, "2026-10-12T01:00:00+00:00"))
            with mock.patch.dict(os.environ, _sqlite_env(root)):
                catalog, _ = il.load_catalog_from_env()
                try:
                    rep1 = il.lift_directory(src, catalog=catalog)                 # empty table: plain append
                    l4x = catalog.load_table("hypercore.l4x_perp_position_snapshot")
                    log_after_1, loc_after_1 = len(l4x.metadata.metadata_log), l4x.metadata_location
                    _write_rows(src / "l4x_perp_position_snapshot.csv", cols,
                                self._l4x_rows(wallets, day, "2026-10-12T02:00:00+00:00"))
                    rep2 = il.lift_directory(src, catalog=catalog)                 # replaces the day for all 7
                    l4x = catalog.load_table("hypercore.l4x_perp_position_snapshot")
                    wm = catalog.load_table("control.table_watermarks")
                    states = _committed_states(l4x)
                    wm_states = _committed_states(wm)
                    rows_per_state = {sid: l4x.scan(snapshot_id=sid).to_arrow().num_rows
                                      for op, sid in states if sid is not None}
                    total = il._table_row_count(l4x)
                    props = dict(l4x.properties)
                    log_after_2 = len(l4x.metadata.metadata_log)
                    prev_file = l4x.metadata.metadata_log[-1].metadata_file
                finally:
                    catalog.engine.dispose()
            self.assertEqual(rep1["l4x_perp_position_snapshot"]["written"], 14)
            self.assertEqual(rep2["l4x_perp_position_snapshot"]["commits"], 1)
            self.assertEqual(rep2["l4x_perp_position_snapshot"]["replaced_scopes"], 7)
            self.assertEqual(total, 14)                                                   # replaced, not doubled
            # exactly ONE catalog commit for the replace: the metadata log grew by one and
            # the previous pointer is the state lift 1 left behind
            self.assertEqual(log_after_2, log_after_1 + 1)
            self.assertEqual(prev_file, loc_after_1)
            # every state a reader could have fetched is an append holding all 14 rows
            self.assertTrue(all(op == "append" for op, sid in states if sid is not None), states)
            self.assertEqual(set(rows_per_state.values()), {14})
            self.assertEqual(rep2["_watermarks"], {"l4x_perp_position_snapshot": "written"})
            # the control table: its creation (empty) + one commit per lift
            self.assertEqual([op for op, sid in wm_states], ["empty", "append", "append"])
            self.assertEqual(props.get("write.metadata.delete-after-commit.enabled"), "true")

    def test_replace_keys_is_one_commit_and_keys_never_vanish(self):
        from blueblocx_hyperliquid import schema as S
        cols = S.TOKEN_WHITELIST_COLUMNS

        def rows(n, stamp, start=0):
            return [{"token_address": f"hlcore:token:{i}", "token_group": i % 4000, "is_active": True,
                     "whitelist_reason": "hypercore_token", "created_at": stamp, "updated_at": stamp}
                    for i in range(start, start + n)]
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            src = root / "staging"
            src.mkdir()
            with mock.patch.dict(os.environ, _sqlite_env(root)):
                catalog, _ = il.load_catalog_from_env()
                try:
                    _write_rows(src / "token_whitelist.csv", cols, rows(600, "2026-10-12T01:00:00+00:00"))
                    il.lift_directory(src, catalog=catalog)                                   # 600 keys, one In()
                    _write_rows(src / "token_whitelist.csv", cols, rows(605, "2026-10-12T02:00:00+00:00"))
                    rep2 = il.lift_directory(src, catalog=catalog)                            # 600 replaced + 5 new
                    _write_rows(src / "token_whitelist.csv", cols, rows(10, "2026-10-12T03:00:00+00:00", 100))
                    rep3 = il.lift_directory(src, catalog=catalog)
                    t = catalog.load_table("hypercore.token_whitelist")
                    states = _committed_states(t)
                    keys_per_state = {sid: set(t.scan(snapshot_id=sid, selected_fields=("token_address",))
                                               .to_arrow().column("token_address").to_pylist())
                                      for op, sid in states if sid is not None}
                    total = il._table_row_count(t)
                finally:
                    catalog.engine.dispose()
            self.assertEqual((rep2["token_whitelist"]["commits"], rep3["token_whitelist"]["commits"]), (1, 1))
            self.assertEqual(total, 605)
            # table creation (empty) + exactly one committed state per lift, each an append
            self.assertEqual([op for op, sid in states], ["empty", "append", "append", "append"])
            original = {f"hlcore:token:{i}" for i in range(600)}
            for sid, keys in keys_per_state.items():
                self.assertTrue(original <= keys, "a committed state lacks keys that were present before")

    def test_failed_commit_leaves_previous_snapshot_intact(self):
        from pyiceberg.exceptions import CommitFailedException
        from blueblocx_hyperliquid import schema as S
        wallets = [f"0x{i:040x}" for i in range(1, 8)]
        cols = S.L4X_PERP_POSITION_SNAPSHOT_COLUMNS
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            src = root / "staging"
            src.mkdir()
            _write_rows(src / "l4x_perp_position_snapshot.csv", cols,
                        self._l4x_rows(wallets, "2026-10-12", "2026-10-12T01:00:00+00:00"))
            with mock.patch.dict(os.environ, _sqlite_env(root)):
                catalog, _ = il.load_catalog_from_env()
                try:
                    il.lift_directory(src, catalog=catalog)
                    t = catalog.load_table("hypercore.l4x_perp_position_snapshot")
                    loc1, log1 = t.metadata_location, len(t.metadata.metadata_log)
                    _write_rows(src / "l4x_perp_position_snapshot.csv", cols,
                                self._l4x_rows(wallets, "2026-10-12", "2026-10-12T02:00:00+00:00"))
                    with mock.patch.object(type(catalog), "commit_table", side_effect=CommitFailedException("injected")):
                        with self.assertRaises(CommitFailedException):
                            il.lift_directory(src, catalog=catalog, watermarks=False)
                    t = catalog.load_table("hypercore.l4x_perp_position_snapshot")
                    self.assertEqual((t.metadata_location, len(t.metadata.metadata_log)), (loc1, log1))
                    self.assertEqual(il._table_row_count(t), 14)
                finally:
                    catalog.engine.dispose()

    def test_failed_table_writes_failed_watermark_and_reraises(self):
        from blueblocx_hyperliquid import schema as S
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            src = root / "staging"
            src.mkdir()
            now = "2026-10-12T01:00:00+00:00"
            _write_rows(src / "token_whitelist.csv", S.TOKEN_WHITELIST_COLUMNS,
                        [{"token_address": "hlcore:token:1", "token_group": 7, "is_active": True,
                          "whitelist_reason": "x", "created_at": now, "updated_at": now}])
            _write_rows(src / "l4x_perp_position_snapshot.csv", S.L4X_PERP_POSITION_SNAPSHOT_COLUMNS,
                        self._l4x_rows(["0x" + "1" * 40], "2026-10-12", now))
            real_load = il.load_table

            def boom(table, *a, **kw):
                if "l4x" in il._ident(table):
                    raise RuntimeError("injected")
                return real_load(table, *a, **kw)
            with mock.patch.dict(os.environ, _sqlite_env(root)):
                catalog, _ = il.load_catalog_from_env()
                try:
                    with mock.patch.object(il, "load_table", side_effect=boom):
                        with self.assertRaises(RuntimeError):
                            il.lift_directory(src, catalog=catalog)
                    wm = {r["table_name"]: r for r in catalog.load_table("control.table_watermarks")
                          .scan().to_arrow().to_pylist()}
                    self.assertEqual(wm["token_whitelist"]["status"], il.WM_STATUS_OK)
                    self.assertEqual(wm["l4x_perp_position_snapshot"]["status"], il.WM_STATUS_FAILED)
                    self.assertIsNone(wm["l4x_perp_position_snapshot"]["watermark_ts"])
                    rep = il.lift_directory(src, catalog=catalog)                # unpatched: heals the row
                    wm = {r["table_name"]: r for r in catalog.load_table("control.table_watermarks")
                          .scan().to_arrow().to_pylist()}
                    self.assertEqual(wm["l4x_perp_position_snapshot"]["status"], il.WM_STATUS_OK)
                    self.assertEqual(rep["_watermarks"]["l4x_perp_position_snapshot"], "written")
                finally:
                    catalog.engine.dispose()

    def test_failed_watermark_row_keeps_the_previous_value_column_and_count(self):
        # a failed lift must not move the cursor the ETL reads NOR change what it was measured on
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            good_ts = dt.datetime(2026, 10, 12, 8, 0, tzinfo=dt.timezone.utc)
            with mock.patch.dict(os.environ, _sqlite_env(root)):
                catalog, _ = il.load_catalog_from_env()
                try:
                    il.write_watermarks(catalog, "hypercore",
                                        [("t1", "updated_at", good_ts, 5, 11, dt.date(2026, 10, 10), il.WM_STATUS_OK)],
                                        "run-1")
                    il.write_watermarks(catalog, "hypercore",
                                        [("t1", "created_at", None, None, None, None, il.WM_STATUS_FAILED)], "run-2")
                    rows = catalog.load_table("control.table_watermarks").scan().to_arrow().to_pylist()
                finally:
                    catalog.engine.dispose()
            self.assertEqual(len(rows), 1)                                  # one row per table, replaced
            wm = rows[0]
            self.assertEqual(wm["status"], il.WM_STATUS_FAILED)
            self.assertEqual(wm["watermark_col"], "updated_at")              # not the failed row's column
            self.assertEqual(wm["watermark_ts"], dt.datetime(2026, 10, 12, 8, 0))
            self.assertEqual(wm["watermark_date"], dt.date(2026, 10, 10))
            self.assertEqual(wm["row_count"], 5)

    def test_watermark_ts_is_write_stamp_and_date_is_business_date(self):
        from blueblocx_hyperliquid import schema as S
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            src = root / "staging"
            src.mkdir()
            row = {c: "" for c in S.L1_WALLET_LEDGER_COLUMNS}
            row.update({"tx_hash": "hlcore:0xa:tid:1", "event_index": 0, "direction": "in", "event_type": "spot_fill",
                        "wallet_address": "0x" + "a" * 40, "token_address": "hlcore:usdc",
                        "block_timestamp": "2026-10-10T05:00:00+00:00", "block_date": "2026-10-10",
                        "amount_signed": "1", "created_at": "2026-10-12T08:00:00+00:00",
                        "updated_at": "2026-10-12T08:00:00+00:00"})
            _write_rows(src / "l1_wallet_ledger.csv", S.L1_WALLET_LEDGER_COLUMNS, [row])
            with mock.patch.dict(os.environ, _sqlite_env(root)):
                catalog, _ = il.load_catalog_from_env()
                try:
                    il.lift_directory(src, catalog=catalog)
                    wm = catalog.load_table("control.table_watermarks").scan().to_arrow().to_pylist()[0]
                finally:
                    catalog.engine.dispose()
            self.assertEqual(wm["watermark_col"], "updated_at")
            self.assertEqual(wm["watermark_ts"], dt.datetime(2026, 10, 12, 8, 0))        # naive UTC write stamp
            self.assertEqual(wm["watermark_date"], dt.date(2026, 10, 10))                # business date
            self.assertEqual(wm["status"], il.WM_STATUS_OK)

    def test_null_in_required_column_or_duplicate_key_fails_before_any_commit(self):
        from blueblocx_hyperliquid import schema as S
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            src = root / "staging"
            src.mkdir()
            now = "2026-10-12T01:00:00+00:00"
            bad = {"token_address": "hlcore:token:1", "token_group": 7, "is_active": "",     # NULL is_active
                   "whitelist_reason": "x", "created_at": now, "updated_at": now}
            _write_rows(src / "token_whitelist.csv", S.TOKEN_WHITELIST_COLUMNS, [bad])
            with mock.patch.dict(os.environ, _sqlite_env(root)):
                catalog, _ = il.load_catalog_from_env()
                try:
                    with self.assertRaises(ValueError) as cm:
                        il.lift_directory(src, catalog=catalog, watermarks=False)
                    self.assertIn("is_active", str(cm.exception))
                    t = catalog.load_table("hypercore.token_whitelist")
                    self.assertIsNone(t.current_snapshot())                               # nothing committed
                    good = dict(bad, is_active=True)
                    _write_rows(src / "token_whitelist.csv", S.TOKEN_WHITELIST_COLUMNS, [good, dict(good)])
                    with self.assertRaises(ValueError) as cm:                              # duplicate key in batch
                        il.lift_directory(src, catalog=catalog, watermarks=False)
                    self.assertIn("duplicate", str(cm.exception))
                    # a fresh table carries required=True for the by-construction columns
                    self.assertTrue(t.schema().find_field("is_active").required)
                    self.assertTrue(t.schema().find_field("token_address").required)
                    self.assertFalse(t.schema().find_field("whitelist_reason").required)
                    _write_rows(src / "token_whitelist.csv", S.TOKEN_WHITELIST_COLUMNS, [good])
                    rep = il.lift_directory(src, catalog=catalog)                           # valid row lands
                    self.assertEqual(rep["token_whitelist"]["written"], 1)
                finally:
                    catalog.engine.dispose()

    def test_restated_status_and_iceberg_load_main_lock(self):
        from blueblocx_hyperliquid import schema as S
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            src = root / "staging"
            src.mkdir()
            now = "2026-10-12T01:00:00+00:00"
            _write_rows(src / "token_whitelist.csv", S.TOKEN_WHITELIST_COLUMNS,
                        [{"token_address": "hlcore:token:1", "token_group": 7, "is_active": True,
                          "whitelist_reason": "x", "created_at": now, "updated_at": now}])
            env = dict(_sqlite_env(root), HL_OUT_DIR=str(root / "out"))
            with mock.patch.dict(os.environ, env):
                with redirect_stdout(io.StringIO()):
                    self.assertEqual(il.main(["--from", str(src), "--full"]), 0)
                catalog, _ = il.load_catalog_from_env()
                try:
                    wm = catalog.load_table("control.table_watermarks").scan().to_arrow().to_pylist()[0]
                    self.assertEqual(wm["status"], il.WM_STATUS_RESTATED)
                finally:
                    catalog.engine.dispose()
                self.assertTrue((src / il.LIFT_STATE_FILE).exists())
                # a live lock on the state dir: the hand-run lift yields with exit 3 and lifts nothing
                import socket
                from blueblocx_hyperliquid import ops
                lock = src / ".daily.lock"
                lock.write_text(json.dumps({"pid": os.getpid(), "host": socket.gethostname()}), encoding="utf-8")
                with mock.patch.object(il, "lift_directory", side_effect=AssertionError("must not lift")):
                    self.assertEqual(il.main(["--from", str(src)]), 3)
                self.assertTrue(lock.exists())
                lock.unlink()
                self.assertIsInstance(ops.RunLock(lock), ops.RunLock)

    def test_write_watermarks_batch_is_monotonic_per_table(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            env = {"NESSIE_URI": "", "HL_CATALOG_URI": f"sqlite:///{(root / 'c.db').as_posix()}",
                   "WAREHOUSE": f"file://{(root / 'wh').as_posix()}", "HL_WAREHOUSE_MOUNT": ""}
            t_new = dt.datetime(2026, 10, 12, 3, tzinfo=dt.timezone.utc)
            t_old = dt.datetime(2026, 10, 11, 3, tzinfo=dt.timezone.utc)
            with mock.patch.dict(os.environ, env):
                catalog, _ = il.load_catalog_from_env()
                try:
                    s1 = il.write_watermarks(catalog, "hypercore",
                                             [("a", "block_timestamp", t_new, 10, 1), ("b", "block_timestamp", t_old, 5, 2)], "r1")
                    s2 = il.write_watermarks(catalog, "hypercore",
                                             [("a", "block_timestamp", t_old, 11, 3), ("b", "block_timestamp", t_new, 6, 4)], "r2")
                    tbl = catalog.load_table("control.table_watermarks")
                    rows = {r["table_name"]: r for r in tbl.scan().to_arrow().to_pylist()}
                finally:
                    catalog.engine.dispose()
            self.assertEqual(s1, {"a": "written", "b": "written"})
            self.assertEqual(s2, {"a": "kept_newer", "b": "written"})
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows["a"]["row_count"], 10)          # the older watermark did not overwrite
            self.assertEqual(rows["b"]["row_count"], 6)


class WarehouseMountTests(unittest.TestCase):
    ROOT_ONLY = "/dev/nvme0n1p5 / ext4 rw,relatime 0 0\n"
    NAS = ROOT_ONLY + "192.168.1.121:/volume1/blueblocx /mnt/blueblocx nfs4 rw,vers=4.1 0 0\n"
    WH = Path("/mnt/blueblocx/hypercore/warehouse")

    def test_require_warehouse_mount(self):
        with mock.patch.dict(os.environ, {"HL_WAREHOUSE_MOUNT": "/mnt/blueblocx"}):
            with self.assertRaises(RuntimeError) as cm:
                il.require_warehouse_mount(self.WH, mounts_text=self.ROOT_ONLY)
            self.assertIn("nothing is mounted at /mnt/blueblocx", str(cm.exception))
            self.assertIn("HL_WAREHOUSE_MOUNT=/mnt/blueblocx", str(cm.exception))
            il.require_warehouse_mount(self.WH, mounts_text=self.NAS)                     # plain host: no raise
            # inside the systemd unit (ProtectSystem=strict + ReadWritePaths) the warehouse
            # directory is its own bind mount below the NAS mount point - the production
            # failure of 2026-10-13: must pass
            in_unit = (self.NAS.replace("rw,vers=4.1", "ro,vers=4.1")
                       + "192.168.1.121:/volume1/blueblocx /mnt/blueblocx/hypercore nfs4 rw,vers=4.1 0 0\n"
                       + "/dev/nvme0n1p5 /var/lib/hypercore ext4 rw 0 0\n")
            il.require_warehouse_mount(self.WH, mounts_text=in_unit)
            # NAS unmounted: the ReadWritePaths bind mount of a root-disk directory
            with self.assertRaises(RuntimeError):
                il.require_warehouse_mount(self.WH, mounts_text=self.ROOT_ONLY
                                           + "/dev/nvme0n1p5 /mnt/blueblocx/hypercore ext4 rw 0 0\n")
            with self.assertRaises(RuntimeError) as cm:
                il.require_warehouse_mount(self.WH, mounts_text=self.ROOT_ONLY
                                           + "/dev/nvme0n1p5 /mnt/blueblocx ext4 rw 0 0\n")
            self.assertIn("bind mount of the root disk", str(cm.exception))
            # a warehouse path outside the NAS subtree
            with self.assertRaises(RuntimeError) as cm:
                il.require_warehouse_mount(Path("/data/warehouse"), mounts_text=self.NAS)
            self.assertIn("not on /mnt/blueblocx", str(cm.exception))
            from blueblocx_hyperliquid import ops
            with mock.patch.object(ops, "mount_entries", return_value=None):
                il.require_warehouse_mount(self.WH)                                       # Windows: no table
        with mock.patch.dict(os.environ, {"HL_WAREHOUSE_MOUNT": "/data"}):
            local = self.ROOT_ONLY + "/dev/sdb1 /data ext4 rw 0 0\n"
            il.require_warehouse_mount(Path("/data/hypercore/warehouse"), mounts_text=local)   # second disk
            il.require_warehouse_mount(Path("/data/hypercore/warehouse"),
                                       mounts_text=local + "/dev/sdb1 /data/hypercore ext4 rw 0 0\n")
        with mock.patch.dict(os.environ, {"HL_WAREHOUSE_MOUNT": ""}):
            il.require_warehouse_mount(Path("/anything"), mounts_text=self.ROOT_ONLY)        # unset: no-op

    def test_preflight_fails_when_nas_not_mounted(self):
        from blueblocx_hyperliquid import ops
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            env = {"NESSIE_URI": "", "HL_CATALOG_URI": f"sqlite:///{(root / 'c.db').as_posix()}",
                   "WAREHOUSE": f"file://{(root / 'wh').as_posix()}", "HL_WAREHOUSE_MOUNT": "/mnt/blueblocx"}
            with mock.patch.dict(os.environ, env), \
                    mock.patch.object(ops, "mount_entries", return_value=ops.mount_entries(self.ROOT_ONLY)):
                c = preflight.check_iceberg(required=True)
            self.assertEqual(c.status, "FAIL")
            self.assertIn("HL_WAREHOUSE_MOUNT=/mnt/blueblocx", c.detail)
            self.assertFalse((root / "wh").exists())      # nothing was created on the wrong disk


class MaintenanceTests(unittest.TestCase):
    def test_expire_snapshots_keeps_current(self):
        from blueblocx_hyperliquid import maintenance
        from blueblocx_hyperliquid import schema as S
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            src = root / "staging"
            src.mkdir()
            env = {"NESSIE_URI": "", "HL_CATALOG_URI": f"sqlite:///{(root / 'c.db').as_posix()}",
                   "WAREHOUSE": f"file://{(root / 'wh').as_posix()}", "HL_WAREHOUSE_MOUNT": ""}
            with mock.patch.dict(os.environ, env):
                catalog, _ = il.load_catalog_from_env()
                try:
                    for i in range(3):                        # three lifts -> three append snapshots
                        now = f"2026-10-12T0{i}:00:00+00:00"
                        with (src / "token_whitelist.csv").open("w", newline="", encoding="utf-8") as fh:
                            w = csv.DictWriter(fh, fieldnames=S.TOKEN_WHITELIST_COLUMNS)
                            w.writeheader()
                            w.writerow({"token_address": f"hlcore:token:{i}", "token_group": 1, "is_active": True,
                                        "whitelist_reason": "x", "created_at": now, "updated_at": now})
                        il.lift_directory(src, catalog=catalog, tables=["token_whitelist"])
                    tbl = catalog.load_table("hypercore.token_whitelist")
                    before = len(list(tbl.snapshots()))
                    rep = maintenance.run("hypercore", expire_days=0.0, catalog=catalog)
                    tbl.refresh()
                    after = len(list(tbl.snapshots()))
                    total = il._table_row_count(tbl)
                finally:
                    catalog.engine.dispose()
            self.assertGreaterEqual(before, 3)
            self.assertEqual(after, 1)                                   # only the current snapshot survives
            self.assertEqual(total, 3)                                   # data intact
            entry = rep["tables"]["hypercore.token_whitelist"]
            self.assertEqual(entry["expired"], before - 1)
            self.assertIn("skipped", rep["tables"]["hypercore.l1_wallet_ledger"])   # never created: skipped


class LiftOrderTests(unittest.TestCase):
    def test_table_plan_parents_before_facts_before_derived(self):
        names = list(il.TABLE_PLAN)
        for parent in ("l1_token_metadata", "token_whitelist", "wallet_watchlist"):
            self.assertLess(names.index(parent), names.index("l1_wallet_ledger"))
        self.assertLess(names.index("l1_wallet_ledger"), names.index("l4_wallet_metadata"))
        self.assertLess(names.index("l3_token_price_usd_1m"), names.index("l4x_perp_position_snapshot"))

    def test_partial_lift_never_leaves_an_orphan_ledger_row(self):
        """The ledger fails to lift: its token's metadata row must already be committed
        (parents first) and no ledger row may exist (facts are committed after)."""
        from blueblocx_hyperliquid import schema as S
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            src = root / "staging"
            src.mkdir()
            now = "2026-10-12T01:00:00+00:00"
            meta = {c: "" for c in S.L1_TOKEN_METADATA_COLUMNS}
            meta.update({"token_address": "hlcore:token:9", "token_group": 1, "standard": "ERC20", "symbol": "X",
                         "name": "X", "is_stablecoin": False, "created_at": now, "updated_at": now})
            _write_rows(src / "l1_token_metadata.csv", S.L1_TOKEN_METADATA_COLUMNS, [meta])
            row = {c: "" for c in S.L1_WALLET_LEDGER_COLUMNS}
            row.update({"tx_hash": "hlcore:0xa:tid:1", "event_index": 0, "direction": "in", "event_type": "spot_fill",
                        "wallet_address": "0x" + "a" * 40, "token_address": "hlcore:token:9",
                        "block_timestamp": now, "block_date": "2026-10-12", "amount_signed": "1",
                        "created_at": now, "updated_at": now})
            _write_rows(src / "l1_wallet_ledger.csv", S.L1_WALLET_LEDGER_COLUMNS, [row])
            order: list[str] = []
            real_load = il.load_table

            def record(table, *a, **kw):
                order.append(il._ident(table))
                if "l1_wallet_ledger" in order[-1]:
                    raise RuntimeError("injected")
                return real_load(table, *a, **kw)
            with mock.patch.dict(os.environ, _sqlite_env(root)):
                catalog, _ = il.load_catalog_from_env()
                try:
                    with mock.patch.object(il, "load_table", side_effect=record):
                        with self.assertRaises(RuntimeError):
                            il.lift_directory(src, catalog=catalog, watermarks=False)
                    self.assertEqual(order, ["hypercore.l1_token_metadata", "hypercore.l1_wallet_ledger"])
                    self.assertEqual(il._table_row_count(catalog.load_table("hypercore.l1_token_metadata")), 1)
                    self.assertEqual(il._table_row_count(catalog.load_table("hypercore.l1_wallet_ledger")), 0)
                finally:
                    catalog.engine.dispose()


class DuckDBSmokeTests(unittest.TestCase):
    def test_pointer_query_and_scan_variants(self):
        sql, params = duckdb_smoke.pointer_query("hl", "hypercore")
        self.assertIn("FROM iceberg_tables", sql)
        self.assertEqual(params, {"catalog_name": "hl", "namespace": "hypercore"})
        loc = "file:///mnt/blueblocx/hypercore/warehouse/hypercore.db/l1_wallet_ledger/metadata/00009-x.metadata.json"
        variants = duckdb_smoke.scan_variants(loc)
        self.assertEqual([v for v, _ in variants], ["raw", "stripped", "moved"])
        self.assertEqual(variants[0][1], f"iceberg_scan('{loc}')")
        self.assertEqual(variants[1][1], "iceberg_scan('/mnt/blueblocx/hypercore/warehouse/hypercore.db/l1_wallet_ledger"
                                         "/metadata/00009-x.metadata.json')")
        # the table root with the committed version pinned: never reads an uncommitted file
        self.assertEqual(variants[2][1], "iceberg_scan('/mnt/blueblocx/hypercore/warehouse/hypercore.db/l1_wallet_ledger', "
                                         "version = '00009-x', allow_moved_paths = true)")
        self.assertEqual(duckdb_smoke.table_root_and_version("s3://b/wh/ns/t/metadata/00001-abc.gz.metadata.json"),
                         ("s3://b/wh/ns/t", "00001-abc"))
        self.assertIsNone(duckdb_smoke.table_root_and_version("s3://bucket/m.json"))          # no metadata/ dir
        self.assertIsNone(duckdb_smoke.table_root_and_version("file:///x/metadata/.metadata.json"))
        self.assertEqual(duckdb_smoke.local_path("file://D:/x/wh/m.json"), "D:/x/wh/m.json")
        self.assertEqual(duckdb_smoke.local_path("s3://bucket/m.json"), "s3://bucket/m.json")
        self.assertEqual([v for v, _ in duckdb_smoke.scan_variants("s3://bucket/m.json")], ["raw"])
        self.assertEqual([v for v, _ in duckdb_smoke.scan_variants("s3://b/wh/ns/t/metadata/00001-abc.metadata.json")],
                         ["raw", "moved"])
        # a quote in a NAS path cannot break the statement
        self.assertIn("iceberg_scan('/mnt/Leo''s/wh/t/metadata/00001-a.metadata.json')",
                      dict(duckdb_smoke.scan_variants("file:///mnt/Leo's/wh/t/metadata/00001-a.metadata.json"))["stripped"])

    def test_pointer_select_against_the_sqlite_catalog(self):
        from blueblocx_hyperliquid import schema as S
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            src = root / "staging"
            src.mkdir()
            now = "2026-10-12T01:00:00+00:00"
            _write_rows(src / "token_whitelist.csv", S.TOKEN_WHITELIST_COLUMNS,
                        [{"token_address": "hlcore:token:1", "token_group": 7, "is_active": True,
                          "whitelist_reason": "x", "created_at": now, "updated_at": now}])
            with mock.patch.dict(os.environ, _sqlite_env(root)):
                catalog, _ = il.load_catalog_from_env()
                try:
                    il.lift_directory(src, catalog=catalog)
                finally:
                    catalog.engine.dispose()
                pointers = duckdb_smoke.fetch_pointers(os.environ["HL_CATALOG_URI"], "hl", "hypercore")
            self.assertEqual(list(pointers), ["token_whitelist"])
            self.assertTrue(pointers["token_whitelist"].endswith(".metadata.json"))
            self.assertTrue(pointers["token_whitelist"].startswith("file://"))

    def test_duckdb_missing_is_skip_unless_required(self):
        import builtins
        real_import = builtins.__import__

        def no_duckdb(name, *a, **k):
            if name == "duckdb":
                raise ModuleNotFoundError("duckdb")
            return real_import(name, *a, **k)
        with mock.patch.object(builtins, "__import__", side_effect=no_duckdb):
            self.assertEqual(preflight.check_duckdb(required=False).status, "SKIP")
            self.assertEqual(preflight.check_duckdb(required=True).status, "FAIL")
            with tempfile.TemporaryDirectory() as d, redirect_stdout(io.StringIO()), \
                    mock.patch.dict(os.environ, {"NESSIE_URI": "", "HL_CATALOG_URI": "", "HL_WEIGHT_BUDGET": "600"}):
                self.assertEqual(preflight.main(["--offline", "--out", d]), 0)              # SKIP row, exit 0
                self.assertEqual(preflight.main(["--offline", "--duckdb", "--out", d]), 1)  # required -> FAIL


class PreflightSqlCatalogTests(unittest.TestCase):
    def test_sql_catalog_requires_warehouse(self):
        with mock.patch.dict(os.environ, {"NESSIE_URI": "", "HL_CATALOG_URI": "sqlite:///x.db", "WAREHOUSE": ""}):
            c = preflight.check_iceberg(required=True)
        self.assertEqual(c.status, "FAIL")
        self.assertIn("WAREHOUSE", c.detail)

    def test_sql_catalog_sqlite_warns_but_works(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            env = {"NESSIE_URI": "", "HL_CATALOG_URI": f"sqlite:///{(root / 'c.db').as_posix()}",
                   "WAREHOUSE": f"file://{(root / 'wh').as_posix()}"}
            with mock.patch.dict(os.environ, env):
                c = preflight.check_iceberg(required=True)
            self.assertEqual(c.status, "WARN")
            self.assertIn("single-writer", c.detail)
            self.assertIn("hypercore tables 0/", c.detail)
            self.assertTrue((root / "wh").is_dir())

    def test_sql_catalog_bad_warehouse_scheme(self):
        with mock.patch.dict(os.environ, {"NESSIE_URI": "", "HL_CATALOG_URI": "sqlite:///x.db", "WAREHOUSE": "nfs://nas/x"}):
            c = preflight.check_iceberg(required=True)
        self.assertEqual(c.status, "FAIL")
        self.assertIn("neither file:// nor s3://", c.detail)


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
