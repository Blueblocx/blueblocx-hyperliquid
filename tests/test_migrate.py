"""Lake schema migrations (v1.5.0: l4_wallet_metadata.first_tx/latest_tx -> TIMESTAMPTZ)
and the schema-drift guard that makes them mandatory. SQLite catalog + file://
warehouse in a temp dir; no network, no systemd."""
from __future__ import annotations

import csv
import datetime as dt
import io
import os
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

from blueblocx_hyperliquid import iceberg_load as il, migrate, ops, preflight, wallet_meta
from blueblocx_hyperliquid import schema as S

W1 = "0x" + "a" * 40
W2 = "0x" + "b" * 40
NOW = "2026-10-12T01:00:00+00:00"
UTC = dt.timezone.utc


def _sqlite_env(root: Path) -> dict:
    return {"NESSIE_URI": "", "HL_CATALOG_URI": f"sqlite:///{(root / 'c.db').as_posix()}",
            "WAREHOUSE": f"file://{(root / 'wh').as_posix()}", "HL_WAREHOUSE_MOUNT": "", "HL_OUT_DIR": str(root)}


def _old_spec() -> dict:
    """The pre-v1.5.0 shape: first_tx / latest_tx as STRING."""
    spec = il.parse_ddl()["l4_wallet_metadata"]
    return {"columns": [(c, "STRING" if c in ("first_tx", "latest_tx") else t) for c, t in spec["columns"]],
            "partition": []}


def _old_rows() -> list[dict]:
    """Rows as the pre-v1.5.0 producer staged them (tx hashes in first_tx/latest_tx)."""
    return [
        {"wallet_address": W1, "first_block_timestamp": "2026-10-10T05:00:00+00:00",
         "latest_block_timestamp": "2026-10-11T06:00:00+00:00", "count_tx": 3,
         "first_tx": f"hlcore:{W1}:tid:1", "latest_tx": f"hlcore:{W1}:tid:3", "active_days": 2,
         "last_active": "", "active_on_avg": "", "updated_at": NOW},
        {"wallet_address": W2, "first_block_timestamp": "2026-10-09T00:00:00+00:00",
         "latest_block_timestamp": "2026-10-09T00:00:00+00:00", "count_tx": 1,
         "first_tx": f"hlcore:{W2}:funding:BTC:1", "latest_tx": f"hlcore:{W2}:funding:BTC:1", "active_days": 1,
         "last_active": "", "active_on_avg": "", "updated_at": NOW},
    ]


def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=S.L4_WALLET_METADATA_COLUMNS)
        w.writeheader()
        w.writerows(rows)


def _read_csv(path: Path) -> list[dict]:
    with path.open("r", newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


class _LakeCase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.ns_dir = self.root / "hypercore"
        self.ns_dir.mkdir()
        self._env = mock.patch.dict(os.environ, _sqlite_env(self.root))
        self._env.start()
        self.catalog, _ = il.load_catalog_from_env()

    def tearDown(self):
        il.dispose_catalog(self.catalog)
        self._env.stop()
        self._tmp.cleanup()

    def _old_table(self, rows: list[dict]):
        table = il.ensure_table(self.catalog, "hypercore", "l4_wallet_metadata", _old_spec())
        if rows:
            table.append(il.rows_to_arrow(rows, _old_spec()["columns"]))
            table.refresh()
        return table

    def _types(self):
        table = self.catalog.load_table("hypercore.l4_wallet_metadata")
        return {f.name: type(f.field_type).__name__ for f in table.schema().fields}

    def _rows(self):
        return {r["wallet_address"]: r for r in
                self.catalog.load_table("hypercore.l4_wallet_metadata").scan().to_arrow().to_pylist()}


class FirstTxMigrationTests(_LakeCase):
    def test_plan_apply_backfill_and_csv_rewrite_then_idempotent(self):
        self._old_table(_old_rows())
        _write_csv(self.ns_dir / "l4_wallet_metadata.csv", _old_rows())
        plan = migrate.run(self.catalog, "hypercore", self.ns_dir)
        p = plan["migrations"][0]["plan"]
        self.assertTrue(p["pending"])
        self.assertEqual(p["lake"]["retype"], ["first_tx", "latest_tx"])
        self.assertEqual(p["lake"]["types"], {"first_tx": "StringType", "latest_tx": "StringType"})
        self.assertEqual(p["lake"]["rows"], 2)
        self.assertEqual(p["csv"]["rows_to_rewrite"], 2)
        self.assertEqual(plan["pending"], 1)

        rep = migrate.run(self.catalog, "hypercore", self.ns_dir, apply=True)
        applied = rep["migrations"][0]["applied"]
        self.assertEqual(applied["lake"]["retyped"], ["first_tx", "latest_tx"])
        self.assertEqual(applied["lake"]["backfilled_rows"], 2)
        self.assertEqual(applied["csv"]["rewritten"], 2)
        self.assertEqual(rep["pending"], 0)
        # the lake: types, DDL column order, backfilled instants, untouched neighbours
        types = self._types()
        self.assertEqual((types["first_tx"], types["latest_tx"]), ("TimestamptzType", "TimestamptzType"))
        self.assertEqual(list(types), S.L4_WALLET_METADATA_COLUMNS)
        rows = self._rows()
        self.assertEqual(rows[W1]["first_tx"], dt.datetime(2026, 10, 10, 5, tzinfo=UTC))
        self.assertEqual(rows[W1]["latest_tx"], dt.datetime(2026, 10, 11, 6, tzinfo=UTC))
        self.assertEqual(rows[W2]["first_tx"], dt.datetime(2026, 10, 9, tzinfo=UTC))
        self.assertEqual((rows[W1]["count_tx"], rows[W1]["active_days"]), (3, 2))
        # the staged CSV: hashes replaced by the block timestamps, nothing else touched
        staged = {r["wallet_address"]: r for r in _read_csv(self.ns_dir / "l4_wallet_metadata.csv")}
        self.assertEqual(staged[W1]["first_tx"], "2026-10-10T05:00:00+00:00")
        self.assertEqual(staged[W1]["latest_tx"], "2026-10-11T06:00:00+00:00")
        self.assertEqual(staged[W2]["latest_tx"], "2026-10-09T00:00:00+00:00")
        self.assertEqual((staged[W1]["count_tx"], staged[W1]["updated_at"]), ("3", NOW))
        # no drift left; nothing to do a second time
        self.assertEqual(il.schema_drift(self.catalog.load_table("hypercore.l4_wallet_metadata"),
                                         il.parse_ddl()["l4_wallet_metadata"]), [])
        again = migrate.run(self.catalog, "hypercore", self.ns_dir, apply=True)
        self.assertNotIn("applied", again["migrations"][0])
        self.assertFalse(again["migrations"][0]["plan"]["pending"])

    def test_commits_are_retype_reorder_and_one_backfill(self):
        self._old_table(_old_rows())
        table = self.catalog.load_table("hypercore.l4_wallet_metadata")
        before = len(table.metadata.metadata_log)
        migrate.run(self.catalog, "hypercore", self.ns_dir, apply=True)
        table = self.catalog.load_table("hypercore.l4_wallet_metadata")
        # drop+re-add (1), DDL order (2), backfill (3) = three catalog commits; in every state a
        # reader of the pointer can observe, both columns exist and the table holds its 2 rows
        self.assertEqual(len(table.metadata.metadata_log) - before, 3)
        # pyiceberg's overwrite() = a delete snapshot + an append snapshot inside ONE commit
        self.assertIn(table.current_snapshot().summary.operation.value, ("append", "overwrite"))
        import json as _json
        states = [e.metadata_file for e in table.metadata.metadata_log[before:]] + [table.metadata_location]
        for loc in states:
            meta = _json.loads(Path(il.warehouse_local_path(loc)).read_text(encoding="utf-8"))
            current = next(s for s in meta["schemas"] if s["schema-id"] == meta["current-schema-id"])
            self.assertTrue({"first_tx", "latest_tx"} <= {f["name"] for f in current["fields"]})
            snap = next(s for s in meta["snapshots"] if s["snapshot-id"] == meta["current-snapshot-id"])
            self.assertEqual(snap["summary"]["total-records"], "2")

    def test_empty_table_and_absent_table(self):
        plan = migrate.run(self.catalog, "hypercore", self.ns_dir)                 # no table, no csv
        self.assertFalse(plan["migrations"][0]["plan"]["pending"])
        self.assertIn("absent", plan["migrations"][0]["plan"]["lake"]["table"])
        self._old_table([])                                                       # old shape, 0 rows
        rep = migrate.run(self.catalog, "hypercore", self.ns_dir, apply=True)
        self.assertEqual(rep["migrations"][0]["applied"]["lake"]["backfilled_rows"], 0)
        self.assertEqual(self._types()["first_tx"], "TimestamptzType")

    def test_lift_refuses_a_drifted_table_and_succeeds_after_the_migration(self):
        self._old_table([])
        src = self.root / "staging"
        src.mkdir()
        l1 = [{"block_timestamp": "2026-10-10T05:00:00+00:00", "block_date": "2026-10-10", "event_type": "spot_fill"},
              {"block_timestamp": "2026-10-11T06:00:00+00:00", "block_date": "2026-10-11", "event_type": "spot_fill"}]
        now_ms = wallet_meta._ms(NOW)
        rows = [wallet_meta.row(W1, wallet_meta.seed(l1, now_ms), now_ms, NOW)]
        _write_csv(src / "l4_wallet_metadata.csv", rows)
        with self.assertRaises(il.SchemaDriftError) as cm:
            il.lift_directory(src, catalog=self.catalog)
        self.assertIn("first_tx: table STRING vs ddl TIMESTAMP", str(cm.exception))
        wm = {r["table_name"]: r for r in self.catalog.load_table("control.table_watermarks").scan().to_arrow().to_pylist()}
        self.assertEqual(wm["l4_wallet_metadata"]["status"], il.WM_STATUS_FAILED)
        self.assertEqual(self._rows(), {})                                        # nothing written through a cast
        migrate.run(self.catalog, "hypercore", self.ns_dir, apply=True)
        rep = il.lift_directory(src, catalog=self.catalog)
        self.assertEqual(rep["l4_wallet_metadata"]["written"], 1)
        got = self._rows()[W1]
        self.assertEqual(got["first_tx"], dt.datetime(2026, 10, 10, 5, tzinfo=UTC))
        self.assertEqual(got["latest_tx"], dt.datetime(2026, 10, 11, 6, tzinfo=UTC))
        self.assertEqual(got["first_tx"], got["first_block_timestamp"])

    def test_schema_drift_reports_missing_and_extra_columns(self):
        table = self._old_table([])
        spec = il.parse_ddl()["l4_wallet_metadata"]
        drift = il.schema_drift(table, spec)
        self.assertEqual(drift, ["first_tx: table STRING vs ddl TIMESTAMP", "latest_tx: table STRING vs ddl TIMESTAMP"])
        fewer = {"columns": [c for c in spec["columns"] if c[0] != "active_days"], "partition": []}
        self.assertIn("active_days: in the table but not in the ddl", il.schema_drift(table, fewer))
        more = {"columns": spec["columns"] + [("new_col", "DOUBLE")], "partition": []}
        self.assertIn("new_col: missing in the table (ddl DOUBLE)", il.schema_drift(table, more))

    def test_revert_restores_string_columns(self):
        self._old_table(_old_rows())
        migrate.run(self.catalog, "hypercore", self.ns_dir, apply=True)
        rep = migrate.run(self.catalog, "hypercore", self.ns_dir, apply=True, revert=True)
        self.assertEqual(rep["migrations"][0]["revert"]["lake"]["retyped_to_string"], ["first_tx", "latest_tx"])
        self.assertTrue(rep["migrations"][0]["pending_after"])                    # forward migration pending again
        types = self._types()
        self.assertEqual((types["first_tx"], types["latest_tx"]), ("StringType", "StringType"))
        self.assertEqual(list(types), S.L4_WALLET_METADATA_COLUMNS)
        rows = self._rows()
        self.assertIsNone(rows[W1]["first_tx"])
        self.assertEqual(rows[W1]["count_tx"], 3)
        # a dry-run revert touches nothing
        dry = migrate.run(self.catalog, "hypercore", self.ns_dir, revert=True)
        self.assertIn("dry run", dry["migrations"][0]["revert"])

    def test_preflight_reports_drift_as_fail_until_migrated(self):
        self._old_table([])
        il.dispose_catalog(self.catalog)
        c = preflight.check_iceberg(required=False)
        self.assertEqual(c.status, "FAIL")
        self.assertIn("schema drift", c.detail)
        self.assertIn("migrate --apply", c.detail)
        migrate.run(self.catalog, "hypercore", self.ns_dir, apply=True)
        il.dispose_catalog(self.catalog)
        self.assertNotEqual(preflight.check_iceberg(required=False).status, "FAIL")

    def test_main_exit_codes(self):
        self._old_table(_old_rows())
        il.dispose_catalog(self.catalog)
        out = io.StringIO()
        with redirect_stdout(out):
            self.assertEqual(migrate.main(["--out", str(self.root)]), 2)                 # pending, dry run
            self.assertEqual(migrate.main(["--out", str(self.root), "--apply"]), 0)
            self.assertEqual(migrate.main(["--out", str(self.root)]), 0)                 # up to date
            self.assertEqual(migrate.main(["--out", str(self.root), "--json"]), 0)
        self.assertIn("up to date", out.getvalue())
        self.assertEqual(self._types()["first_tx"], "TimestamptzType")
        # a live lift holds the shared lock: never run alongside it
        (self.ns_dir / ".daily.lock").write_text('{"pid": 4242, "host": "%s"}' % __import__("socket").gethostname(),
                                                 encoding="utf-8")
        with mock.patch.object(ops, "pid_alive", return_value=True), \
             mock.patch.object(ops, "pid_is_ours", return_value=True):
            self.assertEqual(migrate.main(["--out", str(self.root), "--apply"]), 3)


class WalletMetadataRowTests(unittest.TestCase):
    def test_first_and_latest_tx_are_block_timestamp_instants_the_lift_parses(self):
        l1 = [{"block_timestamp": "2026-10-11T06:00:00+00:00", "block_date": "2026-10-11", "event_type": "spot_fill"},
              {"block_timestamp": "2026-10-10T05:00:00+00:00", "block_date": "2026-10-10", "event_type": "spot_fill"},
              {"block_timestamp": "2026-10-10T07:00:00+00:00", "block_date": "2026-10-10", "event_type": "fee"}]
        now_ms = wallet_meta._ms(NOW)
        row = wallet_meta.row(W1.upper(), wallet_meta.seed(l1, now_ms), now_ms, NOW)
        self.assertEqual(row["wallet_address"], W1)
        self.assertEqual(row["first_tx"], "2026-10-10T05:00:00+00:00")
        self.assertEqual(row["latest_tx"], "2026-10-11T06:00:00+00:00")
        self.assertEqual(row["first_tx"], row["first_block_timestamp"])
        self.assertEqual(row["latest_tx"], row["latest_block_timestamp"])
        self.assertEqual((row["count_tx"], row["active_days"]), (3, 2))
        # the lift parses the row as TIMESTAMP (no hash reaches a timestamptz column)
        spec = il.parse_ddl()["l4_wallet_metadata"]
        arrow = il.rows_to_arrow([row], spec["columns"])
        self.assertEqual(arrow.to_pylist()[0]["first_tx"], dt.datetime(2026, 10, 10, 5, tzinfo=UTC))


if __name__ == "__main__":
    unittest.main()
