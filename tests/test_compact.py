"""v1.8.0: nightly compaction of closed, fragmented partitions (maintenance --compact):
one commit per table, closed-partition rule, size cap, absolute deadline, readers never
see a partition missing. SQLite catalog + file:// warehouse in a temp dir; no network."""
from __future__ import annotations

import datetime as dt
import io
import json
import os
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

from blueblocx_hyperliquid import iceberg_load as il, maintenance
from blueblocx_hyperliquid import schema as S

W1 = "0x" + "a" * 40
TODAY = dt.date(2026, 10, 15)
NOW = dt.datetime(2026, 10, 15, 0, 5, tzinfo=dt.timezone.utc)      # the maintenance timer's slot
UTC = dt.timezone.utc


class _Clock:
    def __init__(self, t: float):
        self.t = t

    def time(self) -> float:
        return self.t


def _ledger_row(day: dt.date, i: int, event_type: str = "spot_fill") -> dict:
    hour, minute = divmod(i, 60)
    ts = dt.datetime(day.year, day.month, day.day, 1 + hour, minute, tzinfo=UTC).isoformat()
    row = {c: "" for c in S.L1_WALLET_LEDGER_COLUMNS}
    row.update({"tx_hash": f"hlcore:{W1}:tid:{day}:{i}", "event_index": 0, "direction": "in", "event_type": event_type,
                "wallet_address": W1, "token_address": "hlcore:usdc", "block_timestamp": ts,
                "block_date": day.isoformat(), "amount_signed": "1", "created_at": ts, "updated_at": ts})
    return row


def _l4x_row(day: dt.date, coin: str) -> dict:
    ts = dt.datetime(day.year, day.month, day.day, 2, tzinfo=UTC).isoformat()
    row = {c: "" for c in S.L4X_PERP_POSITION_SNAPSHOT_COLUMNS}
    row.update({"as_of_ts": ts, "as_of_date": day.isoformat(), "wallet_address": W1, "coin": coin, "dex": "main",
                "unrealized_pnl_usd": "1.5", "created_at": ts})
    return row


def _states(table) -> list[tuple[int, int]]:
    """(total records, data files) of every published state of the table, oldest first."""
    out = []
    for loc in [e.metadata_file for e in table.metadata.metadata_log] + [table.metadata_location]:
        meta = json.loads(Path(il.warehouse_local_path(loc)).read_text(encoding="utf-8"))
        sid = meta.get("current-snapshot-id")
        if sid in (None, -1):
            out.append((0, 0))
            continue
        snap = next(s for s in meta["snapshots"] if s["snapshot-id"] == sid)
        out.append((int(snap["summary"]["total-records"]), int(snap["summary"]["total-data-files"])))
    return out


class _LakeCase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        env = {"NESSIE_URI": "", "HL_CATALOG_URI": f"sqlite:///{(self.root / 'c.db').as_posix()}",
               "WAREHOUSE": f"file://{(self.root / 'wh').as_posix()}", "HL_WAREHOUSE_MOUNT": "",
               "HL_OUT_DIR": str(self.root)}
        self._env = mock.patch.dict(os.environ, env)
        self._env.start()
        self.catalog, _ = il.load_catalog_from_env()
        self.ddl = il.parse_ddl()

    def tearDown(self):
        il.dispose_catalog(self.catalog)
        self._env.stop()
        self._tmp.cleanup()

    def _table(self, name: str):
        return il.ensure_table(self.catalog, "hypercore", name, self.ddl[name], required=il.REQUIRED_COLS.get(name, ()))

    def _append_each(self, table, name: str, rows: list[dict]) -> None:
        """One append (= one data file) per row, like one hourly lift each."""
        for r in rows:
            table.append(il.conform(il.rows_to_arrow([r], self.ddl[name]["columns"]), table))
        table.refresh()

    def _files(self, table) -> dict:
        return {str(p["partition"]): (int(p["file_count"]), int(p["record_count"]))
                for p in table.inspect.partitions().to_pylist()}


class CompactionTests(_LakeCase):
    def test_closed_fragmented_partition_is_rewritten_in_one_commit_recent_untouched(self):
        name = "l1_wallet_ledger"
        table = self._table(name)
        old = TODAY - dt.timedelta(days=3)
        thin = TODAY - dt.timedelta(days=4)
        self._append_each(table, name, [_ledger_row(old, i) for i in range(9)]           # 9 files, closed
                          + [_ledger_row(thin, 0)]                                        # 1 file, closed: below min
                          + [_ledger_row(TODAY - dt.timedelta(days=1), i) for i in range(9)]   # 9 files, NOT closed (1 day)
                          + [_ledger_row(old, 99, "fee")])                                # another closed partition, 1 file
        before = self._files(table)
        self.assertEqual(before[str({'block_date': old, 'event_type': 'spot_fill'})], (9, 9))
        log_before = len(table.metadata.metadata_log)
        clock = _Clock(NOW.timestamp())
        rep = maintenance.compact("hypercore", self.catalog, now=NOW, clock=clock)
        entry = rep["tables"]["hypercore.l1_wallet_ledger"]
        self.assertEqual((entry["candidates"], entry["commits"], entry["files_before"], entry["files_after"], entry["rows"]),
                         (1, 1, 9, 1, 9))
        self.assertEqual(entry["compacted"][0]["partition"], {"block_date": old.isoformat(), "event_type": "spot_fill"})
        table = self.catalog.load_table("hypercore.l1_wallet_ledger")
        after = self._files(table)
        self.assertEqual(after[str({'block_date': old, 'event_type': 'spot_fill'})], (1, 9))
        self.assertEqual(after[str({'block_date': TODAY - dt.timedelta(days=1), 'event_type': 'spot_fill'})], (9, 9))
        self.assertEqual(after[str({'block_date': thin, 'event_type': 'spot_fill'})], (1, 1))
        self.assertEqual(il._table_row_count(table), 20)
        self.assertEqual(len(table.metadata.metadata_log) - log_before, 1)                 # ONE catalog commit
        self.assertTrue(all(rows == 20 for rows, _ in _states(table)[-2:]))                # no published state lost rows
        # the rows themselves are intact
        got = sorted(r["tx_hash"] for r in table.scan(row_filter=f"block_date = '{old}'").to_arrow().to_pylist())
        self.assertEqual(got, sorted([f"hlcore:{W1}:tid:{old}:{i}" for i in range(9)] + [f"hlcore:{W1}:tid:{old}:99"]))
        # a second pass finds nothing to do
        rep2 = maintenance.compact("hypercore", self.catalog, now=NOW, clock=clock)
        self.assertEqual(rep2["tables"]["hypercore.l1_wallet_ledger"]["candidates"], 0)

    def test_l4x_yesterday_is_closed_today_is_not(self):
        name = "l4x_perp_position_snapshot"
        table = self._table(name)
        yday = TODAY - dt.timedelta(days=1)
        self._append_each(table, name, [_l4x_row(yday, f"C{i}") for i in range(8)] + [_l4x_row(TODAY, f"C{i}") for i in range(8)])
        rep = maintenance.compact("hypercore", self.catalog, now=NOW, clock=_Clock(NOW.timestamp()))
        entry = rep["tables"]["hypercore.l4x_perp_position_snapshot"]
        self.assertEqual((entry["candidates"], entry["files_before"], entry["files_after"]), (1, 8, 1))
        after = self._files(self.catalog.load_table("hypercore.l4x_perp_position_snapshot"))
        self.assertEqual(after[str({'as_of_date': yday})], (1, 8))
        self.assertEqual(after[str({'as_of_date': TODAY})], (8, 8))

    def test_deadline_and_size_cap_leave_the_table_untouched(self):
        name = "l1_wallet_ledger"
        table = self._table(name)
        old = TODAY - dt.timedelta(days=3)
        self._append_each(table, name, [_ledger_row(old, i) for i in range(8)])
        log_before = len(table.metadata.metadata_log)
        # the deadline has passed before the first partition
        rep = maintenance.compact("hypercore", self.catalog, now=NOW, deadline_ts=NOW.timestamp() - 1,
                                  clock=_Clock(NOW.timestamp()))
        entry = rep["tables"]["hypercore.l1_wallet_ledger"]
        self.assertEqual((entry["candidates"], entry["deadline_hit"], entry["commits"]), (1, True, 0))
        # a partition above the row cap is skipped and reported
        rep = maintenance.compact("hypercore", self.catalog, now=NOW, clock=_Clock(NOW.timestamp()), max_rows=5)
        entry = rep["tables"]["hypercore.l1_wallet_ledger"]
        self.assertEqual((entry["commits"], len(entry["skipped_large"])), (0, 1))
        table = self.catalog.load_table("hypercore.l1_wallet_ledger")
        self.assertEqual(len(table.metadata.metadata_log), log_before)                      # nothing committed

    def test_next_tick_deadline(self):
        self.assertEqual(maintenance.next_tick_deadline(NOW), dt.datetime(2026, 10, 15, 0, 9, tzinfo=UTC).timestamp())
        at_30 = dt.datetime(2026, 10, 15, 14, 30, tzinfo=UTC)
        self.assertEqual(maintenance.next_tick_deadline(at_30), dt.datetime(2026, 10, 15, 15, 9, tzinfo=UTC).timestamp())
        self.assertEqual(maintenance.next_tick_deadline(dt.datetime(2026, 10, 15, 23, 10, tzinfo=UTC)),
                         dt.datetime(2026, 10, 16, 0, 9, tzinfo=UTC).timestamp())

    def test_main_runs_compaction_before_expiry_and_sets_the_retention_properties(self):
        name = "l1_wallet_ledger"
        table = self._table(name)
        old = TODAY - dt.timedelta(days=3)
        self._append_each(table, name, [_ledger_row(old, i) for i in range(8)])
        il.dispose_catalog(self.catalog)
        out = io.StringIO()
        real_compact = maintenance.compact                     # pin the clock: main() has no clock argument
        with redirect_stdout(out), mock.patch.object(maintenance, "compact",
                                                     side_effect=lambda ns, cat, **kw: real_compact(
                                                         ns, cat, now=NOW, clock=_Clock(NOW.timestamp()), **kw)):
            self.assertEqual(maintenance.main(["--compact", "--expire-days", "7", "--out", str(self.root)]), 0)
        rep = json.loads(out.getvalue())
        self.assertEqual(rep["compact"]["tables"]["hypercore.l1_wallet_ledger"]["commits"], 1)
        self.assertIn("hypercore.l1_wallet_ledger", rep["tables"])
        table = self.catalog.load_table("hypercore.l1_wallet_ledger")
        self.assertEqual(table.properties["write.metadata.previous-versions-max"], "100")
        self.assertEqual(self._files(table)[str({'block_date': old, 'event_type': 'spot_fill'})], (1, 8))


if __name__ == "__main__":
    unittest.main()
