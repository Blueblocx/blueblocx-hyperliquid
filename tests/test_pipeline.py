"""
Tests for the pipeline modules added after the core mapper: S3 backfill, Iceberg
lift (DDL parsing + Arrow coercion, no catalog needed), HyperEVM mapper
invariants, incremental watermark windows, and HIP-3 multi-dex net worth.

Run:  python -m unittest discover -s tests -t . -v
"""
from __future__ import annotations

import datetime as dt
import json
import unittest

import pyarrow as pa

from hl_poc import evm_mapper, extract, iceberg_load, s3_backfill, schema as S

W = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
OTHER = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
NOW = "2026-10-05T00:00:00+00:00"


class S3BackfillTests(unittest.TestCase):
    def test_hour_keys_cover_window_inclusive_of_partial_hours(self):
        t0 = dt.datetime(2026, 10, 6, 8, 15, tzinfo=dt.timezone.utc)   # 08:15Z
        start = int(t0.timestamp() * 1000)
        end = start + 2 * 3_600_000                       # 10:15Z -> hours 08,09,10
        keys = s3_backfill.hour_keys(start, end)
        self.assertEqual(keys, ["node_fills_by_block/hourly/20261006/8.lz4",
                                "node_fills_by_block/hourly/20261006/9.lz4",
                                "node_fills_by_block/hourly/20261006/10.lz4"])

    def test_iter_wallet_fills_filters_wallet_and_window(self):
        fill = {"coin": "BTC", "tid": 1, "time": 1000, "closedPnl": "1"}
        other = {"coin": "ETH", "tid": 2, "time": 1000}
        late = {"coin": "BTC", "tid": 3, "time": 5000}
        raw = "\n".join(json.dumps({"block_number": 1, "block_time": "x",
                                    "events": [[W, fill], [OTHER, other], [W, late]]})
                        for _ in range(2)).encode()
        got = list(s3_backfill._iter_wallet_fills(raw, {W}, 0, 2000))
        self.assertEqual([(w, f["tid"]) for w, f in got], [(W, 1), (W, 1)])  # dedup is the caller's job

    def test_merge_fills_dedups_on_tid_api_wins(self):
        api = [{"tid": 1, "time": 10, "src": "api"}]
        s3 = [{"tid": 1, "time": 10, "src": "s3"}, {"tid": 0, "time": 5, "src": "s3"}]
        merged = s3_backfill.merge_fills(api, s3)
        self.assertEqual([f["tid"] for f in merged], [0, 1])
        self.assertEqual(merged[1]["src"], "api")


class IcebergLoadTests(unittest.TestCase):
    def test_parse_ddl_finds_all_planned_tables(self):
        ddl = iceberg_load.parse_ddl()
        for name, (key_cols, _, scope) in iceberg_load.TABLE_PLAN.items():
            self.assertIn(name, ddl, name)
            cols = {c for c, _ in ddl[name]["columns"]}
            for k in key_cols + scope:
                self.assertIn(k, cols, f"{name}.{k}")
        self.assertEqual(ddl["l1_wallet_ledger"]["partition"], ["block_date", "event_type"])
        self.assertEqual(ddl["l4x_perp_position_snapshot"]["partition"], ["as_of_date"])

    def test_rows_to_arrow_coerces_csv_strings(self):
        cols = [("a", "BIGINT"), ("b", "INT"), ("c", "DOUBLE"), ("d", "BOOLEAN"),
                ("e", "DATE"), ("f", "TIMESTAMP"), ("g", "STRING")]
        t = iceberg_load.rows_to_arrow(
            [{"a": "12", "b": "", "c": "1.5", "d": "True", "e": "2026-10-05",
              "f": "2026-10-05T01:02:03.004000+00:00", "g": None}], cols)
        r = t.to_pylist()[0]
        self.assertEqual(r["a"], 12)
        self.assertIsNone(r["b"])
        self.assertEqual(r["c"], 1.5)
        self.assertTrue(r["d"])
        self.assertEqual(r["e"], dt.date(2026, 10, 5))
        self.assertEqual(r["f"], dt.datetime(2026, 10, 5, 1, 2, 3, 4000))   # naive UTC
        self.assertIsNone(r["g"])
        self.assertEqual(t.schema.field("f").type, pa.timestamp("us"))


class EvmMapperTests(unittest.TestCase):
    TRANSFER = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

    def _log(self, frm, to, value=10**18, idx=3):
        pad = lambda a: "0x" + a[2:].rjust(64, "0")  # noqa: E731
        return {"address": "0xT0KEN000000000000000000000000000000000001".lower(),
                "topics": [self.TRANSFER, pad(frm), pad(to)],
                "data": "0x" + hex(value)[2:].rjust(64, "0"),
                "blockNumber": hex(100), "logIndex": hex(idx), "transactionIndex": hex(7),
                "transactionHash": "0x" + "cd" * 32}

    def test_standard_is_erc20_and_symbol_not_in_row(self):
        rows = evm_mapper.map_evm_transfers_to_ledger(
            [self._log(OTHER, W)], W, {100: 1_700_000_000},
            {"0xt0ken000000000000000000000000000000000001": {"decimals": 18, "symbol": "USDT0"}}, NOW)
        self.assertEqual(len(rows), 1)
        r = rows[0]
        self.assertEqual(r["standard"], S.STANDARD_ERC20)
        self.assertEqual(r["direction"], S.DIRECTION_IN)
        self.assertEqual(r["tx_index"], 7)
        self.assertEqual(r["event_index"], 3)
        self.assertAlmostEqual(r["amount"], 1.0)
        self.assertEqual(r["event_type"], S.EVT_TRANSFER)

    def test_mint_and_burn_detection(self):
        zero = evm_mapper.ZERO_ADDR
        mint = evm_mapper.map_evm_transfers_to_ledger([self._log(zero, W)], W, {100: 1}, {}, NOW)[0]
        burn = evm_mapper.map_evm_transfers_to_ledger([self._log(W, zero, idx=4)], W, {100: 1}, {}, NOW)[0]
        self.assertEqual(mint["event_type"], S.EVT_MINT)
        self.assertEqual(burn["event_type"], S.EVT_BURN)
        self.assertIsNone(mint["amount"])   # unknown decimals -> amount NULL, never guessed


class IncrementalWindowTests(unittest.TestCase):
    def test_no_watermark_uses_default(self):
        self.assertEqual(extract.compute_start_ms(1000, None), 1000)

    def test_watermark_resumes_with_overlap_but_not_before_default(self):
        self.assertEqual(extract.compute_start_ms(1000, 10_000_000, overlap_ms=3_600_000),
                         10_000_000 - 3_600_000)
        self.assertEqual(extract.compute_start_ms(9_000_000, 10_000_000, overlap_ms=3_600_000),
                         9_000_000)


class MergeRowsTests(unittest.TestCase):
    def test_merge_rows_by_key_new_wins_and_unions(self):
        a = {"tx_hash": "h1", "event_index": 0, "direction": "in", "amount_signed": 1.0}
        b = {"tx_hash": "h2", "event_index": 0, "direction": "in", "amount_signed": 2.0}
        a2 = dict(a, amount_signed=5.0)
        merged = extract.merge_rows_by_key([a, b], [a2])
        self.assertEqual(len(merged), 2)
        self.assertEqual({r["tx_hash"]: r["amount_signed"] for r in merged}, {"h1": 5.0, "h2": 2.0})


class Hip3NetWorthTests(unittest.TestCase):
    def test_other_dex_equity_added_without_overlap(self):
        from hl_poc import analytics
        chs = {"marginSummary": {"accountValue": "500"}, "assetPositions": []}
        spot = {"balances": [{"coin": "USDC", "token": 0, "total": "2000", "spotHold": "500"}]}
        a = analytics.compute_analytics([], [], chs, spot, 1_700_000_000_000, NOW,
                                        account_value_by_dex={"xyz": 300.0, "flx": 0.0})
        # main dex fully overlapped by spot USDC (portfolio margin); xyz adds 300
        self.assertAlmostEqual(a["perp_spot_overlap_usd"], 500.0)
        self.assertAlmostEqual(a["account_value_other_dex_usd"], 300.0)
        self.assertAlmostEqual(a["net_worth"], 2000 + 300)


if __name__ == "__main__":
    unittest.main()
