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

from blueblocx_hyperliquid import evm_mapper, extract, iceberg_load, s3_backfill, schema as S

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
        s3 = [{"tid": 1, "time": 10, "src": "s3"}, {"tid": 0, "time": 5, "coin": "@1", "sz": "1", "src": "s3"}]
        merged = s3_backfill.merge_fills(api, s3)
        self.assertEqual([f["tid"] for f in merged], [0, 1])
        self.assertEqual(merged[1]["src"], "api")

    def test_merge_keeps_distinct_dust_fills(self):
        # tid=0 dust conversions: several per midnight, one per dust coin
        s3 = [{"tid": 0, "time": 5, "coin": "@142", "sz": "0.006"},
              {"tid": 0, "time": 5, "coin": "@166", "sz": "1.0"},
              {"tid": 0, "time": 6, "coin": "@142", "sz": "0.006"}]
        api = [{"tid": 0, "time": 5, "coin": "@142", "sz": "0.006", "src": "api"}]
        merged = s3_backfill.merge_fills(api, s3)
        self.assertEqual(len(merged), 3)
        self.assertEqual(len({s3_backfill.fill_key(f) for f in merged}), 3)

    def test_result_completeness_is_reported_not_assumed(self):
        res = s3_backfill.S3Result(fills={}, keys_requested=["a", "b"], keys_missing=["b"])
        self.assertFalse(res.complete)
        self.assertEqual(res.summary()["keys_missing"], ["b"])
        self.assertTrue(s3_backfill.S3Result(fills={}, keys_requested=["a"]).complete)


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
        # data tables: timestamptz (UTC) — matches Spark TIMESTAMP_LTZ / python-ingestion
        self.assertEqual(r["f"], dt.datetime(2026, 10, 5, 1, 2, 3, 4000, tzinfo=dt.timezone.utc))
        self.assertIsNone(r["g"])
        self.assertEqual(t.schema.field("f").type, pa.timestamp("us", tz="UTC"))
        # control.table_watermarks: naive
        t2 = iceberg_load.rows_to_arrow([{"f": "2026-10-05T01:02:03+00:00"}], [("f", "TIMESTAMP")], tz=False)
        self.assertEqual(t2.to_pylist()[0]["f"], dt.datetime(2026, 10, 5, 1, 2, 3))


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

    def test_watermark_always_resumes_from_watermark_never_clamped(self):
        # a missed day must NOT open a hole between the watermark and now-days
        self.assertEqual(extract.compute_start_ms(1000, 10_000_000, overlap_ms=3_600_000),
                         10_000_000 - 3_600_000)
        self.assertEqual(extract.compute_start_ms(9_000_000, 10_000_000, overlap_ms=3_600_000),
                         10_000_000 - 3_600_000)


class TruncationFlagTests(unittest.TestCase):
    def _c(self):
        from blueblocx_hyperliquid.info_client import InfoClient
        return InfoClient()

    def test_cap_sized_but_window_covered_is_not_truncated(self):
        # live: 12,757 fills whose oldest is 1.4 min after the window start
        start = 1_700_000_000_000
        fills = [{"time": start + 84_000}] * 12_757
        self.assertFalse(self._c().user_fills_saturated(fills, start))

    def test_cap_sized_with_hours_missing_is_truncated(self):
        start = 1_700_000_000_000
        fills = [{"time": start + 9 * 3_600_000}] * 12_954
        self.assertTrue(self._c().user_fills_saturated(fills, start))

    def test_new_wallet_below_count_is_not_truncated(self):
        start = 1_700_000_000_000
        fills = [{"time": start + 33 * 3_600_000}] * 9_300
        self.assertFalse(self._c().user_fills_saturated(fills, start))


class TwapSliceFillTests(unittest.TestCase):
    def test_twap_slice_normalised_and_merged_by_tid(self):
        from blueblocx_hyperliquid import mappers
        from blueblocx_hyperliquid.spot_meta import SpotMetaResolver
        # live shape from userTwapSliceFillsByTime (twapId null INSIDE, set outside)
        slice_ = {"fill": {"coin": "HYPE/USDC", "px": "0.30087", "sz": "38.74", "side": "B",
                           "time": 1_700_000_000_000, "startPosition": "1", "dir": "Buy",
                           "closedPnl": "0.0", "hash": S.ZERO_HASH, "oid": 1, "crossed": True,
                           "fee": "0.026", "tid": 651818585970396, "feeToken": "HYPE", "twapId": None},
                  "twapId": 2287760}
        f = dict(slice_["fill"]); f["twapId"] = slice_["twapId"]
        merged = s3_backfill.merge_fills([{"tid": 1, "time": 1, "coin": "BTC", "sz": "1"}], [f])
        self.assertEqual(len(merged), 2)
        rows = mappers.map_fills_to_ledger([f], W, NOW, SpotMetaResolver(
            {"tokens": [{"name": "USDC", "index": 0, "weiDecimals": 8}, {"name": "HYPE", "index": 150, "weiDecimals": 8}],
             "universe": [{"name": "HYPE/USDC", "index": 1, "tokens": [150, 0]}]}), mappers.TokenRegistry())
        self.assertEqual(len(rows), 3)                     # base, quote, base-token fee
        self.assertTrue(all(r["tx_hash"].startswith(f"hlcore:{W}:tid:651818585970396") for r in rows))


class SharedS3WindowTests(unittest.TestCase):
    def test_window_covers_all_gaps_and_ignores_empty(self):
        self.assertEqual(extract.plan_s3_window([(10, 50), (30, 70), (90, 90)]), (10, 70))
        self.assertIsNone(extract.plan_s3_window([(5, 5)]))


class LendingCacheTests(unittest.TestCase):
    def test_merge_lending_adds_deltas_and_drops_zero(self):
        from blueblocx_hyperliquid import analytics
        self.assertEqual(analytics.merge_lending({"USDC": 100.0}, {"USDC": -100.0, "USDT0": 5.0}),
                         {"USDT0": 5.0})


class OracleToleranceTests(unittest.TestCase):
    def test_absolute_floor_for_tiny_accounts(self):
        from blueblocx_hyperliquid import analytics
        pf = [["perpAllTime", {"accountValueHistory": [[1, "13.712637"]]}],
              ["allTime", {"accountValueHistory": [[1, "19.340574"]]}]]
        a = {"account_value_usd": 0.0, "account_value_other_dex_usd": 13.734737, "net_worth": 19.43}
        o = analytics.validate_against_oracle(a, pf, 0)
        self.assertTrue(o["account_value_matches"])    # 2.2 cents on $13.7 (0.16%) — fine either way
        self.assertTrue(o["networth_matches"])         # 9 cents gap (0.46%) under the $2 floor
        a2 = dict(a, net_worth=25.0)
        self.assertFalse(analytics.validate_against_oracle(a2, pf, 0)["networth_matches"])


class PnlOracleTests(unittest.TestCase):
    def test_pending_without_prior_snapshot(self):
        from blueblocx_hyperliquid import analytics
        r = analytics.pnl_oracle({"perpDay": {"pnlHistory": [[0, "10"], [1, "25"]]}}, 10.0, 5.0, None, None)
        self.assertFalse(r["checked"])

    def test_matches_when_realized_plus_unrealized_delta_equals_history(self):
        from blueblocx_hyperliquid import analytics
        pf = {"perpDay": {"pnlHistory": [[0, "100.0"], [1, "160.0"]]}}      # +60 in 24h
        r = analytics.pnl_oracle(pf, realized_perp_24h=45.0, unrealized_now=30.0,
                                 prior_unrealized=15.0, prior_age_h=24.0)        # 45 + 15 = 60
        self.assertTrue(r["checked"]); self.assertTrue(r["pnl_matches"])
        r2 = analytics.pnl_oracle(pf, 45.0, 30.0, 25.0, 24.0)                  # 45 + 5 = 50 != 60
        self.assertFalse(r2["pnl_matches"])


class WeightModelTests(unittest.TestCase):
    def test_documented_weights(self):
        from blueblocx_hyperliquid.info_client import InfoClient
        self.assertEqual(InfoClient._weight_of({"type": "clearinghouseState"}, {}), 2)
        self.assertEqual(InfoClient._weight_of({"type": "portfolio"}, [["day", {}]]), 20)
        self.assertEqual(InfoClient._weight_of({"type": "userFillsByTime"}, [{}] * 2000), 120)


class ClosedCandleTests(unittest.TestCase):
    def test_in_progress_candle_is_not_emitted(self):
        from blueblocx_hyperliquid import prices
        now = 1_700_000_000_000
        candles = [{"t": now - 120_000, "T": now - 60_001, "o": "1", "h": "1", "l": "1", "c": "1", "v": "1", "n": 1},
                   {"t": now - 60_000, "T": now + 59_999, "o": "1", "h": "1", "l": "1", "c": "1", "v": "1", "n": 1}]
        rows = prices.candles_to_price_1m("hlcore:token:150", candles, NOW, closed_before_ms=now)
        self.assertEqual(len(rows), 1)
        self.assertEqual(len(prices.candles_to_price_1m("x", candles, NOW)), 2)


class IcebergPredicateSafetyTests(unittest.TestCase):
    def test_empty_key_list_never_becomes_a_delete_predicate(self):
        self.assertIsNone(iceberg_load._in_filter("k", []))

    def test_null_key_raises(self):
        with self.assertRaises(ValueError):
            iceberg_load._in_filter("k", ["a", None])

    def test_watermark_schema_has_required_fields_like_python_ingestion(self):
        sch = iceberg_load.arrow_schema(iceberg_load._WM_COLS, tz=False)
        required = {f.name for f in sch if not f.nullable}
        self.assertEqual(required, {"namespace", "table_name", "watermark_col", "updated_at", "status"})
        self.assertEqual(sch.field("updated_at").type, pa.timestamp("us"))

    def test_data_tables_use_timestamptz(self):
        cols = [("block_timestamp", "TIMESTAMP")]
        self.assertEqual(iceberg_load.arrow_schema(cols, tz=True).field("block_timestamp").type,
                         pa.timestamp("us", tz="UTC"))
        v = iceberg_load._coerce("2026-10-05T01:02:03+00:00", "TIMESTAMP", tz=True)
        self.assertIsNotNone(v.tzinfo)


class LendingPositionTests(unittest.TestCase):
    def _bl(self, op, amount, interest="0"):
        return {"time": 1, "hash": "0x" + "ab" * 32,
                "delta": {"type": "borrowLend", "token": "USDC", "operation": op,
                          "amount": amount, "interestAmount": interest}}

    def test_supply_minus_withdraw_plus_interest_matches_live_shape(self):
        from blueblocx_hyperliquid import analytics
        # live wallet 0x4fb2…: supplies 32,567.98, withdraws 26,968.58, interest 6.6044 -> 5,606.00 (== oracle gap)
        led = [self._bl("supply", "32567.98"), self._bl("withdraw", "26968.58", "6.6044")]
        pos = analytics.lending_position(led)
        self.assertAlmostEqual(pos["USDC"], 5606.0044, places=3)

    def test_non_pm_account_adds_lending_to_net_worth(self):
        from blueblocx_hyperliquid import analytics
        chs = {"marginSummary": {"accountValue": "0"}, "assetPositions": []}
        spot = {"balances": [{"coin": "USDC", "token": 0, "total": "0.47", "hold": "0"}]}  # no `supplied` key
        a = analytics.compute_analytics([], [], chs, spot, 1_700_000_000_000, NOW,
                                        lending_by_token={"USDC": 5606.0})
        self.assertAlmostEqual(a["lending_value_usd"], 5606.0)
        self.assertAlmostEqual(a["net_worth"], 5606.47)

    def test_pm_account_with_supplied_in_total_does_not_double_count(self):
        from blueblocx_hyperliquid import analytics
        chs = {"marginSummary": {"accountValue": "0"}, "assetPositions": []}
        spot = {"portfolioMarginEnabled": True,
                "balances": [{"coin": "USDC", "token": 0, "total": "2603", "hold": "218", "spotHold": "358", "supplied": "2245"}]}
        a = analytics.compute_analytics([], [], chs, spot, 1_700_000_000_000, NOW,
                                        lending_by_token={"USDC": 2245.0})
        self.assertEqual(a["lending_value_usd"], 0.0)
        self.assertAlmostEqual(a["net_worth"], 2603.0)


class CollateralTokenTests(unittest.TestCase):
    def test_hip3_coin_uses_dex_collateral(self):
        from blueblocx_hyperliquid import mappers
        from blueblocx_hyperliquid.spot_meta import TokenInfo
        usdh = TokenInfo(index=360, symbol="USDH", decimals=8, token_address="hlcore:token:360",
                         is_stablecoin=True)
        t = mappers.perp_cash_token("flx:TSLA", {"flx:": usdh})
        self.assertEqual(t[0], "hlcore:token:360")
        self.assertEqual(mappers.perp_cash_token("BTC", {"flx:": usdh})[0], S.USDC_ADDRESS)
        self.assertEqual(mappers.perp_cash_token("xyz:SP500", {"flx:": usdh})[0], S.USDC_ADDRESS)


class EvmDecimalsTests(unittest.TestCase):
    def test_unresolved_decimals_stay_null_in_metadata(self):
        from blueblocx_hyperliquid import prices
        rows = prices.token_metadata_rows({"0xabc": {"symbol": "X", "decimals": None, "is_stablecoin": False}}, NOW)
        self.assertIsNone(rows[0]["decimals"])

    def test_missing_block_timestamp_drops_row_and_counts(self):
        TRANSFER = EvmMapperTests.TRANSFER
        pad = lambda a: "0x" + a[2:].rjust(64, "0")  # noqa: E731
        lg = {"address": "0x" + "11" * 20, "topics": [TRANSFER, pad(OTHER), pad(W)],
              "data": "0x" + "1".rjust(64, "0"), "blockNumber": hex(5), "logIndex": "0x0",
              "transactionIndex": "0x0", "transactionHash": "0x" + "ab" * 32}
        stats = {}
        rows = evm_mapper.map_evm_transfers_to_ledger([lg], W, {}, {}, NOW, stats=stats)
        self.assertEqual(rows, [])
        self.assertEqual(stats["dropped_no_timestamp"], 1)


class MergeRowsTests(unittest.TestCase):
    def test_merge_rows_by_key_new_wins_and_unions(self):
        a = {"tx_hash": "h1", "event_index": 0, "direction": "in", "amount_signed": 1.0}
        b = {"tx_hash": "h2", "event_index": 0, "direction": "in", "amount_signed": 2.0}
        a2 = dict(a, amount_signed=5.0)
        merged = extract.merge_rows_by_key([a, b], [a2])
        self.assertEqual(len(merged), 2)
        self.assertEqual({r["tx_hash"]: r["amount_signed"] for r in merged}, {"h1": 5.0, "h2": 2.0})


class Hip3NetWorthTests(unittest.TestCase):
    def test_overlap_applies_to_total_perp_equity_with_spot_hold(self):
        from blueblocx_hyperliquid import analytics
        chs = {"marginSummary": {"accountValue": "500", "totalMarginUsed": "50"}, "assetPositions": []}
        # unified account: collateral for main+xyz (800) is inside spot USDC total
        spot = {"portfolioMarginEnabled": True,
                "balances": [{"coin": "USDC", "token": 0, "total": "2000", "spotHold": "800"}]}
        a = analytics.compute_analytics([], [], chs, spot, 1_700_000_000_000, NOW,
                                        account_value_by_dex={"xyz": 300.0, "flx": 0.0})
        self.assertAlmostEqual(a["perp_spot_overlap_usd"], 800.0)
        self.assertAlmostEqual(a["account_value_other_dex_usd"], 300.0)
        self.assertAlmostEqual(a["net_worth"], 2000.0)

    def test_hip3_trader_unified_via_hold_margin(self):
        # live shape (HIP-3 trader): no spotHold; hold ≈ perp margin reserved from
        # spot; oracle allTime == spot + staked (perp not additive)
        from blueblocx_hyperliquid import analytics
        chs = {"marginSummary": {"accountValue": "276477", "totalMarginUsed": "400000"}, "assetPositions": []}
        spot = {"balances": [{"coin": "USDC", "token": 0, "total": "603330", "hold": "421308"}]}
        a = analytics.compute_analytics([], [], chs, spot, 1_700_000_000_000, NOW,
                                        account_value_by_dex={"xyz": 145147.0})
        self.assertEqual(a["perp_spot_overlap_rule"], "hold_reserved")
        self.assertAlmostEqual(a["perp_spot_overlap_usd"], 421624.0)
        self.assertAlmostEqual(a["net_worth"], 603330.0)

    def test_flat_unified_account_hold_equals_perp_equity(self):
        # live shape (0xfe35…): no open positions (margin used 0) but hold == perp
        # accountValue; oracle allTime == spot USDC total (46,392)
        from blueblocx_hyperliquid import analytics
        chs = {"marginSummary": {"accountValue": "34618.945332", "totalMarginUsed": "0.0"}, "assetPositions": []}
        spot = {"balances": [{"coin": "USDC", "token": 0, "total": "46392.2910905", "hold": "34618.945332"}]}
        a = analytics.compute_analytics([], [], chs, spot, 1_700_000_000_000, NOW)
        self.assertEqual(a["perp_spot_overlap_rule"], "hold_reserved")
        self.assertAlmostEqual(a["net_worth"], 46392.2910905, places=4)

    def test_whale_unified_hold_minus_spot_bids_matches_margin(self):
        # live shape (48M whale): hold 4.63M, spot bids lock 0.06M, margin used 4.44M
        from blueblocx_hyperliquid import analytics
        chs = {"marginSummary": {"accountValue": "7407327", "totalMarginUsed": "4435961"}, "assetPositions": []}
        spot = {"balances": [{"coin": "USDC", "token": 0, "total": "24531182", "hold": "4633671"}]}
        orders = [{"coin": "@292", "side": "B", "sz": "47784.01", "limitPx": "0.0402"}] * 30
        a = analytics.compute_analytics([], [], chs, spot, 1_700_000_000_000, NOW,
                                        staking={"delegated": "249406"}, spot_price_by_symbol={"HYPE": 87.4},
                                        open_orders=orders)
        self.assertEqual(a["perp_spot_overlap_rule"], "hold_reserved")
        self.assertAlmostEqual(a["perp_spot_overlap_usd"], 7407327.0)
        self.assertAlmostEqual(a["net_worth"], 24531182 + 249406 * 87.4, places=0)

    def test_separate_accounts_hold_zero_no_overlap(self):
        # live shape (0x16c9…): perp 257k funded in the perp book, spot USDC 58k with
        # hold 0 -> oracle == perp + spot; nothing overlaps
        from blueblocx_hyperliquid import analytics
        chs = {"marginSummary": {"accountValue": "257454", "totalMarginUsed": "48392"}, "assetPositions": []}
        spot = {"balances": [{"coin": "USDC", "token": 0, "total": "58260", "hold": "0"},
                             {"coin": "HYPE", "token": 150, "total": "3710"}]}
        a = analytics.compute_analytics([], [], chs, spot, 1_700_000_000_000, NOW,
                                        spot_price_by_symbol={"HYPE": 88.0})
        self.assertEqual(a["perp_spot_overlap_rule"], "separate")
        self.assertEqual(a["perp_spot_overlap_usd"], 0.0)
        self.assertAlmostEqual(a["net_worth"], 257454 + 58260 + 3710 * 88.0)

    def test_no_spot_usdc_row_means_no_overlap(self):
        # live shape (market maker): equity only in the perp book
        from blueblocx_hyperliquid import analytics
        chs = {"marginSummary": {"accountValue": "3182671"}, "assetPositions": []}
        a = analytics.compute_analytics([], [], chs, {"balances": []}, 1_700_000_000_000, NOW)
        self.assertEqual(a["perp_spot_overlap_rule"], "none")
        self.assertAlmostEqual(a["net_worth"], 3182671.0)


if __name__ == "__main__":
    unittest.main()
