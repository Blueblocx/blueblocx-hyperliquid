"""Token Heatmap (v1.11.0, TOKEN_HEATMAP_DATA_SPEC.md section 2): the pure row builders of
heatmap.py on synthetic clearinghouseState / leaderboard / metaAndAssetCtxs payloads, the
cohort bands, the registration of the three tables, the wiring in extract.run (default
OFF = zero change; ON = three staged CSVs, the separate guarded lift, pending_since), the
daily seed and the maintenance retention step against the local SQLite catalog. No network."""
from __future__ import annotations

import csv
import datetime as dt
import json
import os
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

from blueblocx_hyperliquid import config, extract, heatmap, maintenance, ops, preflight, tracked
from blueblocx_hyperliquid import iceberg_load as il
from blueblocx_hyperliquid import schema as S
from tests.test_tracked import _FakeClient, _FakeTime

W1 = "0x" + "11" * 20
W2 = "0x" + "22" * 20
W3 = "0x" + "33" * 20
T0 = 1_762_600_000.0                       # 2025-11-08 ~10:26:40 UTC
NOW_MS = int(T0 * 1000)
BUCKET_MS = (NOW_MS // 3_600_000) * 3_600_000
RUN_ID = "hourly-%d" % NOW_MS
NOW_ISO = dt.datetime.fromtimestamp(T0, tz=dt.timezone.utc).isoformat()
SIZE_B = [100_000.0, 500_000.0, 1_000_000.0, 5_000_000.0]
PNL_B = [-10_000.0, 10_000.0, 100_000.0, 1_000_000.0]


def _pos(coin, szi, px, *, lev=("cross", 5), max_lev=50, upnl="100", liq=None, cf=("1", "0.5", "0.1")):
    return {"type": "oneWay", "position": {
        "coin": coin, "szi": str(szi), "entryPx": str(px), "positionValue": str(abs(szi) * px * 1.01),
        "unrealizedPnl": upnl, "returnOnEquity": "0.02", "liquidationPx": liq, "marginUsed": "20",
        "maxLeverage": max_lev, "leverage": {"type": lev[0], "value": lev[1]},
        "cumFunding": {"allTime": cf[0], "sinceOpen": cf[1], "sinceChange": cf[2]}}}


def _chs(account_value, positions):
    return {"marginSummary": {"accountValue": str(account_value), "totalNtlPos": "0", "totalRawUsd": "0",
                              "totalMarginUsed": "0"},
            "crossMarginSummary": {"accountValue": str(account_value)}, "withdrawable": "1", "time": 0,
            "assetPositions": positions}


class _Env:
    def __init__(self, **kv):
        self.kv, self.old = kv, {}

    def __enter__(self):
        for k, v in self.kv.items():
            self.old[k] = os.environ.get(k)
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = str(v)
        return self

    def __exit__(self, *exc):
        for k, v in self.old.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


class CohortTests(unittest.TestCase):
    def test_size_cohort_bands_and_fine_ids(self):
        cases = [(0, "under_100k", 16), (249.99, "under_100k", 16), (250, "under_100k", 1), (9_999, "under_100k", 1),
                 (10_000, "under_100k", 2), (50_000, "under_100k", 3), (99_999.99, "under_100k", 3),
                 (100_000, "small_whale", 4), (499_999, "small_whale", 4), (500_000, "whale", 5),
                 (1_000_000, "tidal_whale", 6), (4_999_999, "tidal_whale", 6), (5_000_000, "leviathan", 7),
                 (1e9, "leviathan", 7)]
        for e, name, fine in cases:
            self.assertEqual(heatmap.size_cohort(e, SIZE_B), (name, fine), e)
        self.assertEqual(heatmap.size_cohort(None, SIZE_B), (None, None))
        self.assertEqual(heatmap.size_cohort("nan", SIZE_B), (None, None))
        self.assertEqual(heatmap.size_cohort("750000", SIZE_B), ("whale", 5))        # CSV strings welcome

    def test_perf_cohort_bands_and_fine_ids(self):
        cases = [(1_000_000, "money_printer", 8), (5e7, "money_printer", 8), (999_999, "smart_money", 9),
                 (100_000, "smart_money", 9), (99_999, "consistent_trader", 10), (10_000, "consistent_trader", 10),
                 (9_999, "average_trader", 11), (0, "average_trader", 11), (-1, "average_trader", 12),
                 (-9_999, "average_trader", 12), (-10_000, "losing_trader", 13), (-99_999, "losing_trader", 13),
                 (-100_000, "losing_trader", 14), (-999_999, "losing_trader", 14), (-1_000_000, "losing_trader", 15)]
        for p, name, fine in cases:
            self.assertEqual(heatmap.perf_cohort(p, PNL_B), (name, fine), p)
        self.assertEqual(heatmap.perf_cohort(None, PNL_B), ("unknown", None))

    def test_bounds_come_from_env_and_are_validated(self):
        with _Env(HL_HEATMAP_SIZE_BOUNDS="1,2,3,4", HL_HEATMAP_PNL_BOUNDS="-1,0,1,2"):
            self.assertEqual(heatmap.size_bounds(), [1, 2, 3, 4])
            self.assertEqual(heatmap.size_cohort(2.5), ("whale", 5))
            self.assertEqual(heatmap.perf_cohort(1.5), ("smart_money", 9))
        for bad in ("1,2,3", "4,3,2,1", "a,b,c,d", "1,1,2,3", "", "1,2,3,inf"):
            with self.assertRaises(ValueError, msg=bad):
                heatmap.parse_bounds(bad)
        with _Env(HL_HEATMAP_SIZE_BOUNDS=None, HL_HEATMAP_PNL_BOUNDS=None):
            self.assertEqual(heatmap.size_bounds(), SIZE_B)                 # the documented defaults
            self.assertEqual(heatmap.pnl_bounds(), PNL_B)

    def test_bucket_and_run_id(self):
        self.assertEqual(heatmap.bucket_start_ms(NOW_MS), BUCKET_MS)
        self.assertEqual(heatmap.bucket_start_ms(NOW_MS) % 3_600_000, 0)
        self.assertEqual(heatmap.bucket_start_ms(NOW_MS, 30) % 1_800_000, 0)
        self.assertGreaterEqual(heatmap.bucket_start_ms(NOW_MS, 15), heatmap.bucket_start_ms(NOW_MS, 60))
        self.assertTrue(heatmap.iso(BUCKET_MS).endswith(":00:00+00:00"))
        self.assertEqual(heatmap.make_run_id("hourly", NOW_MS), RUN_ID)
        self.assertEqual(heatmap.date_of(BUCKET_MS), heatmap.iso(BUCKET_MS)[:10])

    def test_enabled_requires_the_switch_and_the_hourly_writer(self):
        with _Env(HL_HEATMAP=None, HL_HEATMAP_WRITER=None):
            self.assertFalse(heatmap.enabled())                             # default OFF
        with _Env(HL_HEATMAP="1", HL_HEATMAP_WRITER=None):
            self.assertTrue(heatmap.enabled())
        with _Env(HL_HEATMAP="1", HL_HEATMAP_WRITER="sweep"):
            self.assertFalse(heatmap.enabled())
        with _Env(HL_HEATMAP="0", HL_HEATMAP_WRITER="hourly"):
            self.assertFalse(heatmap.enabled())


class PositionRowTests(unittest.TestCase):
    def setUp(self):
        self.main = _chs(2_000_000, [_pos("BTC", 2, 50_000), _pos("ETH", -10, 3_000, lev=("isolated", 3), liq="3500"),
                                     _pos("DOGE", 0, 0.1)])
        self.xyz = _chs(500_000, [_pos("xyz:SP500", 100, 60, max_lev=20)])
        self.io = _chs(0, [])

    def test_rows_from_main_and_hip3_with_wallet_equity_summed_over_dexes(self):
        rows = heatmap.position_rows(self.main, {"xyz": self.xyz, "io": self.io}, W1.upper(), bucket_ms=BUCKET_MS,
                                     fetched_at_ms=NOW_MS + 5_000, run_id=RUN_ID, now_iso=NOW_ISO)
        self.assertEqual([(r["dex"], r["coin"], r["side"]) for r in rows],
                         [("main", "BTC", "long"), ("main", "ETH", "short"), ("xyz", "xyz:SP500", "long")])   # szi == 0 skipped
        for r in rows:
            self.assertEqual(set(r), set(S.L4X_PERP_POSITION_SNAP_COLUMNS))
            self.assertEqual(r["as_of_bucket"], heatmap.iso(BUCKET_MS))
            self.assertEqual(r["bucket_minutes"], 60)
            self.assertEqual(r["as_of_ts"], heatmap.iso(NOW_MS + 5_000))         # the wallet's fetch time, not the run start
            self.assertEqual(r["as_of_date"], heatmap.iso(BUCKET_MS)[:10])
            self.assertEqual(r["wallet_address"], W1)                               # lowercased
            self.assertEqual(r["wallet_group"], S.addr_group(W1))
            self.assertEqual(r["wallet_perp_equity_usd"], 2_500_000.0)             # main + xyz (+ io = 0)
            self.assertEqual((r["source"], r["run_id"], r["created_at"]), ("hourly", RUN_ID, NOW_ISO))
        btc, eth, sp = rows
        self.assertEqual((btc["szi"], btc["entry_px"], btc["max_leverage"], btc["leverage_type"], btc["leverage_value"]),
                         (2.0, 50_000.0, 50, "cross", 5.0))
        self.assertAlmostEqual(btc["mark_px"], 50_500.0)
        self.assertEqual((btc["cum_funding_all_time"], btc["cum_funding_since_open"], btc["cum_funding_since_change"]),
                         (1.0, 0.5, 0.1))
        self.assertEqual((btc["account_value_usd"], sp["account_value_usd"]), (2_000_000.0, 500_000.0))
        self.assertEqual((eth["leverage_type"], eth["liquidation_px"], eth["side"]), ("isolated", 3500.0, "short"))
        self.assertIsNone(btc["liquidation_px"])
        self.assertEqual(sp["max_leverage"], 20)

    def test_equity_helpers(self):
        self.assertEqual(heatmap.wallet_perp_equity(self.main, {"xyz": self.xyz, "io": self.io}), 2_500_000.0)
        self.assertEqual(heatmap.wallet_perp_equity(self.main, None), 2_000_000.0)
        self.assertEqual(heatmap.wallet_perp_equity({}, {}), 0.0)
        collateral = {"xyz": "USDC", "flx": "USDH", "io": "USDC(assumed)"}
        self.assertFalse(heatmap.equity_has_non_usd({"xyz": self.xyz, "io": self.io}, collateral))
        self.assertTrue(heatmap.equity_has_non_usd({"flx": _chs(10, [])}, collateral))
        self.assertFalse(heatmap.equity_has_non_usd({"flx": _chs(0, [])}, collateral))       # zero equity: nothing contributed
        self.assertFalse(heatmap.equity_has_non_usd({"unknown": _chs(5, [])}, {}))          # assumed USDC

    def test_flat_wallet_yields_no_rows_but_a_present_snapshot(self):
        self.assertEqual(heatmap.position_rows(_chs(1234, []), {}, W1, bucket_ms=BUCKET_MS, fetched_at_ms=NOW_MS,
                                               run_id=RUN_ID, now_iso=NOW_ISO), [])


def _lb(addr, av, *, day=(1, 0.01, 10), week=(2, 0.02, 20), month=(3, 0.03, 30), allt=(4, 0.04, 40), rank=None):
    return tracked.Row(addr, av, month[0], month[1], month[2], day[0], day[1], day[2], week[0], week[1], week[2],
                       allt[0], allt[1], allt[2], rank)


class CohortRowTests(unittest.TestCase):
    def test_present_deferred_and_unlisted_wallets(self):
        lb = {W1: _lb(W1, 50e6, allt=(2e6, 0.5, 1e9), month=(1e5, 0.1, 5e6), rank=3),
              W2: _lb(W2, 2e5, allt=(-20_000, -0.1, 1e6), month=(0, 0, 0), rank=900)}
        snaps = {W1: {"equity": 2_500_000.0, "has_non_usd": False, "open_positions": 3},
                 W3: {"equity": 50.0, "has_non_usd": True, "open_positions": 0}}
        rows = heatmap.cohort_rows([W1, W2, W3, W1.upper()], snaps, bucket_ms=BUCKET_MS, run_start_ms=NOW_MS, run_id=RUN_ID,
                                   now_iso=NOW_ISO, leaderboard_rows=lb, whales=[W1], smart=[W2], pins=[W3],
                                   leaderboard_fetched_at="2026-11-08T10:11:00+00:00",
                                   leaderboard_last_modified="2026-11-08T09:23:05+00:00", size_b=SIZE_B, pnl_b=PNL_B)
        self.assertEqual([r["wallet_address"] for r in rows], [W1, W2, W3])            # one row per planned wallet, deduped
        by = {r["wallet_address"]: r for r in rows}
        for r in rows:
            self.assertEqual(set(r), set(S.L4X_WALLET_COHORT_SNAP_COLUMNS))
            self.assertEqual((r["as_of_bucket"], r["bucket_minutes"], r["as_of_ts"], r["as_of_date"]),
                             (heatmap.iso(BUCKET_MS), 60, heatmap.iso(NOW_MS), heatmap.iso(BUCKET_MS)[:10]))
            self.assertEqual((r["size_cohort_version"], r["perf_cohort_version"]), ("size_v1", "perf_v1"))
            self.assertEqual((r["leaderboard_fetched_at"], r["leaderboard_last_modified"]),
                             ("2026-11-08T10:11:00+00:00", "2026-11-08T09:23:05+00:00"))
        w1 = by[W1]          # fetched, whale, money printer, active
        self.assertTrue(w1["position_snapshot_present"])
        self.assertEqual((w1["perp_equity_usd"], w1["equity_source"], w1["equity_has_non_usd"]), (2_500_000.0, "snapshot", False))
        self.assertEqual((w1["size_cohort"], w1["size_cohort_fine"]), ("tidal_whale", 6))
        self.assertEqual((w1["leaderboard_account_value_usd"], w1["rank_account_value"]), (50e6, 3))
        self.assertEqual((w1["pnl_all"], w1["roi_all"], w1["vlm_all"]), (2e6, 0.5, 1e9))
        self.assertEqual((w1["pnl_month"], w1["roi_month"], w1["vlm_month"]), (1e5, 0.1, 5e6))
        self.assertEqual((w1["pnl_day"], w1["pnl_week"]), (1, 2))
        self.assertEqual((w1["perf_cohort"], w1["perf_cohort_fine"]), ("money_printer", 8))
        self.assertEqual((w1["is_tracked_whale"], w1["is_tracked_smart"], w1["is_pinned"]), (True, False, False))
        self.assertEqual((w1["is_active"], w1["has_open_position"]), (True, True))
        w2 = by[W2]          # deferred (not fetched): equity / size NULL, leaderboard columns filled, inactive
        self.assertFalse(w2["position_snapshot_present"])
        self.assertIsNone(w2["perp_equity_usd"])
        self.assertIsNone(w2["equity_source"])
        self.assertIsNone(w2["size_cohort"])
        self.assertIsNone(w2["has_open_position"])
        self.assertEqual((w2["perf_cohort"], w2["perf_cohort_fine"]), ("losing_trader", 13))
        self.assertEqual((w2["is_tracked_whale"], w2["is_tracked_smart"], w2["is_pinned"]), (False, True, False))
        self.assertFalse(w2["is_active"])                                                   # vlm_month 0, no snapshot
        w3 = by[W3]          # pinned wallet below the leaderboard floor: fetched, flat, perf unknown
        self.assertTrue(w3["position_snapshot_present"])
        self.assertEqual((w3["size_cohort"], w3["size_cohort_fine"], w3["equity_has_non_usd"]), ("under_100k", 16, True))
        self.assertEqual((w3["perf_cohort"], w3["perf_cohort_fine"]), ("unknown", None))
        self.assertIsNone(w3["pnl_all"])
        self.assertIsNone(w3["rank_account_value"])
        self.assertEqual((w3["is_tracked_whale"], w3["is_tracked_smart"], w3["is_pinned"]), (False, False, True))
        self.assertEqual((w3["is_active"], w3["has_open_position"]), (False, False))

    def test_without_a_leaderboard_the_tracked_flags_are_null_not_false(self):
        rows = heatmap.cohort_rows([W1, W2], {W1: {"equity": 1e6, "has_non_usd": False, "open_positions": 1}},
                                   bucket_ms=BUCKET_MS, run_start_ms=NOW_MS, run_id=RUN_ID, now_iso=NOW_ISO,
                                   leaderboard_rows=None, size_b=SIZE_B, pnl_b=PNL_B)
        w1, w2 = rows
        self.assertIsNone(w1["is_tracked_whale"])
        self.assertIsNone(w1["is_tracked_smart"])
        self.assertEqual((w1["perf_cohort"], w1["is_active"]), ("unknown", True))      # open position keeps it active
        self.assertIsNone(w2["is_active"])                                              # nothing known about W2 this bucket


class MarketCtxTests(unittest.TestCase):
    META = {"universe": [{"name": "BTC", "szDecimals": 5, "maxLeverage": 40},
                         {"name": "OLD", "szDecimals": 0, "maxLeverage": 3, "isDelisted": True},
                         {"name": "ETH", "szDecimals": 4, "maxLeverage": 25}]}
    CTXS = [{"funding": "0.0000125", "openInterest": "1000", "prevDayPx": "99000", "dayNtlVlm": "5e9", "premium": "0.0001",
             "oraclePx": "100500", "markPx": "100000", "midPx": "100001"},
            {"funding": "0", "openInterest": "0", "markPx": "1"},
            {"funding": "-0.00001", "openInterest": "20000", "prevDayPx": "3000", "dayNtlVlm": "1e9", "premium": None,
             "oraclePx": "3100", "markPx": "3050"}]

    def test_rows_pair_universe_with_ctxs_and_skip_delisted(self):
        rows = heatmap.market_ctx_rows(self.META, self.CTXS, "main", bucket_ms=BUCKET_MS, as_of_ms=NOW_MS + 1,
                                       run_id=RUN_ID, now_iso=NOW_ISO)
        self.assertEqual([r["coin"] for r in rows], ["BTC", "ETH"])
        btc = rows[0]
        self.assertEqual(set(btc), set(S.L4X_PERP_MARKET_CTX_SNAP_COLUMNS))
        self.assertEqual((btc["dex"], btc["open_interest"], btc["mark_px"], btc["open_interest_usd"]), ("main", 1000.0, 100_000.0, 1e8))
        self.assertEqual((btc["funding_1h"], btc["oracle_px"], btc["prev_day_px"], btc["day_ntl_vlm"], btc["premium"], btc["max_leverage"]),
                         (0.0000125, 100_500.0, 99_000.0, 5e9, 0.0001, 40))
        self.assertEqual((btc["as_of_bucket"], btc["as_of_ts"], btc["bucket_minutes"], btc["source"], btc["run_id"]),
                         (heatmap.iso(BUCKET_MS), heatmap.iso(NOW_MS + 1), 60, "hourly", RUN_ID))
        self.assertIsNone(rows[1]["premium"])

    def test_hip3_coins_are_dex_qualified(self):
        meta = {"universe": [{"name": "xyz:SP500", "maxLeverage": 20}, {"name": "GOLD", "maxLeverage": 10}]}
        ctxs = [{"markPx": "6000", "openInterest": "10"}, {"markPx": "4000", "openInterest": "1"}]
        rows = heatmap.market_ctx_rows(meta, ctxs, "xyz", bucket_ms=BUCKET_MS, as_of_ms=NOW_MS, run_id=RUN_ID, now_iso=NOW_ISO)
        self.assertEqual([(r["coin"], r["dex"]) for r in rows], [("xyz:SP500", "xyz"), ("xyz:GOLD", "xyz")])
        # a ragged response (fewer ctxs than universe entries) stops at the shorter side
        self.assertEqual(len(heatmap.market_ctx_rows(meta, ctxs[:1], "xyz", bucket_ms=BUCKET_MS, as_of_ms=NOW_MS,
                                                     run_id=RUN_ID, now_iso=NOW_ISO)), 1)

    def test_fetch_market_ctx_one_call_per_dex_and_isolated_failures(self):
        calls = []

        class C:
            def meta_and_asset_ctxs(self, dex=None):
                calls.append(dex)
                if dex == "io":
                    raise RuntimeError("HTTP 500")
                return MarketCtxTests.META, MarketCtxTests.CTXS
        rows, errors = heatmap.fetch_market_ctx(C(), ["main", "xyz", "io"], bucket_ms=BUCKET_MS, run_id=RUN_ID,
                                                now_iso=NOW_ISO, clock=_FakeTime(T0))
        self.assertEqual(calls, [None, "xyz", "io"])               # main = no dex parameter
        self.assertEqual(len(rows), 4)
        self.assertEqual(list(errors), ["io"])
        with _Env(HL_HEATMAP_MARKET_DEXES=" main, xyz,,xyz "):
            self.assertEqual(heatmap.market_dexes(), ["main", "xyz"])
        with _Env(HL_HEATMAP_MARKET_DEXES=None):
            self.assertEqual(heatmap.market_dexes(), ["main", "xyz", "io", "mkts", "para"])

    def test_saw_rate_limit(self):
        self.assertTrue(heatmap.saw_rate_limit({W1: {"fills": "RuntimeError: Info API failed after 5 tries: HTTP 429"}}))
        self.assertFalse(heatmap.saw_rate_limit({W1: {"fills": "HTTP 500"}, W2: {}}))
        self.assertFalse(heatmap.saw_rate_limit(None))


class TrackedWindowsTests(unittest.TestCase):
    def test_parse_keeps_four_windows_and_ranks_by_account_value(self):
        def row(addr, av, allt):
            return {"ethAddress": addr, "accountValue": str(av), "windowPerformances": [
                ["day", {"pnl": "1", "roi": "0.1", "vlm": "10"}], ["week", {"pnl": "2", "roi": "0.2", "vlm": "20"}],
                ["month", {"pnl": "3", "roi": "0.3", "vlm": "30"}], ["allTime", {"pnl": str(allt), "roi": "0.4", "vlm": "40"}],
                ["bogus", {"pnl": "9"}]]}
        rows, total = tracked.parse_leaderboard(json.dumps({"leaderboardRows": [row(W1, 2e5, 5e6), row(W2, 9e6, -1), row(W3, 5e4, 1)]}))
        self.assertEqual(total, 3)
        by = {r.address: r for r in rows}
        self.assertEqual(set(by), {W1, W2})                                      # W3 below the floor
        self.assertEqual(by[W1].window("day"), (1.0, 0.1, 10.0))
        self.assertEqual(by[W1].window("week"), (2.0, 0.2, 20.0))
        self.assertEqual(by[W1].window("month"), (3.0, 0.3, 30.0))
        self.assertEqual(by[W1].window("allTime"), (5e6, 0.4, 40.0))
        self.assertEqual(by[W1].window("all"), by[W1].window("allTime"))
        self.assertEqual((by[W2].rank_account_value, by[W1].rank_account_value), (1, 2))
        self.assertEqual((by[W1].month_pnl, by[W1].month_roi, by[W1].month_vlm), (3.0, 0.3, 30.0))   # unchanged fields
        # a row without allTime: 0.0 like every other missing number (never a crash)
        rows2, _ = tracked.parse_leaderboard(json.dumps({"leaderboardRows": [{"ethAddress": W1, "accountValue": "2e5"}]}))
        self.assertEqual(rows2[0].window("all"), (0.0, 0.0, 0.0))

    def test_last_modified_header_is_captured_and_converted(self):
        class R:
            status_code = 200
            headers = {"Last-Modified": "Sat, 08 Nov 2026 19:22:59 GMT"}

            def raise_for_status(self):
                pass

            def iter_content(self, n):
                yield b'{"leaderboardRows": []}'

            def close(self):
                pass
        hdrs: dict = {}
        tracked.fetch_leaderboard_text("u", get=lambda url, **kw: R(), headers_out=hdrs)
        self.assertEqual(hdrs["last_modified"], "Sat, 08 Nov 2026 19:22:59 GMT")
        self.assertEqual(tracked.http_date_to_iso(hdrs["last_modified"]), "2026-11-08T19:22:59+00:00")
        self.assertIsNone(tracked.http_date_to_iso(None))
        self.assertIsNone(tracked.http_date_to_iso("not a date"))

    def test_refresh_tracked_returns_rows_for_the_plan_and_the_last_modified(self):
        def row(addr, av, pnl=0, roi=0, vlm=0):
            return {"ethAddress": addr, "accountValue": str(av), "windowPerformances": [
                ["month", {"pnl": str(pnl), "roi": str(roi), "vlm": str(vlm)}], ["allTime", {"pnl": "7", "roi": "0", "vlm": "0"}]]}
        payload = json.dumps({"leaderboardRows": [row(W1, 9e8), row(W2, 2e5, 1e6, 0.5, 6e6), row(W3, 1.5e5)]})

        def fetch(url, headers_out=None):
            headers_out["last_modified"] = "Sat, 08 Nov 2026 19:22:59 GMT"
            return payload
        with tempfile.TemporaryDirectory() as d:
            sets, meta = tracked.refresh_tracked(Path(d), now=T0, fetch=fetch, keep_rows_for=[W3.upper()],
                                                 whale_top_n=1, smart_top_n=5, smart_min_account_value=1e5,
                                                 smart_min_month_roi=0.2, smart_min_month_vlm=5e6)
            self.assertEqual(sets, {"whales": [W1], "smart": [W2]})
            self.assertEqual(meta["last_modified"], "2026-11-08T19:22:59+00:00")
            self.assertEqual(set(meta["rows_by_address"]), {W1, W2, W3})           # selected + pins
            self.assertEqual(meta["rows_by_address"][W1].all_pnl, 7.0)
            self.assertEqual(tracked.load_cached(Path(d))["last_modified"], "2026-11-08T19:22:59+00:00")
            # a fetcher without the kwarg (older stand-ins) still works, without a Last-Modified
            sets2, meta2 = tracked.refresh_tracked(Path(d), now=T0, fetch=lambda url: payload, whale_top_n=1, smart_top_n=5,
                                                   smart_min_account_value=1e5, smart_min_month_roi=0.2, smart_min_month_vlm=5e6)
            self.assertEqual(sets2, sets)
            self.assertIsNone(meta2["last_modified"])


class RegistrationTests(unittest.TestCase):
    def test_tables_registered_everywhere_with_the_spec_shapes(self):
        from blueblocx_hyperliquid import duckdb_smoke, query
        ddl = il.parse_ddl()
        expect = {"l4x_perp_position_snap": (S.L4X_PERP_POSITION_SNAP_COLUMNS, 28,
                                             (["as_of_bucket", "wallet_address", "coin"], "replace_scope", ["as_of_bucket", "wallet_address"])),
                  "l4x_wallet_cohort_snap": (S.L4X_WALLET_COHORT_SNAP_COLUMNS, 39,
                                             (["as_of_bucket", "wallet_address"], "replace_scope", ["as_of_bucket"])),
                  "l4x_perp_market_ctx_snap": (S.L4X_PERP_MARKET_CTX_SNAP_COLUMNS, 18,
                                               (["as_of_bucket", "coin"], "replace_scope", ["as_of_date", "as_of_bucket"]))}
        self.assertEqual(tuple(expect), il.HEATMAP_TABLES)
        self.assertEqual(heatmap.TABLES, il.HEATMAP_TABLES)
        for name, (cols, n, plan) in expect.items():
            self.assertEqual([c for c, _ in ddl[name]["columns"]], cols, name)       # CSV order = DDL order, one column per line
            self.assertEqual(len(cols), n, name)
            self.assertEqual(ddl[name]["partition"], ["as_of_date"], name)
            self.assertTrue({t for _, t in ddl[name]["columns"]} <= {"BIGINT", "INT", "DOUBLE", "STRING", "DATE", "BOOLEAN", "TIMESTAMP"})
            self.assertEqual(il.TABLE_PLAN[name], plan, name)
            self.assertNotIn(name, il.ROLLUP_TABLES)
            self.assertIn(name, il.OPTIONAL_TABLES)                                   # absent while HL_HEATMAP=0: not a warning
            self.assertEqual(il._STAMP_COL[name], "created_at")
            self.assertEqual(il._BUSINESS_DATE_COL[name], "as_of_date")
            self.assertEqual(il._SINCE_FALLBACK_COL[name], "as_of_ts")
            self.assertEqual(query.DATE_COLUMN[name], "as_of_date")
            self.assertEqual(maintenance.COMPACT_CLOSED_DAYS[name], 1)
            self.assertIn(name, duckdb_smoke._CREATED_AT_STAMPED)
            for c in il.REQUIRED_COLS[name]:
                self.assertIn(c, cols, f"{name}.{c}")
        self.assertEqual(il.REQUIRED_COLS["l4x_perp_position_snap"],
                         ["as_of_bucket", "bucket_minutes", "as_of_ts", "as_of_date", "wallet_address", "coin", "dex", "side", "source", "created_at"])
        self.assertEqual(il.REQUIRED_COLS["l4x_wallet_cohort_snap"],
                         ["as_of_bucket", "bucket_minutes", "as_of_ts", "as_of_date", "wallet_address", "position_snapshot_present", "source", "created_at"])
        self.assertEqual(il.REQUIRED_COLS["l4x_perp_market_ctx_snap"],
                         ["as_of_bucket", "bucket_minutes", "as_of_ts", "as_of_date", "coin", "dex", "source", "created_at"])
        # the lift order: after the l4x snapshots they extend, before the rollup tables
        names = list(il.TABLE_PLAN)
        self.assertLess(names.index("l4x_spot_balance_snapshot"), names.index("l4x_perp_position_snap"))
        self.assertLess(names.index("l4x_perp_market_ctx_snap"), names.index("l1_token_categories"))

    def test_env_vars_documented_and_preflight_accepts_them(self):
        for k in ("HL_HEATMAP", "HL_HEATMAP_WRITER", "HL_HEATMAP_SIZE_BOUNDS", "HL_HEATMAP_PNL_BOUNDS", "HL_HEATMAP_MARKET_DEXES",
                  "HL_HEATMAP_LIFT_DEADLINE_S", "HL_HEATMAP_LAKE_RETENTION_DAYS", "HL_HEATMAP_CTX_RETENTION_DAYS"):
            self.assertIn(k, config.ENV_VARS)
            self.assertIn(k, (Path(__file__).resolve().parent.parent / "deploy" / ".env.example").read_text(encoding="utf-8"))
        self.assertEqual(config.ENV_VARS["HL_HEATMAP"][0], "1")   # default ON since the 2026-11-21 release decision
        self.assertEqual(config.ENV_VARS["HL_HEATMAP_LIFT_DEADLINE_S"][0], "2900")
        with _Env(HL_HEATMAP="0", HL_HEATMAP_WRITER=None, HL_HEATMAP_SIZE_BOUNDS=None, HL_HEATMAP_PNL_BOUNDS=None):
            c = next(c for c in preflight.check_env(None) if c.name == "heatmap")
            self.assertEqual(c.status, "OK")
            self.assertIn("off", c.detail)
        with _Env(HL_HEATMAP="1", HL_HEATMAP_WRITER="hourly", HL_HEATMAP_LIFT_DEADLINE_S="2400"):
            c = next(c for c in preflight.check_env(None) if c.name == "heatmap")
            self.assertEqual(c.status, "OK")
            self.assertIn("+100 weight", c.detail)
        with _Env(HL_HEATMAP="1", HL_HEATMAP_SIZE_BOUNDS="3,2,1"):
            self.assertEqual(next(c for c in preflight.check_env(None) if c.name == "heatmap").status, "FAIL")
        with _Env(HL_HEATMAP="1", HL_HEATMAP_WRITER="nope"):
            self.assertEqual(next(c for c in preflight.check_env(None) if c.name == "heatmap").status, "FAIL")
        with _Env(HL_HEATMAP="1", HL_HEATMAP_WRITER="sweep"):
            self.assertEqual(next(c for c in preflight.check_env(None) if c.name == "heatmap").status, "WARN")


class _HeatClient(_FakeClient):
    """The tracked-tests client plus positions and a metaAndAssetCtxs that counts its calls."""
    ctx_calls: list = []
    chs_by_wallet: dict = {}

    def clearinghouse_state(self, wallet, dex=None):
        return json.loads(json.dumps(self.chs_by_wallet.get((wallet, dex), self._EMPTY_CHS)))

    def meta_and_asset_ctxs(self, dex=None):
        self.ctx_calls.append(dex)
        return MarketCtxTests.META, MarketCtxTests.CTXS


def _read(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


class ExtractWiringTests(unittest.TestCase):
    def _run(self, out: Path, wallets, *, clock, env, **kw):
        fake = _HeatClient
        fake.clock, fake.bump, fake.fills, fake.ctx_calls = clock, 0.0, {}, []
        fake.chs_by_wallet = {(W1, None): _chs(2_000_000, [_pos("BTC", 2, 50_000)]), (W2, None): _chs(500, [])}
        with _Env(**env), mock.patch.object(extract, "InfoClient", fake), mock.patch.object(extract, "time", clock):
            return extract.run(wallets, 2, out, incremental=True, hip3=False, **kw)

    def test_off_by_default_nothing_changes(self):
        with tempfile.TemporaryDirectory() as d:
            recon = self._run(Path(d), [W1], clock=_FakeTime(T0), env={"HL_HEATMAP": None})
            ns = Path(d) / S.NS_HYPERCORE
            self.assertIsNone(recon["heatmap"])
            self.assertEqual([p.name for p in ns.glob("l4x_*_snap.csv")], [])
            self.assertEqual(_HeatClient.ctx_calls, [])
            summary = ops.build_summary(recon, [W1], ops.utcnow(), ops.utcnow())
            self.assertIsNone(summary["heatmap"])
            self.assertFalse(any(w.startswith("heatmap") for w in summary["warnings"]))

    def test_on_stages_three_csvs_from_the_pull_and_one_ctx_call_per_dex(self):
        clock = _FakeTime(T0)
        lb = {W1: _lb(W1, 50e6, allt=(5e6, 1, 1e9), rank=1)}
        with tempfile.TemporaryDirectory() as d:
            recon = self._run(Path(d), [W1, W2, W3], clock=clock, env={"HL_HEATMAP": "1", "HL_HEATMAP_MARKET_DEXES": "main,xyz"},
                              soft_wallets={W1, W2}, deadline_ts=None, leaderboard_rows=lb,
                              leaderboard_meta={"fetched_at": "2026-11-08T10:11:00+00:00", "last_modified": "2026-11-08T09:23:05+00:00"},
                              tracked_sets={"whales": [W1], "smart": [W2]})
            ns = Path(d) / S.NS_HYPERCORE
            heat = recon["heatmap"]
            self.assertTrue(heat["enabled"])
            self.assertEqual((heat["positions"], heat["cohort"], heat["market_ctx"], heat["wallets_present"]), (1, 3, 4, 3))
            self.assertNotIn("error", heat)
            self.assertEqual(_HeatClient.ctx_calls, [None, "xyz"])                     # the only new Info calls
            self.assertEqual(heat["bucket"], heatmap.iso(heatmap.bucket_start_ms(NOW_MS)))
            pos = _read(ns / "l4x_perp_position_snap.csv")
            self.assertEqual([(r["wallet_address"], r["coin"], r["side"], r["wallet_perp_equity_usd"]) for r in pos],
                             [(W1, "BTC", "long", "2000000.0")])
            self.assertEqual(pos[0]["as_of_ts"], heatmap.iso(NOW_MS))                     # the fake clock: fetch time = run start
            coh = {r["wallet_address"]: r for r in _read(ns / "l4x_wallet_cohort_snap.csv")}
            self.assertEqual(set(coh), {W1, W2, W3})
            self.assertEqual((coh[W1]["position_snapshot_present"], coh[W1]["size_cohort"], coh[W1]["perf_cohort"],
                              coh[W1]["is_tracked_whale"], coh[W1]["is_pinned"], coh[W1]["has_open_position"]),
                             ("True", "tidal_whale", "money_printer", "True", "False", "True"))
            self.assertEqual((coh[W2]["size_cohort"], coh[W2]["perf_cohort"], coh[W2]["is_tracked_smart"], coh[W2]["has_open_position"]),
                             ("under_100k", "unknown", "True", "False"))
            self.assertEqual((coh[W3]["is_pinned"], coh[W3]["perf_cohort"], coh[W3]["leaderboard_last_modified"]),
                             ("True", "unknown", "2026-11-08T09:23:05+00:00"))
            ctx = _read(ns / "l4x_perp_market_ctx_snap.csv")
            self.assertEqual(sorted((r["dex"], r["coin"]) for r in ctx), [("main", "BTC"), ("main", "ETH"), ("xyz", "xyz:BTC"), ("xyz", "xyz:ETH")])
            # a second run in the SAME bucket replaces the bucket's cohort / ctx rows and W1's positions, never duplicates
            recon2 = self._run(Path(d), [W1], clock=clock, env={"HL_HEATMAP": "1", "HL_HEATMAP_MARKET_DEXES": "main,xyz"},
                               leaderboard_rows=None, tracked_sets=None)
            self.assertEqual(len(_read(ns / "l4x_perp_position_snap.csv")), 1)
            self.assertEqual(len(_read(ns / "l4x_wallet_cohort_snap.csv")), 1)          # the new plan is one wallet
            self.assertEqual(len(_read(ns / "l4x_perp_market_ctx_snap.csv")), 4)
            self.assertEqual(recon2["heatmap"]["leaderboard"], "absent")
            summary = ops.build_summary(recon2, [W1], ops.utcnow(), ops.utcnow())
            self.assertFalse(any(w.startswith("heatmap") for w in summary["warnings"]))
            self.assertTrue(summary["heatmap"]["enabled"])

    def test_deferred_wallets_get_a_present_false_row_and_429_skips_the_ctx_call(self):
        clock = _FakeTime(T0)
        fake = _HeatClient
        fake.clock, fake.bump, fake.fills, fake.ctx_calls = clock, 700.0, {}, []
        fake.chs_by_wallet = {(W1, None): _chs(2_000_000, [_pos("BTC", 2, 50_000)])}
        with tempfile.TemporaryDirectory() as d, _Env(HL_HEATMAP="1"), mock.patch.object(extract, "InfoClient", fake), \
                mock.patch.object(extract, "time", clock):
            recon = extract.run([W1, W2], 2, Path(d), incremental=True, hip3=False, deadline_ts=T0 + 600, soft_wallets={W1, W2})
            self.assertEqual(recon["deferred"], [W2])
            coh = {r["wallet_address"]: r for r in _read(Path(d) / S.NS_HYPERCORE / "l4x_wallet_cohort_snap.csv")}
            self.assertEqual((coh[W1]["position_snapshot_present"], coh[W2]["position_snapshot_present"]), ("True", "False"))
            self.assertEqual((coh[W2]["perp_equity_usd"], coh[W2]["size_cohort"], coh[W2]["equity_source"]), ("", "", ""))
            self.assertEqual(fake.ctx_calls, [None, "xyz", "io", "mkts", "para"])
            # a core pull that ended in 429: the wallet is skipped by the run, the ctx call by the guard
            fake.ctx_calls = []

            class Limited(fake):
                def user_fills_by_time(self, wallet, start_ms, end_ms):
                    raise RuntimeError("Info API userFillsByTime failed after 5 tries: HTTP 429")
            with mock.patch.object(extract, "InfoClient", Limited):
                recon = extract.run([W1], 2, Path(d), incremental=True, hip3=False)
            self.assertTrue(recon["by_wallet"][W1]["skipped"])
            self.assertEqual(recon["heatmap"]["ctx_skipped"], "429 during the pull phase")
            self.assertEqual(fake.ctx_calls, [])
            self.assertIn("heatmap:ctx_skipped", ops.build_summary(recon, [W1], ops.utcnow(), ops.utcnow())["warnings"])

    def test_a_failing_heatmap_step_is_a_warning_never_a_failed_run(self):
        with tempfile.TemporaryDirectory() as d, mock.patch.object(heatmap, "fetch_market_ctx", side_effect=RuntimeError("boom")):
            recon = self._run(Path(d), [W1], clock=_FakeTime(T0), env={"HL_HEATMAP": "1"})
            self.assertIn("boom", recon["heatmap"]["error"])
            self.assertEqual(recon["by_wallet"][W1]["open_positions"], 1)             # the wallet itself is fine
            summary = ops.build_summary(recon, [W1], ops.utcnow(), ops.utcnow())
            self.assertIn("heatmap:failed", summary["warnings"])
            self.assertNotIn("heatmap", " ".join(summary["problems"]))


class LiftAndGuardTests(unittest.TestCase):
    """The three tables through the real lift (local SQLite catalog): replace_scope on a
    timestamp lead column, the separate guarded lift, pending_since and its repair."""

    def _stage(self, ns: Path, bucket_ms: int, run_ms: int, wallets, coins=("BTC",), equity=2e6):
        rid = heatmap.make_run_id("hourly", run_ms)
        now_iso = heatmap.iso(run_ms)
        pos, snaps = [], {}
        for w in wallets:
            chs = _chs(equity, [_pos(c, 1, 100) for c in coins])
            pos += heatmap.position_rows(chs, {}, w, bucket_ms=bucket_ms, fetched_at_ms=run_ms + 1000, run_id=rid, now_iso=now_iso)
            snaps[w] = {"equity": equity, "has_non_usd": False, "open_positions": len(coins)}
        coh = heatmap.cohort_rows(list(wallets), snaps, bucket_ms=bucket_ms, run_start_ms=run_ms, run_id=rid, now_iso=now_iso,
                                  size_b=SIZE_B, pnl_b=PNL_B)
        ctx = heatmap.market_ctx_rows(MarketCtxTests.META, MarketCtxTests.CTXS, "main", bucket_ms=bucket_ms, as_of_ms=run_ms,
                                      run_id=rid, now_iso=now_iso)
        return heatmap.stage(ns, pos, coh, ctx, bucket_iso=heatmap.iso(bucket_ms), scope_wallets=set(wallets))

    def test_lift_replace_scope_per_bucket_and_bounded_since(self):
        with tempfile.TemporaryDirectory() as d, _Env(HL_OUT_DIR=d, NESSIE_URI=None, HL_CATALOG_URI=None, WAREHOUSE=None):
            ns = Path(d) / S.NS_HYPERCORE
            b1 = BUCKET_MS
            self._stage(ns, b1, b1 + 600_000, [W1, W2], coins=("BTC", "ETH"))
            rep = il.lift_directory(ns, tables=list(il.HEATMAP_TABLES), local_dir=Path(d) / "cat")
            self.assertEqual({t: rep[t]["written"] for t in il.HEATMAP_TABLES},
                             {"l4x_perp_position_snap": 4, "l4x_wallet_cohort_snap": 2, "l4x_perp_market_ctx_snap": 2})
            self.assertEqual({t: rep["_watermarks"][t] for t in il.HEATMAP_TABLES}, {t: "written" for t in il.HEATMAP_TABLES})
            # same bucket, W1 only, one coin closed: W1's rows replaced, W2's kept; cohort = the new plan; ctx replaced
            self._stage(ns, b1, b1 + 1_200_000, [W1], coins=("BTC",))
            rep = il.lift_directory(ns, tables=list(il.HEATMAP_TABLES), local_dir=Path(d) / "cat")
            catalog, _ = il.load_catalog_from_env(local_dir=Path(d) / "cat")
            try:
                pos = catalog.load_table("hypercore.l4x_perp_position_snap").scan().to_arrow().to_pylist()
                self.assertEqual(sorted((r["wallet_address"], r["coin"]) for r in pos), [(W1, "BTC"), (W2, "BTC"), (W2, "ETH")])
                self.assertTrue(all(r["as_of_bucket"] == dt.datetime.fromtimestamp(b1 / 1000, tz=dt.timezone.utc) for r in pos))
                self.assertTrue(all(isinstance(r["as_of_date"], dt.date) for r in pos))
                coh = catalog.load_table("hypercore.l4x_wallet_cohort_snap").scan().to_arrow().to_pylist()
                self.assertEqual([r["wallet_address"] for r in coh], [W1])
                self.assertEqual((coh[0]["position_snapshot_present"], coh[0]["size_cohort"], coh[0]["perf_cohort"]),
                                 (True, "tidal_whale", "unknown"))
                ctx = catalog.load_table("hypercore.l4x_perp_market_ctx_snap").scan().to_arrow().to_pylist()
                self.assertEqual(len(ctx), 2)
                wm = catalog.load_table("control.table_watermarks").scan().to_arrow().to_pylist()
                row = next(r for r in wm if r["table_name"] == "l4x_wallet_cohort_snap")
                self.assertEqual((row["watermark_col"], row["status"], row["watermark_date"]),
                                 ("created_at", "completed", dt.date.fromisoformat(heatmap.date_of(b1))))
                # the next bucket: the bounded lift (since = previous run - 1h) picks only the new rows
                b2 = b1 + 3_600_000
                self._stage(ns, b2, b2 + 600_000, [W1, W2])
                rep = il.lift_directory(ns, catalog=catalog, tables=list(il.HEATMAP_TABLES),
                                        since_iso=heatmap.iso(b1 + 1_500_000))
                self.assertEqual(rep["l4x_perp_position_snap"]["incoming"], 2)
                pos = catalog.load_table("hypercore.l4x_perp_position_snap").scan().to_arrow().to_pylist()
                self.assertEqual(len(pos), 5)
                # and the maintenance retention drops partitions older than the cutoff, one commit per table
                today = dt.datetime.fromtimestamp(b2 / 1000, tz=dt.timezone.utc).date()
                rep = heatmap.drop_partitions(catalog, "hypercore", today=today + dt.timedelta(days=50),
                                              days={"l4x_perp_position_snap": 45, "l4x_wallet_cohort_snap": 0, "l4x_perp_market_ctx_snap": 90})
                self.assertEqual(rep["hypercore.l4x_perp_position_snap"]["commits"], 1)
                self.assertEqual(rep["hypercore.l4x_perp_position_snap"]["rows_dropped"], 5)
                self.assertIn("skipped", rep["hypercore.l4x_wallet_cohort_snap"])
                self.assertEqual(rep["hypercore.l4x_perp_market_ctx_snap"]["commits"], 0)
                self.assertEqual(catalog.load_table("hypercore.l4x_perp_position_snap").scan().to_arrow().num_rows, 0)
                self.assertEqual(catalog.load_table("hypercore.l4x_perp_market_ctx_snap").scan().to_arrow().num_rows, 4)
            finally:
                il.dispose_catalog(catalog)

    def test_skip_guard_records_pending_since_and_the_next_run_repairs_it(self):
        # extract.run stamps rows with time.time() (the fake clock) but pending_since with the
        # wall clock (run start iso): anchor the fake clock at the wall clock so the markers compare
        t_start = float(int(time.time()))
        clock = _FakeTime(t_start)
        fake = _HeatClient
        fake.clock, fake.bump, fake.fills, fake.ctx_calls = clock, 0.0, {}, []
        fake.chs_by_wallet = {(W1, None): _chs(2_000_000, [_pos("BTC", 2, 50_000)])}
        with tempfile.TemporaryDirectory() as d, _Env(HL_HEATMAP="1", HL_HEATMAP_LIFT_DEADLINE_S="2400", HL_OUT_DIR=d,
                                                        NESSIE_URI=None, HL_CATALOG_URI=None, WAREHOUSE=None, HL_ROLLUPS="0",
                                                        HL_PRICE_TIME_BUDGET_MIN="0"), \
                mock.patch.object(extract, "InfoClient", fake), mock.patch.object(extract, "time", clock):
            out = Path(d)
            ns = out / S.NS_HYPERCORE
            # run 1: the clock jumps past the guard before the lift phase -> heatmap lift skipped, main lift done
            real_lift = il.lift_directory
            calls: list = []

            def slow_then_lift(*a, **kw):
                calls.append(kw.get("tables"))
                if len(calls) == 1:
                    clock.t += 2_500.0
                return real_lift(*a, **kw)
            with mock.patch("blueblocx_hyperliquid.iceberg_load.lift_directory", side_effect=slow_then_lift):
                recon = extract.run([W1], 2, out, incremental=True, hip3=False, iceberg=True)
            heat = recon["heatmap"]
            self.assertTrue(heat["lift_skipped"])
            self.assertEqual(heat["pending_since"], recon["run_at"])                 # this run's start = the bucket's as_of_ts floor
            self.assertEqual(len(calls), 1)                                        # only the main lift ran
            self.assertNotIn("l4x_perp_position_snap", calls[0])                   # the main lift excludes the heatmap tables
            state = il.read_lift_state(ns)
            self.assertEqual(state["heatmap"]["pending_since"], recon["run_at"])
            self.assertIn("heatmap:lift_skipped", ops.build_summary(recon, [W1], ops.utcnow(), ops.utcnow())["warnings"])
            self.assertNotIn("l4x_perp_position_snap", recon["iceberg"])
            pending_1 = recon["run_at"]
            # run 2, two hours later (the main lift's 1h margin would have dropped bucket 1): the
            # separate heatmap lift reads from pending_since and lands BOTH buckets
            clock.t = t_start + 2 * 3600.0
            recon = extract.run([W1], 2, out, incremental=True, hip3=False, iceberg=True)
            heat = recon["heatmap"]
            self.assertNotIn("lift_skipped", heat)
            self.assertEqual(heat["repaired_since"], pending_1)
            self.assertEqual(heat["lift"]["l4x_perp_position_snap"]["written"], 2)
            self.assertEqual(heat["lift"]["l4x_wallet_cohort_snap"]["written"], 2)
            self.assertEqual(il.read_lift_state(ns)["heatmap"], {"last_lift_iso": recon["run_at"]})
            catalog, _ = il.load_catalog_from_env()
            try:
                pos = catalog.load_table("hypercore.l4x_perp_position_snap").scan().to_arrow().to_pylist()
                b1 = heatmap.bucket_start_ms(int(t_start * 1000)) / 1000
                self.assertEqual(sorted(r["as_of_bucket"].timestamp() for r in pos), [b1, b1 + 7200])
                wm = {r["table_name"]: r["status"] for r in catalog.load_table("control.table_watermarks").scan().to_arrow().to_pylist()}
                self.assertEqual({wm[t] for t in il.HEATMAP_TABLES}, {"completed"})
            finally:
                il.dispose_catalog(catalog)
            # run 3: a heatmap lift that raises is a warning and leaves a pending marker for the next run
            clock.t = t_start + 3 * 3600.0
            with mock.patch("blueblocx_hyperliquid.heatmap.lift_since_iso", side_effect=RuntimeError("nas gone")):
                recon = extract.run([W1], 2, out, incremental=True, hip3=False, iceberg=True)
            self.assertIn("nas gone", recon["heatmap"]["lift_error"])
            self.assertEqual(il.read_lift_state(ns)["heatmap"]["pending_since"], recon["run_at"])
            self.assertIn("nas gone", il.read_lift_state(ns)["heatmap"]["error"])
            summary = ops.build_summary(recon, [W1], ops.utcnow(), ops.utcnow())
            self.assertIn("heatmap:lift_failed", summary["warnings"])
            self.assertFalse(any("heatmap" in p for p in summary["problems"]))        # never a problem

    def test_lift_since_iso_and_skip_predicate(self):
        self.assertEqual(heatmap.lift_since_iso("2026-11-08T10:12:00+00:00", "2026-11-08T12:10:00+00:00", "2026-11-08T13:10:00+00:00"),
                         "2026-11-08T10:11:00+00:00")                                 # pending: a minute before its as_of_ts
        self.assertEqual(heatmap.lift_since_iso(None, "2026-11-08T12:10:00+00:00", "2026-11-08T13:10:00+00:00"),
                         "2026-11-08T11:10:00+00:00")                                 # no pending: the main lift's rule
        self.assertIsNone(heatmap.lift_since_iso(None, None, "2026-11-08T13:10:00+00:00"))
        self.assertTrue(heatmap.lift_skipped(0.0, 2401.0, 2400))
        self.assertFalse(heatmap.lift_skipped(0.0, 2399.0, 2400))
        self.assertFalse(heatmap.lift_skipped(0.0, 1e9, 0))                            # 0 = guard off
        self.assertEqual(heatmap.earlier_iso("2026-11-08T10:00:00+00:00", "2026-11-08T09:00:00+00:00"), "2026-11-08T09:00:00+00:00")
        self.assertEqual(heatmap.earlier_iso(None, "x"), "x")
        self.assertIsNone(heatmap.pending_since({}))
        self.assertEqual(heatmap.pending_since({"heatmap": {"pending_since": "t"}}), "t")

    def test_prune_staged_keeps_seven_days(self):
        with tempfile.TemporaryDirectory() as d:
            ns = Path(d) / S.NS_HYPERCORE
            old_b = BUCKET_MS - 10 * 86_400_000
            self._stage(ns, old_b, old_b + 1000, [W1])
            self._stage(ns, BUCKET_MS, NOW_MS, [W1])
            rep = heatmap.prune_staged(ns, NOW_MS)
            self.assertEqual({t: v["dropped"] for t, v in rep.items()},
                             {"l4x_perp_position_snap": 1, "l4x_wallet_cohort_snap": 1, "l4x_perp_market_ctx_snap": 2})
            self.assertEqual(len(_read(ns / "l4x_perp_position_snap.csv")), 1)


class SeedDailyTests(unittest.TestCase):
    def _l4x_row(self, day: str, wallet: str, coin: str, dex="main", szi=1.0, av=3e6, ts_h=23):
        ts = dt.datetime.fromisoformat(day + f"T{ts_h:02d}:12:00+00:00")
        return {"as_of_ts": ts, "as_of_date": dt.date.fromisoformat(day), "wallet_address": wallet, "wallet_group": S.addr_group(wallet),
                "coin": coin, "dex": dex, "szi": szi, "entry_px": 10.0, "position_value": abs(szi) * 10.0, "mark_px": 10.0,
                "unrealized_pnl_usd": 1.0, "return_on_equity": 0.1, "liquidation_px": None, "margin_used_usd": 2.0,
                "leverage_type": "cross", "leverage_value": 5.0, "cum_funding_all_time": 0.3, "cum_funding_since_open": 0.0,
                "account_value_usd": av, "withdrawable_usd": 1.0, "created_at": ts}

    def test_seed_rows_from_l4x_history(self):
        rows = [self._l4x_row("2026-10-13", W1, "BTC"), self._l4x_row("2026-10-13", W1, "ETH", szi=-2),
                self._l4x_row("2026-10-13", W1, "xyz:SP500", dex="xyz", av=5e5),
                self._l4x_row("2026-10-13", W2, "BTC", av=50_000), self._l4x_row("2026-10-14", W1, "BTC", av=6e6),
                self._l4x_row("2026-10-15", W1, "BTC"),                                           # >= until: excluded
                self._l4x_row("2026-10-14", W2, "BTC", szi=0.0)]                                     # flat: excluded
        pos, coh = heatmap.seed_daily_rows(rows, until=dt.date(2026, 10, 15), since=None, now_iso=NOW_ISO, run_id="l4x_daily-1",
                                           size_b=SIZE_B)
        self.assertEqual([(r["as_of_date"], r["wallet_address"], r["coin"]) for r in pos],
                         [("2026-10-13", W1, "BTC"), ("2026-10-13", W1, "ETH"), ("2026-10-13", W1, "xyz:SP500"),
                          ("2026-10-13", W2, "BTC"), ("2026-10-14", W1, "BTC")])
        for r in pos:
            self.assertEqual(set(r), set(S.L4X_PERP_POSITION_SNAP_COLUMNS))
            self.assertEqual((r["bucket_minutes"], r["source"], r["run_id"], r["created_at"]), (1440, "l4x_daily", "l4x_daily-1", NOW_ISO))
            self.assertTrue(r["as_of_bucket"].endswith("T00:00:00+00:00"))
            self.assertEqual(r["as_of_bucket"][:10], r["as_of_date"])
            self.assertEqual(r["as_of_ts"], f"{r['as_of_date']}T23:12:00+00:00")                 # the original snapshot instant
            self.assertIsNone(r["max_leverage"])
            self.assertIsNone(r["cum_funding_since_change"])
        self.assertEqual([r["wallet_perp_equity_usd"] for r in pos], [3.5e6, 3.5e6, 3.5e6, 50_000.0, 6e6])   # DISTINCT (dex, av)
        self.assertEqual([(r["side"], r["szi"]) for r in pos[:2]], [("long", 1.0), ("short", -2.0)])
        self.assertEqual([(r["as_of_date"], r["wallet_address"], r["size_cohort"], r["size_cohort_fine"]) for r in coh],
                         [("2026-10-13", W1, "tidal_whale", 6), ("2026-10-13", W2, "under_100k", 3), ("2026-10-14", W1, "leviathan", 7)])
        for r in coh:
            self.assertEqual(set(r), set(S.L4X_WALLET_COHORT_SNAP_COLUMNS))
            self.assertEqual((r["position_snapshot_present"], r["equity_source"], r["perf_cohort"], r["perf_cohort_fine"]),
                             (True, "snapshot", "unknown", None))
            self.assertIsNone(r["is_tracked_whale"])
            self.assertEqual((r["is_active"], r["has_open_position"], r["bucket_minutes"], r["source"]), (True, True, 1440, "l4x_daily"))
        pos2, _ = heatmap.seed_daily_rows(rows, until=dt.date(2026, 10, 15), since=dt.date(2026, 10, 14), now_iso=NOW_ISO,
                                          run_id="x", size_b=SIZE_B)
        self.assertEqual([r["as_of_date"] for r in pos2], ["2026-10-14"])

    def test_seed_daily_commits_directly_and_refuses_to_overlap_the_hourly_series(self):
        with tempfile.TemporaryDirectory() as d, _Env(HL_OUT_DIR=d, NESSIE_URI=None, HL_CATALOG_URI=None, WAREHOUSE=None):
            catalog, _ = il.load_catalog_from_env()
            try:
                ddl = il.parse_ddl()
                spec = ddl["l4x_perp_position_snapshot"]
                l4x = il.ensure_table(catalog, "hypercore", "l4x_perp_position_snapshot", spec)
                rows = [self._l4x_row("2026-10-13", W1, "BTC"), self._l4x_row("2026-10-14", W1, "BTC", av=6e6),
                        self._l4x_row("2026-10-14", W2, "ETH", av=2e5), self._l4x_row("2026-10-15", W1, "BTC")]
                l4x.append(il.conform(il.rows_to_arrow(rows, spec["columns"]), l4x))
                # no hourly series yet: --until is required
                with self.assertRaises(ValueError):
                    heatmap.seed_daily(catalog, "hypercore")
                rep = heatmap.seed_daily(catalog, "hypercore", until=dt.date(2026, 10, 15), dry_run=True)
                self.assertEqual((rep["days"], rep["wallets"], rep["dry_run"], rep["l4x_perp_position_snap"]["incoming"]), (2, 2, True, 3))
                self.assertNotIn("l4x_perp_position_snap", {t[-1] for t in catalog.list_tables("hypercore")})
                rep = heatmap.seed_daily(catalog, "hypercore", until=dt.date(2026, 10, 15), now=dt.datetime(2026, 11, 9, tzinfo=dt.timezone.utc))
                self.assertEqual((rep["l4x_perp_position_snap"]["written"], rep["l4x_wallet_cohort_snap"]["written"]), (3, 3))
                self.assertEqual(rep["watermarks"], {"l4x_perp_position_snap": "written", "l4x_wallet_cohort_snap": "written"})
                self.assertEqual(rep["run_id"], "l4x_daily-%d" % int(dt.datetime(2026, 11, 9, tzinfo=dt.timezone.utc).timestamp() * 1000))
                pos = catalog.load_table("hypercore.l4x_perp_position_snap").scan().to_arrow().to_pylist()
                self.assertEqual(sorted((r["as_of_date"].isoformat(), r["wallet_address"], r["bucket_minutes"], r["source"]) for r in pos),
                                 [("2026-10-13", W1, 1440, "l4x_daily"), ("2026-10-14", W1, 1440, "l4x_daily"), ("2026-10-14", W2, 1440, "l4x_daily")])
                # idempotent: a second run replaces, never duplicates
                heatmap.seed_daily(catalog, "hypercore", until=dt.date(2026, 10, 15))
                self.assertEqual(catalog.load_table("hypercore.l4x_perp_position_snap").scan().to_arrow().num_rows, 3)
                # an hourly bucket on 2026-10-15 exists: the default --until stops there and a later one is refused
                hourly_ms = int(dt.datetime(2026, 10, 15, 10, tzinfo=dt.timezone.utc).timestamp() * 1000)
                hrows = heatmap.position_rows(_chs(1e6, [_pos("BTC", 1, 10)]), {}, W1, bucket_ms=hourly_ms, fetched_at_ms=hourly_ms,
                                              run_id="hourly-1", now_iso=heatmap.iso(hourly_ms))
                t = catalog.load_table("hypercore.l4x_perp_position_snap")
                t.append(il.conform(il.rows_to_arrow(hrows, ddl["l4x_perp_position_snap"]["columns"]), t))
                self.assertEqual(heatmap.first_hourly_date(catalog, "hypercore"), dt.date(2026, 10, 15))
                rep = heatmap.seed_daily(catalog, "hypercore", dry_run=True)
                self.assertEqual(rep["until"], "2026-10-15")
                with self.assertRaises(ValueError):
                    heatmap.seed_daily(catalog, "hypercore", until=dt.date(2026, 10, 16), dry_run=True)
            finally:
                il.dispose_catalog(catalog)

    def test_cli_needs_the_flag_and_takes_the_lock(self):
        with tempfile.TemporaryDirectory() as d, _Env(HL_OUT_DIR=d, NESSIE_URI=None, HL_CATALOG_URI=None, WAREHOUSE=None):
            self.assertEqual(heatmap.main(["--out", d]), 2)
            self.assertEqual(heatmap.main(["--seed-daily", "--until", "not-a-date", "--out", d]), 2)
            ns = Path(d) / S.NS_HYPERCORE
            ns.mkdir(parents=True)
            with ops.RunLock(ns / ".daily.lock"):
                self.assertEqual(heatmap.main(["--seed-daily", "--until", "2026-10-15", "--out", d]), 3)


class MaintenanceCliTests(unittest.TestCase):
    def test_drop_partitions_flag_is_wired_and_skips_absent_tables(self):
        with tempfile.TemporaryDirectory() as d, _Env(HL_OUT_DIR=d, NESSIE_URI=None, HL_CATALOG_URI=None, WAREHOUSE=None), \
                mock.patch("sys.stdout"):
            rc = maintenance.main(["--drop-partitions", "--out", d])
            self.assertEqual(rc, 0)
        unit = (Path(__file__).resolve().parent.parent / "deploy" / "hypercore-maintenance.service").read_text(encoding="utf-8")
        self.assertIn("--drop-partitions", unit)


if __name__ == "__main__":
    unittest.main()
