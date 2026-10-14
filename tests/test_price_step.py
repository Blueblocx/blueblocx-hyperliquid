"""v1.7.0: native 1h/1d candle tables (price_step) - closed-bucket grid, first visit, gap
fetch, 1d once per UTC day, two-tier rotation under the budget, API errors, stable pins,
no perp rows - and the wiring into extract.run (phase 0, early lift, summary warnings).
No network."""
from __future__ import annotations

import csv
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from blueblocx_hyperliquid import extract, iceberg_load as il, ops, price_step, prices
from blueblocx_hyperliquid import schema as S
from blueblocx_hyperliquid.spot_meta import SpotMetaResolver
from tests.test_tracked import W1, _FakeClient, _FakeTime

H, D = price_step.HOUR_MS, price_step.DAY_MS
X = 1_760_000_000_000 - (1_760_000_000_000 % H)          # an hour boundary (2025-10-09T08:00:00Z)
NOW = X + 10 * 60_000                                    # :10 past, like the hourly timer
NOW_ISO = "2026-10-12T01:00:00+00:00"
PURR, HYPE, DEAD = "hlcore:token:1", "hlcore:token:2", "hlcore:token:3"
SPOT_META = {"tokens": [{"index": 0, "name": "USDC", "weiDecimals": 8}, {"index": 1, "name": "PURR", "weiDecimals": 5},
                        {"index": 2, "name": "HYPE", "weiDecimals": 8}, {"index": 3, "name": "DEAD", "weiDecimals": 0}],
             "universe": [{"index": 0, "name": "PURR/USDC", "tokens": [1, 0]},
                          {"index": 1, "name": "HYPE/USDC", "tokens": [2, 0]}]}
META = {S.USDC_ADDRESS: {"symbol": "USDC", "is_stablecoin": True}, PURR: {"symbol": "PURR", "is_stablecoin": False},
        HYPE: {"symbol": "HYPE", "is_stablecoin": False}, DEAD: {"symbol": "DEAD", "is_stablecoin": False}}


class _Clock:
    def __init__(self, t: float):
        self.t = t

    def time(self) -> float:
        return self.t


class _CandleClient:
    """candleSnapshot stand-in: one candle per aligned bucket from start_ms to the bucket
    that contains end_ms (the in-progress one included, as the API does). Records calls;
    coins in `fail` raise; every call advances `clock` by `cost_s`."""

    def __init__(self, clock=None, cost_s: float = 0.0):
        self.calls: list[tuple] = []
        self.fail: set[str] = set()
        self.clock, self.cost_s = clock, cost_s

    def candle_snapshot(self, coin, interval, start_ms, end_ms, est_items=None):
        self.calls.append((coin, interval, start_ms, end_ms, est_items))
        if self.clock is not None:
            self.clock.t += self.cost_s
        if coin in self.fail:
            raise RuntimeError("boom")
        step = H if interval == "1h" else D
        t = -(-start_ms // step) * step
        out = []
        while t <= end_ms:
            out.append({"t": t, "T": t + step - 1, "o": "1", "h": "2", "l": "0.5", "c": "1.5", "v": "10", "n": 3})
            t += step
        return out


def _read(path: Path) -> list[dict]:
    with path.open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


class GridTests(unittest.TestCase):
    def test_interval_grid_is_aligned_and_closed_only(self):
        self.assertEqual(price_step.interval_grid(NOW - 3 * H, NOW, H), [X - 2 * H, X - H])
        self.assertEqual(price_step.interval_grid(NOW - 30 * 60_000, NOW, H), [])        # nothing closed yet
        self.assertEqual(price_step.interval_grid(X - H, X, H), [X - H])                  # closes exactly at end
        self.assertEqual(price_step.interval_grid(X, X - H, H), [])


class PriceStepTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.ns_dir = Path(self._tmp.name) / "hypercore"
        self.ns_dir.mkdir()
        self.resolver = SpotMetaResolver(SPOT_META)

    def tearDown(self):
        self._tmp.cleanup()

    def _run(self, client, now_ms, budget_s=60.0, history_days=2, clock=None):
        return price_step.run(client, self.resolver, self.ns_dir, now_ms, NOW_ISO, budget_s=budget_s,
                              history_days=history_days, clock=clock or _Clock(1000.0), token_meta=META)

    def test_usdc_pairs(self):
        pairs = self.resolver.usdc_pairs()
        self.assertEqual(set(pairs), {PURR, HYPE})
        self.assertEqual(pairs[PURR].candle_coin, "@0")

    def test_first_visit_fetches_history_and_pins_stables_closed_candles_only(self):
        c = _CandleClient()
        rep = self._run(c, NOW)
        grid_h = price_step.interval_grid(((NOW - 2 * D) // H) * H, NOW, H)     # whole buckets from the grid
        grid_d = price_step.interval_grid(((NOW - 2 * D) // D) * D, NOW, D)
        self.assertEqual((rep["tokens"], rep["visited"], rep["deferred"], rep["history_pending"]), (2, 2, 0, 0))
        self.assertEqual((rep["fetched_1h"], rep["fetched_1d"]), (2, 2))
        self.assertEqual(rep["rows_1h"], 3 * len(grid_h))                   # PURR, HYPE + the USDC pin
        self.assertEqual(rep["rows_1d"], 3 * len(grid_d))
        self.assertEqual((rep["unpriceable"], rep["stable_pins"], rep["errors"]), (["DEAD"], [S.USDC_ADDRESS], {}))
        rows = _read(self.ns_dir / "l3_token_price_usd_1h.csv")
        self.assertEqual({r["token_address"] for r in rows}, {PURR, HYPE, S.USDC_ADDRESS})
        self.assertFalse(any(r["token_address"].startswith("hlcore:perp") for r in rows))
        stamps = sorted(r["bucket_minute"] for r in rows)
        self.assertEqual(stamps[-1], prices._iso(X - H))                    # the last CLOSED hour, never the open one
        self.assertTrue(all(s.endswith(":00:00+00:00") for s in stamps))    # aligned opens
        pin = next(r for r in rows if r["token_address"] == S.USDC_ADDRESS)
        self.assertEqual((pin["close_price_usd"], pin["num_trades"]), ("1.0", "0"))
        st = price_step.load_state(self.ns_dir)
        self.assertEqual(st["tokens"][PURR]["last_1h_ms"], X - H)
        self.assertEqual(st["tokens"][PURR]["last_1d_ms"], grid_d[-1])
        self.assertEqual(st["pins"][S.USDC_ADDRESS]["last_1h_ms"], X - H)
        first_1h = next(k for k in c.calls if k[1] == "1h")
        self.assertEqual((first_1h[2], first_1h[3]), (((NOW - 2 * D) // H) * H, NOW))
        self.assertLessEqual(first_1h[4], 60)                                # a sized weight estimate, not the 5000 cap

    def test_second_run_fetches_only_the_gap_and_1d_once_per_utc_day(self):
        c = _CandleClient()
        self._run(c, NOW)
        st1 = price_step.load_state(self.ns_dir)
        c.calls.clear()
        rep = self._run(c, NOW + H)
        self.assertEqual([k[1] for k in c.calls], ["1h", "1h"])             # the day has not closed: no 1d call
        self.assertEqual(c.calls[0][2], X)                                   # start = last closed + 1h
        self.assertEqual((rep["rows_1h"], rep["rows_1d"], rep["fetched_1d"]), (3, 0, 0))
        c.calls.clear()
        rep = self._run(c, NOW + H)                                          # same instant again: nothing to do
        self.assertEqual((len(c.calls), rep["rows_1h"], rep["rows_1d"]), (0, 0, 0))
        nxt = ((NOW // D) + 1) * D + 10 * 60_000                             # next UTC day, :10
        c.calls.clear()
        rep = self._run(c, nxt)
        daily = [k for k in c.calls if k[1] == "1d"]
        self.assertEqual(len(daily), 2)
        self.assertEqual(daily[0][2], st1["tokens"][PURR]["last_1d_ms"] + D)
        self.assertEqual(rep["rows_1d"], 3)                                  # one closed day per token + the pin

    def test_budget_defers_cold_tokens_and_serves_the_hot_tier_first(self):
        price_step.record_seen(self.ns_dir, {HYPE}, NOW)                     # HYPE: referenced by the ledger
        clock = _Clock(0.0)
        c = _CandleClient(clock=clock, cost_s=100.0)
        rep = self._run(c, NOW, budget_s=150.0, clock=clock)                 # HYPE 1h (100 s) + 1d (200 s) > budget
        self.assertEqual((rep["hot"], rep["visited"], rep["deferred"], rep["history_pending"]), (1, 1, 1, 1))
        self.assertEqual(c.calls[0][0], "@1")                                # HYPE's pair first
        st = price_step.load_state(self.ns_dir)
        self.assertIn("last_1h_ms", st["tokens"][HYPE])
        self.assertNotIn(PURR, st["tokens"])
        self.assertGreater(rep["rows_1h"], 0)                                # pins never wait for the budget
        order, n_hot = price_step.plan_order({PURR: None, HYPE: None}, st, NOW)
        self.assertEqual((order, n_hot), ([HYPE, PURR], 1))
        # within a tier: never visited before the most stale
        st2 = {"tokens": {PURR: {"last_1h_ms": 5}, HYPE: {}}, "seen": {}, "pins": {}}
        self.assertEqual(price_step.plan_order({PURR: None, HYPE: None}, st2, NOW)[0], [HYPE, PURR])
        st3 = {"tokens": {PURR: {"last_1h_ms": 5}, HYPE: {"last_1h_ms": 1}}, "seen": {}, "pins": {}}
        self.assertEqual(price_step.plan_order({PURR: None, HYPE: None}, st3, NOW)[0], [HYPE, PURR])

    def test_api_error_keeps_the_cursor_and_is_reported(self):
        c = _CandleClient()
        c.fail = {"@0"}                                                      # PURR's pair
        rep = self._run(c, NOW)
        self.assertIn(PURR, rep["errors"])
        self.assertEqual(rep["history_pending"], 1)
        st = price_step.load_state(self.ns_dir)
        self.assertNotIn("last_1h_ms", st["tokens"][PURR])
        self.assertIn("last_1h_ms", st["tokens"][HYPE])
        c.fail = set()
        rep = self._run(c, NOW)                                              # retried at the same instant: full history
        self.assertEqual((rep["history_pending"], rep["errors"]), (0, {}))

    def test_empty_answer_advances_the_cursor(self):
        class Empty(_CandleClient):
            def candle_snapshot(self, *a, **k):
                self.calls.append(a)
                return []
        c = Empty()
        rep = self._run(c, NOW)
        st = price_step.load_state(self.ns_dir)
        self.assertEqual(st["tokens"][PURR]["last_1h_ms"], X - H)           # no trades = no candle; not retried forever
        self.assertEqual(rep["rows_1h"], len(price_step.interval_grid(((NOW - 2 * D) // H) * H, NOW, H)))   # pins only

    def test_state_file_round_trip_and_corruption(self):
        (self.ns_dir / price_step.STATE_FILE).write_text("{not json", encoding="utf-8")
        self.assertEqual(price_step.load_state(self.ns_dir), {"tokens": {}, "pins": {}, "seen": {}})
        price_step.record_seen(self.ns_dir, {PURR, ""}, 5)
        price_step.record_seen(self.ns_dir, {PURR}, 3)                       # never moves backwards
        self.assertEqual(price_step.load_state(self.ns_dir)["seen"], {PURR: 5})


class ExtractWiringTests(unittest.TestCase):
    FILL = {"coin": "BTC", "px": "100", "sz": "1", "side": "B", "startPosition": "0", "dir": "Close Long",
            "closedPnl": "1.5", "hash": "0x" + "ab" * 32, "oid": 1, "crossed": True, "fee": "0.1",
            "feeToken": "USDC", "twapId": None, "tid": 1, "time": NOW - H}

    def _sqlite_env(self, root: Path) -> dict:
        return {"NESSIE_URI": "", "HL_CATALOG_URI": f"sqlite:///{(root / 'c.db').as_posix()}",
                "WAREHOUSE": f"file://{(root / 'wh').as_posix()}", "HL_WAREHOUSE_MOUNT": "",
                "HL_PRICE_HISTORY_DAYS": "2"}

    def test_prices_run_first_are_lifted_early_and_feed_the_summary(self):
        clock = _FakeTime(NOW / 1000)

        class Client(_FakeClient):
            def spot_meta_and_asset_ctxs(self):
                return SPOT_META, []

            def candle_snapshot(self, coin, interval, start_ms, end_ms, est_items=None):
                return _CandleClient().candle_snapshot(coin, interval, start_ms, end_ms, est_items)
        Client.clock, Client.bump, Client.fills = clock, 0.0, {W1: [dict(self.FILL)]}
        phases: list[str] = []
        with tempfile.TemporaryDirectory() as d, mock.patch.object(extract, "InfoClient", Client), \
                mock.patch.object(extract, "time", clock):
            root = Path(d)
            out = root / "out"
            ns = out / S.NS_HYPERCORE
            ns.mkdir(parents=True)
            # PURR is already in the staged token metadata: it is the price universe (HYPE is not)
            meta_rows = prices.token_metadata_rows({PURR: {"symbol": "PURR", "decimals": 5, "is_stablecoin": False}}, NOW_ISO)
            with (ns / "l1_token_metadata.csv").open("w", newline="", encoding="utf-8") as fh:
                w = csv.DictWriter(fh, fieldnames=S.L1_TOKEN_METADATA_COLUMNS)
                w.writeheader()
                w.writerows([{c: ("" if r.get(c) is None else r[c]) for c in S.L1_TOKEN_METADATA_COLUMNS} for r in meta_rows])
            with mock.patch.dict(os.environ, self._sqlite_env(root)):
                recon = extract.run([W1], 2, out, incremental=True, hip3=False, soft_wallets={W1}, iceberg=True,
                                    price_budget_min=1.0, progress=lambda **kw: phases.append(kw.get("phase")))
                p = recon["prices"]
                self.assertEqual((p["tokens"], p["history_pending"], p["deferred"], p["errors"]), (1, 0, 0, {}))
                self.assertEqual(p["early_lift"]["l3_token_price_usd_1h"]["written"], p["rows_1h"])
                self.assertEqual(p["early_lift"]["l3_token_price_usd_1d"]["written"], p["rows_1d"])
                self.assertLess(phases.index("prices"), phases.index("pull"))         # prices before any wallet
                self.assertIn(S.USDC_ADDRESS, price_step.load_state(ns)["seen"])      # the fill's cash token
                # the main lift re-checked the price rows (already there) and wrote their watermarks
                self.assertEqual(recon["iceberg"]["l3_token_price_usd_1h"]["skipped_existing"], p["rows_1h"])
                self.assertEqual(recon["iceberg"]["l3_token_price_usd_1h"]["written"], 0)
                catalog, _ = il.load_catalog_from_env()
                try:
                    self.assertEqual(il._table_row_count(catalog.load_table("hypercore.l3_token_price_usd_1h")), p["rows_1h"])
                    wm = {r["table_name"] for r in catalog.load_table("control.table_watermarks").scan().to_arrow().to_pylist()}
                    self.assertTrue({"l3_token_price_usd_1h", "l3_token_price_usd_1d"} <= wm)
                finally:
                    il.dispose_catalog(catalog)
            summary = ops.build_summary(recon, [W1], ops.utcnow(), ops.utcnow(), soft_wallets={W1})
            self.assertEqual(summary["prices"]["history_pending"], 0)
            self.assertFalse([w for w in summary["warnings"] if w.startswith("prices")])
            # budget 0: the step is off, nothing is written
            recon = extract.run([W1], 2, root / "out2", hip3=False, soft_wallets={W1}, price_budget_min=0)
            self.assertIsNone(recon["prices"])
            self.assertFalse((root / "out2" / S.NS_HYPERCORE / "l3_token_price_usd_1h.csv").exists())

    def test_summary_warnings_for_the_price_step(self):
        now = ops.utcnow()
        recon = {"natural_key_ok": True, "by_wallet": {}, "wallets": [], "deferred": [],
                 "prices": {"deferred": 3, "history_pending": 2, "errors": {"a": "x"}, "early_lift": {"error": "e"}}}
        s = ops.build_summary(recon, [], now, now)
        self.assertEqual([w for w in s["warnings"] if w.startswith("prices")],
                         ["prices_deferred=3", "prices_history_pending=2", "prices_errors=1", "prices:early_lift_failed"])
        self.assertTrue(s["ok"])                                                      # warnings, never problems
        s = ops.build_summary(dict(recon, prices={"error": "boom"}), [], now, now)
        self.assertEqual(s["warnings"], ["prices:skipped"])
        self.assertIsNone(ops.build_summary(dict(recon, prices=None), [], now, now)["prices"])


if __name__ == "__main__":
    unittest.main()
