"""Rollup engines against the atlas formulas (docs/ATLAS_WALLET_TABLE_FORMULAS.md):
pure functions over small synthetic ledgers, no catalog, no network."""
from __future__ import annotations

import datetime as dt
import math
import statistics
import unittest

import pyarrow as pa

from blueblocx_hyperliquid import schema as S
from blueblocx_hyperliquid.rollups import PLAN, REQUIRED, Budget, balances, categories, fifo, market, quality, signals, tokens
from blueblocx_hyperliquid.rollups.common import asof, ddl_columns, midrank_percentile, table_from_rows

UTC = dt.timezone.utc
W = "0xwallet"
T = "hlcore:token:1"


def ts(day: int, hour: int = 0) -> dt.datetime:
    return dt.datetime(2026, 10, day, hour, tzinfo=UTC)


def row(day, hour, signed, *, idx=0, direction=None, cp="0xother", tx=None, token=T, amount=None):
    return {"wallet_address": W, "token_address": token, "counterparty_address": cp,
            "block_timestamp": ts(day, hour), "block_date": dt.date(2026, 10, day),
            "tx_hash": tx or f"tx{day}{hour}{idx}", "event_index": idx,
            "direction": direction or ("in" if signed > 0 else "out"),
            "amount_raw": abs(signed) * 1e6, "amount": amount if amount is not None else abs(signed),
            "amount_signed": signed}


class BalanceTests(unittest.TestCase):
    def test_running_balance_clamps_and_orders(self):
        rows = [row(2, 1, -5.0), row(1, 1, 10.0), row(1, 2, -12.0), row(3, 0, 4.0)]
        out, offset = balances.running_balances(rows)
        self.assertEqual(offset, 0.0)
        self.assertEqual([r["balance_amount"] for r in out], [10.0, 0.0, 0.0, 4.0])   # clamp discards the debt
        self.assertEqual([r["block_timestamp"] for r in out], sorted(r["block_timestamp"] for r in out))

    def test_self_transfer_rule(self):
        rows = [row(1, 1, 10.0), row(1, 2, 3.0, cp=W, direction="in", idx=1), row(1, 2, -3.0, cp=W, direction="out", idx=0)]
        out, _ = balances.running_balances(rows)
        self.assertEqual(len(out), 2)                                  # the self "in" leg is dropped
        self.assertEqual(out[-1]["amount_signed"], 0.0)                 # the "out" leg carries nothing
        self.assertEqual(out[-1]["balance_amount"], 10.0)

    def test_anchor_pins_the_running_sum_to_the_exchange_balance(self):
        # the lake saw +10 then -4; the exchange says the wallet held 100 right after the -4
        rows = [row(1, 1, 10.0), row(1, 5, -4.0), row(3, 0, 2.0)]
        out, offset = balances.running_balances(rows, anchor=(ts(2), 100.0))
        self.assertAlmostEqual(offset, 94.0)                            # 100 - (10 - 4)
        self.assertEqual([r["balance_amount"] for r in out], [104.0, 100.0, 102.0])
        # an anchor BELOW the movements clamps at zero like the atlas
        out, offset = balances.running_balances(rows, anchor=(ts(2), 1.0))
        self.assertAlmostEqual(offset, -5.0)
        self.assertEqual([r["balance_amount"] for r in out], [5.0, 1.0, 3.0])

    def test_anchor_survives_a_dip_below_zero(self):
        # pre-history the lake never saw (a negative offset): +100 and -100 before the
        # snapshot, the exchange says 10 at the snapshot, +100 after it. The series must
        # end at 110 = exchange balance + later movements; clamping the STATE at the dip
        # (v1.9.2 and before) ended at 200, the symptom of 2026-10-21 (21 of 126 pairs
        # above the exchange balance)
        rows = [row(1, 1, 100.0), row(1, 1, -100.0, idx=1), row(3, 0, 100.0)]
        out, offset = balances.running_balances(rows, anchor=(ts(2), 10.0))
        self.assertAlmostEqual(offset, 10.0)
        self.assertEqual([r["balance_amount"] for r in out], [110.0, 10.0, 110.0])
        rows = [row(1, 1, 100.0), row(1, 2, -150.0), row(3, 0, 100.0)]
        out, offset = balances.running_balances(rows, anchor=(ts(2), 10.0))
        self.assertAlmostEqual(offset, 60.0)
        self.assertEqual([r["balance_amount"] for r in out], [160.0, 10.0, 110.0])
        # the shown value is still never negative (a borrowed USDC balance pins below zero)
        out, offset = balances.running_balances(rows, anchor=(ts(2), -40.0))
        self.assertEqual([r["balance_amount"] for r in out], [110.0, 0.0, 60.0])

    def test_dense_quiet_holding_from_snapshot(self):
        dense = balances.dense_days([], dt.date(2026, 10, 4), quiet_pair=(W, T, dt.date(2026, 10, 2), 7.0))
        self.assertEqual([r["snapshot_date"] for r in dense], [dt.date(2026, 10, d) for d in (2, 3, 4)])
        self.assertTrue(all(r["closing_balance_amount"] == 7.0 and r["in_amount"] == 0.0 for r in dense))
        self.assertEqual(balances.dense_days([], dt.date(2026, 10, 4), quiet_pair=(W, T, dt.date(2026, 10, 2), 0.0)), [])

    def test_dense_days_carry_forward_and_stop_when_flat(self):
        per_tx, _ = balances.running_balances([row(1, 1, 10.0), row(1, 5, -4.0), row(4, 0, -6.0)])
        dense = balances.dense_days(per_tx, dt.date(2026, 10, 8))
        by_day = {r["snapshot_date"]: r for r in dense}
        self.assertEqual(by_day[dt.date(2026, 10, 1)]["opening_balance_amount"], 0.0)
        self.assertEqual(by_day[dt.date(2026, 10, 1)]["closing_balance_amount"], 6.0)
        self.assertEqual(by_day[dt.date(2026, 10, 1)]["in_amount"], 10.0)
        self.assertEqual(by_day[dt.date(2026, 10, 1)]["out_amount"], 4.0)
        self.assertEqual(by_day[dt.date(2026, 10, 1)]["net_flow_amount"], 6.0)
        self.assertEqual(by_day[dt.date(2026, 10, 2)]["closing_balance_amount"], 6.0)      # carried
        self.assertEqual(by_day[dt.date(2026, 10, 2)]["closing_block_timestamp"], ts(1, 5))
        self.assertEqual(by_day[dt.date(2026, 10, 4)]["closing_balance_amount"], 0.0)
        self.assertNotIn(dt.date(2026, 10, 5), by_day)                 # flat + quiet: not carried

    def test_dense_table_matches_ddl(self):
        per_tx, _ = balances.running_balances([row(1, 1, 1.0)])
        t = table_from_rows(balances.dense_days(per_tx, dt.date(2026, 10, 2)), ddl_columns("l4_wallet_balance_1d_dense"))
        self.assertEqual(t.num_rows, 2)
        self.assertEqual(t.schema.field("snapshot_date").type, pa.date32())


class FifoTests(unittest.TestCase):
    def priced(self, rows, prices=None, cutoff=dt.date(2026, 10, 30)):
        prices = prices or {T: [(ts(1), 2.0), (ts(2), 4.0), (ts(3), 1.0)]}
        return fifo.price_rows(rows, prices, {"hlcore:usdc"}, cutoff)

    def test_fifo_match_partial_and_state(self):
        pr = self.priced([row(1, 1, 10.0), row(2, 1, 5.0), row(3, 1, -12.0)])
        state, sells = fifo.fifo_pair(pr)
        self.assertEqual(len(sells), 1)
        s = sells[0]
        self.assertAlmostEqual(s["matched_qty"], 12.0)
        self.assertAlmostEqual(s["cost_usd"], 10 * 2.0 + 2 * 4.0)
        self.assertAlmostEqual(s["proceeds_usd"], 12 * 1.0)
        self.assertAlmostEqual(s["realized_pnl_usd"], 12.0 - 28.0)
        self.assertAlmostEqual(state["open_qty"], 3.0)
        self.assertAlmostEqual(state["open_cost_usd"], 3 * 4.0)
        self.assertEqual(state["last_event_index"], 0)

    def test_sell_without_lots_realizes_nothing(self):
        pr = self.priced([row(1, 1, -5.0), row(2, 1, 1.0)])
        state, sells = fifo.fifo_pair(pr)
        self.assertEqual(sells, [])
        self.assertAlmostEqual(state["open_qty"], 1.0)

    def test_unpriced_zero_and_today_rows_skipped(self):
        rows = [row(1, 1, 10.0), row(1, 2, 0.0, idx=1), row(1, 3, 2.0, idx=2, token="hlcore:token:99"),
                row(29, 0, 1.0), row(30, 0, 1.0)]
        pr = self.priced(rows, cutoff=dt.date(2026, 10, 30))
        self.assertEqual([r["tx_hash"] for r in pr], ["tx110", "tx2900"])
        self.assertEqual(pr[0]["price_usd"], 2.0)

    def test_stables_priced_at_one(self):
        pr = self.priced([row(1, 1, 7.0, token="hlcore:usdc")], prices={})
        self.assertEqual(pr[0]["price_usd"], 1.0)

    def test_dedupe_natural_key(self):
        a = row(1, 1, 1.0)
        pr = self.priced([a, dict(a)])
        self.assertEqual(len(pr), 1)

    def test_opening_lot_backs_a_pre_history_sale(self):
        # exchange balance showed 20 before our first row; selling 15 at 4.0 realizes against
        # the opening lot priced at the first instant (2.0)
        pr = self.priced([row(2, 1, -15.0)])
        opening = {"wallet_address": W, "token_address": T, "counterparty_address": None,
                   "block_timestamp": ts(2, 1) - dt.timedelta(microseconds=1), "block_date": dt.date(2026, 10, 2),
                   "tx_hash": "opening", "event_index": 0, "direction": "in", "amount_raw": None,
                   "amount": 20.0, "amount_signed": 20.0, "price_usd": 2.0}
        state, sells = fifo.fifo_pair([opening] + pr)
        self.assertAlmostEqual(sells[0]["realized_pnl_usd"], 15 * 4.0 - 15 * 2.0)
        self.assertAlmostEqual(state["open_qty"], 5.0)


class CommonTests(unittest.TestCase):
    def test_asof(self):
        s = [(1, "a"), (3, "b"), (5, "c")]
        self.assertEqual(asof(s, 0), None)
        self.assertEqual(asof(s, 1), "a")
        self.assertEqual(asof(s, 4), "b")
        self.assertEqual(asof(s, 9), "c")

    def test_plan_keys_are_required_and_in_ddl(self):
        for name, p in PLAN.items():
            cols = {c for c, _ in ddl_columns(name)}
            for k in p["key"]:
                self.assertIn(k, cols, f"{name}.{k}")
            self.assertTrue(set(REQUIRED[name]) <= cols | {"updated_at"}, name)
            if p["date"]:
                self.assertIn(p["date"], cols, name)

    def test_budget(self):
        b = Budget(0)
        self.assertFalse(b.exhausted())
        b = Budget(1e-9, started=0.0)
        self.assertTrue(b.exhausted())


class CategoryTests(unittest.TestCase):
    def test_rules(self):
        self.assertEqual(categories.categories_for("USDC", True, {"USDC"}), ["spot", "stablecoin", "hip3-collateral"])
        self.assertEqual(categories.categories_for("HYPE", False, set()), ["spot", "native"])
        self.assertEqual(categories.categories_for("UETH", False, set()), ["spot", "wrapped"])
        self.assertEqual(categories.categories_for("PURR", False, set()), ["spot"])


class SignalTests(unittest.TestCase):
    """market-signals Rust port (v1.10.0)."""

    def _rows(self, closes, vols, day=dt.date(2026, 10, 2)):
        return [{"token_address": T, "bucket_minute": dt.datetime(2026, 10, 2, 0, i, tzinfo=UTC), "bucket_date": day,
                 "close_price_usd": c, "volume_token": v, "volume_usd": v * c} for i, (c, v) in enumerate(zip(closes, vols))]

    def test_ema_spike_resistance_and_breakout(self):
        rows = self._rows([1.0, 1.0, 2.0, 1.5], [10, 10, 50, 10])
        out = signals.token_signals(rows, dt.date(2026, 10, 1))
        self.assertEqual(len(out), 4)
        # EMA seeded with the first close and updated on it: unchanged while the price is flat
        self.assertAlmostEqual(out[0]["ema9"], 1.0)
        self.assertAlmostEqual(out[1]["ema21"], 1.0)
        self.assertGreater(out[2]["ema9"], out[2]["ema21"])            # 9 reacts faster: bullish
        self.assertEqual(out[2]["ema_signal"], "bullish")
        # bucket 3: volume 50 vs mean(10, 10, 50) = 23.3 -> 2.14x = spike; close 2.0 > max of
        # the window INCLUDING itself is impossible -> never a breakout by construction
        self.assertAlmostEqual(out[2]["volume_token_spike_ratio"], 50 / (70 / 3))
        self.assertEqual(out[2]["volume_token_spike_signal"], "spike")
        self.assertEqual(out[2]["resistance_price"], 2.0)
        self.assertEqual(out[2]["price_breakout"], "normal")
        self.assertEqual(out[3]["resistance_price"], 2.0)             # rolling max carries
        self.assertEqual(out[3]["volume_usd_spike_signal"], "normal")

    def test_warmup_rows_are_dropped_and_global_rows_aggregate_per_minute(self):
        warm = self._rows([1.0, 1.0], [1, 1], day=dt.date(2026, 10, 1))
        rows = warm + self._rows([2.0, 2.0], [1, 3])
        out = signals.token_signals(rows, dt.date(2026, 10, 2))
        self.assertEqual(len(out), 2)
        self.assertTrue(all(r["bucket_date"] == dt.date(2026, 10, 2) for r in out))
        other = [dict(r, token_address="hlcore:token:9", close_price_usd=4.0, volume_token=1.0, volume_usd=4.0) for r in out]
        glob = signals.global_signals(out + other)
        self.assertEqual([g["global_flag"] for g in glob], ["global", "global"])
        self.assertEqual(glob[0]["token_address"], "")
        # minute 0: token 2.0 x 1 and other 4.0 x 1 -> vwap 3.0, total volume 2
        self.assertAlmostEqual(glob[0]["close_price_usd"], 3.0)
        self.assertAlmostEqual(glob[0]["volume_token"], 2.0)
        self.assertEqual(glob[0]["ema_signal"], "")

    def test_window_is_twenty_buckets(self):
        rows = self._rows([1.0] * 25 + [10.0], [1.0] * 26)
        out = signals.token_signals(rows, dt.date(2026, 10, 1))
        self.assertEqual(out[-1]["resistance_price"], 10.0)
        self.assertAlmostEqual(out[-2]["resistance_price"], 1.0)
        self.assertAlmostEqual(out[-1]["volume_token_spike_ratio"], 1.0)


class QualityTests(unittest.TestCase):
    def _rows(self, tok, day, vwaps, trades):
        return [{"token_address": tok, "bucket_minute": dt.datetime(day.year, day.month, day.day, 0, i, tzinfo=UTC),
                 "bucket_date": day, "vwap_price_usd": v, "num_trades": n} for i, (v, n) in enumerate(zip(vwaps, trades))]

    def test_scores_and_fail_reasons(self):
        d1, d2 = dt.date(2026, 10, 1), dt.date(2026, 10, 2)
        rows = (self._rows(T, d1, [1.0], [5])                       # lookback day: feeds the LAG only
                + self._rows(T, d2, [2.0, 2.2], [6, 6])             # jump 1.0 across midnight, 12 trades
                + self._rows("hlcore:token:9", d2, [1.0, 1.1], [1, 1]))  # 2 trades, jump 0.1
        out = quality.quality_rows(rows, d2)
        self.assertEqual([(r["token_address"], r["as_of_date"]) for r in out], [(T, d2), ("hlcore:token:9", d2)])
        a, b = out
        self.assertEqual((a["trades_in_window"], a["score"], a["is_pass"]), (12, 60, False))
        self.assertAlmostEqual(a["max_jump_from_prev"], 1.0)
        self.assertEqual(a["fail_reason"], "high_jump:1")
        self.assertEqual((b["score"], b["fail_reason"]), (40, "low_trades:2"))
        self.assertEqual(a["latest_bucket_minute"], dt.datetime(2026, 10, 2, 0, 1, tzinfo=UTC))
        self.assertEqual(a["quality_version"], "v1_trades_jump")

    def test_pass_and_zero(self):
        d = dt.date(2026, 10, 2)
        out = quality.quality_rows(self._rows(T, d, [1.0] * 3, [4, 4, 4]), d)
        self.assertEqual((out[0]["score"], out[0]["is_pass"], out[0]["fail_reason"]), (100, True, ""))
        out = quality.quality_rows(self._rows(T, d, [1.0, 3.0], [1, 1]), d)
        self.assertEqual((out[0]["score"], out[0]["fail_reason"]), (0, "low_trades:2,high_jump:2"))


class MarketTests(unittest.TestCase):
    def test_midrank_percentile(self):
        self.assertEqual(midrank_percentile([1, 2, 3, 4], 3), (2 + 0.5) / 4)
        self.assertEqual(midrank_percentile([5, 5, 5], 5), 0.5)          # all tied -> neutral
        self.assertEqual(midrank_percentile([1, 2], 9), 1.0)

    def test_fear_index_needs_sixty_days_then_prints_every_day(self):
        today = dt.date(2026, 10, 21)
        base, caps = [], {}
        for k in range(100, 0, -1):
            d = today - dt.timedelta(days=k)
            close = 100.0 + (100 - k) * 0.5                   # a steady uptrend
            base.append({"token_address": T, "day_date": d, "close": close, "volume_usd": 1000.0 + k, "is_stable": False})
            base.append({"token_address": S.USDC_ADDRESS, "day_date": d, "close": 1.0, "volume_usd": 0.0, "is_stable": True})
            caps[(T, d)] = close * 1e6
        out = market.fear_index_rows(base, caps, today)
        # the 30d realized volatility needs two log returns, so the first two days have no
        # rv and are not calibrated: 98 calibrated days, the first 59 are warm-up
        self.assertEqual(len(out), 98 - 59)
        last = out[-1]
        self.assertEqual(last["as_of_date"], today - dt.timedelta(days=1))
        self.assertTrue(0 <= last["fear_index"] <= 100)
        self.assertIn(last["market_profile"], {"Extreme Fear", "Fear", "Neutral", "Greed", "Extreme Greed"})
        # stable share of volume is 0 every day -> all tied -> market_comp 50 (neutral)
        self.assertEqual(last["market_comp_score"], 50.0)
        # a steady uptrend above its 90d MA: momentum in the upper half, volatility calm
        self.assertGreater(last["momentum_score"], 50.0)
        self.assertEqual(last["fear_index"], round(0.30 * last["momentum_score"] + 0.20 * last["breadth_score"]
                                                   + 0.20 * last["volatility_score"] + 0.15 * last["volume_score"]
                                                   + 0.15 * last["market_comp_score"], 2))


class TokenTests(unittest.TestCase):
    def test_log_returns_forward_fill_and_zero_when_missing(self):
        dates = [dt.date(2026, 10, d) for d in (1, 2, 3, 4)]
        lr = tokens._log_returns({dates[1]: 2.0, dates[3]: 4.0}, dates)
        self.assertEqual(lr[0], 0.0)                      # no price yet
        self.assertEqual(lr[1], 0.0)                      # previous close 0 -> COALESCE 0
        self.assertEqual(lr[2], 0.0)                      # forward-filled, flat
        self.assertAlmostEqual(lr[3], math.log(2.0))

    def test_beta_rows(self):
        eth = [0.01, -0.02, 0.03, 0.0, 0.01]
        rows = tokens._beta_rows({"a": [x * 2 for x in eth], "b": [0.0] * 5}, eth)
        a, b = rows
        self.assertAlmostEqual(a["beta"], 2.0)
        self.assertEqual(a["desc_beta"], "Over Volatility than ETH")
        self.assertAlmostEqual(a["annual_volatility"], statistics.stdev([x * 2 for x in eth]) * math.sqrt(365))
        self.assertAlmostEqual(b["beta"], 0.0)
        self.assertEqual(b["desc_beta"], "Lower Volatility than ETH")
        self.assertIsNone(b["sharp_ratio"])              # stddev 0 -> NULLIF
        self.assertIsNone(tokens._beta_rows({"a": eth}, None)[0]["beta"])

    def test_pct_rules(self):
        t0, t1 = ts(1), ts(2)
        self.assertIsNone(tokens._pct((t1, 2.0), None))
        self.assertIsNone(tokens._pct((t1, 2.0), (t1, 1.0)))       # same bucket both sides
        self.assertIsNone(tokens._pct((t1, 2.0), (t0, 0.0)))       # zero previous
        self.assertAlmostEqual(tokens._pct((t1, 2.0), (t0, 1.0), 100.0), 100.0)
        self.assertAlmostEqual(tokens._pct((t1, 2.0), (t0, 1.0)), 1.0)


if __name__ == "__main__":
    unittest.main()
