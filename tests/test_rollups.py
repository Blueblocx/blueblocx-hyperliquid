"""Rollup engines against the atlas formulas (docs/ATLAS_WALLET_TABLE_FORMULAS.md):
pure functions over small synthetic ledgers, no catalog, no network."""
from __future__ import annotations

import datetime as dt
import unittest

import pyarrow as pa

from blueblocx_hyperliquid.rollups import PLAN, REQUIRED, Budget, balances, categories, fifo
from blueblocx_hyperliquid.rollups.common import asof, table_from_rows, ddl_columns

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


if __name__ == "__main__":
    unittest.main()
