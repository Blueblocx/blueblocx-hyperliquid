"""
Invariant tests for the HyperCore L1 mapping — specifically the failure modes
that single-wallet end-to-end validation (cashflow identity, portfolio oracle)
CANNOT catch:

  * natural-key collisions ACROSS wallets on (tx_hash, event_index, direction),
    which is the dedup key the downstream Rust consumers use without wallet
  * positional (non-content-derived) event_index
  * dust fills (tid=0 / zero hash) collapsing into one key
  * accountClassTransfer not netting to zero
  * non-USDC-quoted spot pairs booked as USDC

Run:  python -m unittest discover -s tests -v
"""
from __future__ import annotations

import unittest

from blueblocx_hyperliquid import mappers, schema as S
from blueblocx_hyperliquid.spot_meta import SpotMetaResolver

WALLET_A = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
WALLET_B = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
NOW = "2026-10-05T00:00:00+00:00"
REAL_HASH = "0x" + "ab" * 32

# Minimal spotMeta: USDC(0), HYPE(150), FOO(200), BAR(201). Pairs:
#   @1  HYPE/USDC   tokens [150, 0]
#   @2  FOO/USDC    tokens [200, 0]
#   @3  FOO quoted in BAR (non-USDC quote) tokens [200, 201]
SPOT_META = {
    "tokens": [
        {"name": "USDC", "index": 0, "weiDecimals": 8},
        {"name": "HYPE", "index": 150, "weiDecimals": 8},
        {"name": "FOO", "index": 200, "weiDecimals": 6},
        {"name": "BAR", "index": 201, "weiDecimals": 5},
    ],
    "universe": [
        {"name": "HYPE/USDC", "index": 1, "tokens": [150, 0], "isCanonical": True},
        {"name": "FOO/USDC", "index": 2, "tokens": [200, 0], "isCanonical": True},
        {"name": "@3", "index": 3, "tokens": [200, 201], "isCanonical": False},
    ],
}


def perp_fill(tid, time_ms, coin="BTC", closed="1.5", fee="0.1", h=REAL_HASH, **kw):
    f = {"coin": coin, "px": "100", "sz": "1", "side": "B", "time": time_ms,
         "startPosition": "0", "dir": "Close Long", "closedPnl": closed,
         "hash": h, "oid": 1, "crossed": True, "fee": fee, "tid": tid,
         "feeToken": "USDC", "twapId": None}
    f.update(kw)
    return f


def spot_fill(tid, time_ms, coin="HYPE/USDC", side="B", sz="2", px="50",
              fee="0.01", fee_token="HYPE", h=REAL_HASH):
    return {"coin": coin, "px": px, "sz": sz, "side": side, "time": time_ms,
            "startPosition": "0", "dir": "Buy", "closedPnl": "0.0", "hash": h,
            "oid": 2, "crossed": True, "fee": fee, "tid": tid, "feeToken": fee_token,
            "twapId": None}


def funding(time_ms, coin, usdc):
    return {"time": time_ms, "hash": S.ZERO_HASH,
            "delta": {"type": "funding", "coin": coin, "usdc": usdc,
                      "szi": "1", "fundingRate": "0.0001", "nSamples": None}}


def ledger(time_ms, typ, h=REAL_HASH, **delta):
    d = {"type": typ}
    d.update(delta)
    return {"time": time_ms, "hash": h, "delta": d}


def map_all(wallet, fills=(), fund=(), led=(), resolver=None, registry=None):
    resolver = resolver or SpotMetaResolver(SPOT_META)
    registry = registry or mappers.TokenRegistry()
    rows = mappers.map_fills_to_ledger(list(fills), wallet, NOW, resolver, registry)
    rows += mappers.map_funding_to_ledger(list(fund), wallet, NOW, registry)
    r, unknown = mappers.map_non_funding_ledger(list(led), wallet, NOW, resolver, registry)
    rows += r
    return rows, unknown


def keys(rows):
    return [mappers.natural_key(r) for r in rows]


class CrossWalletKeyTests(unittest.TestCase):
    """The Rust dedup key has NO wallet. Identical events on two wallets must
    still produce distinct (tx_hash, event_index, direction)."""

    def test_identical_funding_on_two_wallets_distinct(self):
        f = [funding(1_700_000_000_000, "BTC", "0.5")]
        a, _ = map_all(WALLET_A, fund=f)
        b, _ = map_all(WALLET_B, fund=f)
        self.assertEqual(len(a), 1)
        self.assertNotEqual(keys(a)[0], keys(b)[0])

    def test_same_time_deposit_on_two_wallets_distinct(self):
        led = [ledger(1_700_000_000_000, "deposit", h=S.ZERO_HASH, usdc="100")]
        a, _ = map_all(WALLET_A, led=led)
        b, _ = map_all(WALLET_B, led=led)
        self.assertNotEqual(keys(a)[0], keys(b)[0])

    def test_send_same_hash_on_sender_and_receiver_distinct(self):
        # A `send` shows the SAME L1 hash on both wallets' ledgers.
        d = dict(token="HYPE", amount="3", user=WALLET_A, destination=WALLET_B)
        a, _ = map_all(WALLET_A, led=[ledger(1_700_000_000_000, "send", **d)])
        b, _ = map_all(WALLET_B, led=[ledger(1_700_000_000_000, "send", **d)])
        self.assertEqual(a[0]["direction"], S.DIRECTION_OUT)
        self.assertEqual(b[0]["direction"], S.DIRECTION_IN)
        self.assertNotEqual(a[0]["tx_hash"], b[0]["tx_hash"])

    def test_identical_perp_fill_tid_on_two_wallets_distinct(self):
        # Same tid cannot really happen across wallets, but the key must still
        # be wallet-scoped so nothing relies on that assumption.
        f = [perp_fill(tid=42, time_ms=1_700_000_000_000)]
        a, _ = map_all(WALLET_A, fills=f)
        b, _ = map_all(WALLET_B, fills=f)
        self.assertTrue(set(keys(a)).isdisjoint(set(keys(b))))

    def test_chainwide_uniqueness_over_mixed_dataset(self):
        t = 1_700_000_000_000
        fills = [perp_fill(1, t), perp_fill(2, t, h=REAL_HASH),      # same order hash
                 spot_fill(3, t + 1), perp_fill(0, t + 2, h=S.ZERO_HASH),  # dust
                 perp_fill(0, t + 3, h=S.ZERO_HASH, coin="ETH")]      # dust, other coin
        fund = [funding(t, "BTC", "0.1"), funding(t, "ETH", "-0.2")]
        led = [ledger(t, "deposit", usdc="10"),
               ledger(t + 5, "accountClassTransfer", h=S.ZERO_HASH, usdc="5", toPerp=True),
               ledger(t + 6, "vaultDeposit", usdc="7", vault="0xv")]
        rows = []
        for w in (WALLET_A, WALLET_B):
            r, unknown = map_all(w, fills, fund, led)
            self.assertEqual(unknown, {})
            rows += r
        ks = keys(rows)
        self.assertEqual(len(ks), len(set(ks)), "natural key must be unique chain-wide")


class ContentAddressedKeyTests(unittest.TestCase):
    def test_dust_fills_tid0_distinct_by_time_and_coin(self):
        t = 1_700_000_000_000
        rows, _ = map_all(WALLET_A, fills=[perp_fill(0, t, h=S.ZERO_HASH),
                                           perp_fill(0, t + 1, h=S.ZERO_HASH),
                                           perp_fill(0, t, h=S.ZERO_HASH, coin="ETH")])
        hashes = {r["tx_hash"] for r in rows}
        self.assertEqual(len(hashes), 3)

    def test_multi_fill_same_order_hash_distinct_tx_hash(self):
        t = 1_700_000_000_000
        rows, _ = map_all(WALLET_A, fills=[perp_fill(10, t, h=REAL_HASH),
                                           perp_fill(11, t, h=REAL_HASH)])
        self.assertEqual(len({r["tx_hash"] for r in rows}), 2)
        self.assertEqual(len(keys(rows)), len(set(keys(rows))))

    def test_fixed_ordinals_perp(self):
        rows, _ = map_all(WALLET_A, fills=[perp_fill(1, 1_700_000_000_000)])
        by_type = {r["event_type"]: r["event_index"] for r in rows}
        self.assertEqual(by_type[S.EVT_PERP_PNL], S.LEG_PERP_PNL)
        self.assertEqual(by_type[S.EVT_PERP_FEE], S.LEG_PERP_FEE)

    def test_fixed_ordinals_spot_buy_fee_in_base_token(self):
        rows, _ = map_all(WALLET_A, fills=[spot_fill(5, 1_700_000_000_000)])
        self.assertEqual(sorted(r["event_index"] for r in rows),
                         [S.LEG_SPOT_BASE, S.LEG_SPOT_QUOTE, S.LEG_SPOT_FEE])
        fee = next(r for r in rows if r["event_type"] == S.EVT_PERP_FEE)
        base = next(r for r in rows if r["event_index"] == S.LEG_SPOT_BASE)
        self.assertEqual(fee["token_address"], base["token_address"],
                         "spot BUY fee is charged in the received (base) token")
        self.assertEqual(base["decimals"], 8)

    def test_spot_legs_name_the_market_as_counterparty(self):
        # A spot fill settles against the order book: base, quote AND fee legs
        # carry "market:<BASE>/<QUOTE>" (lowercased like every counterparty) so the
        # site can say "Market HYPE/USDC" instead of NULL or the raw pair id.
        rows, _ = map_all(WALLET_A, fills=[spot_fill(5, 1_700_000_000_000)])
        self.assertEqual({r["counterparty_address"] for r in rows}, {"market:hype/usdc"})

    def test_unknown_spot_pair_keeps_the_raw_coin_in_the_market_label(self):
        rows, _ = map_all(WALLET_A, fills=[spot_fill(6, 1_700_000_000_000, coin="@999",
                                                     fee_token="USDC")])
        self.assertEqual({r["counterparty_address"] for r in rows}, {"market:@999"})

    def test_perp_fee_leg_keeps_the_coin_as_counterparty(self):
        rows, _ = map_all(WALLET_A, fills=[perp_fill(7, 1_700_000_000_000)])
        fee = next(r for r in rows if r["event_type"] == S.EVT_PERP_FEE)
        self.assertEqual(fee["counterparty_address"], "btc")

    def test_idempotent_mapping(self):
        t = 1_700_000_000_000
        args = dict(fills=[perp_fill(1, t), spot_fill(2, t)],
                    fund=[funding(t, "BTC", "0.3")],
                    led=[ledger(t, "deposit", usdc="1")])
        a, _ = map_all(WALLET_A, **args)
        b, _ = map_all(WALLET_A, **args)
        strip = lambda r: {k: v for k, v in r.items() if k not in ("created_at", "updated_at")}
        self.assertEqual([strip(r) for r in a], [strip(r) for r in b])

    def test_event_index_does_not_depend_on_input_order(self):
        t = 1_700_000_000_000
        f1, f2 = perp_fill(1, t), perp_fill(2, t + 1)
        a, _ = map_all(WALLET_A, fills=[f1, f2])
        b, _ = map_all(WALLET_A, fills=[f2, f1])
        self.assertEqual(sorted(keys(a)), sorted(keys(b)))

    def test_two_withdrawals_same_millisecond_get_distinct_keys(self):
        # withdraw deltas carry no real L1 hash (a withdrawal is a bridge request,
        # not yet settled when the ledger records it); the fallback tx_hash was
        # (wallet, type, time_ms) with no sub-millisecond disambiguator, so two
        # withdrawals landing in the same millisecond collided and aborted the
        # run's natural-key check (confirmed 2026-10-23: $346,408.54 vs $149,899.00
        # for the same wallet, same millisecond)
        t = 1_700_000_000_000
        rows, _ = map_all(WALLET_A, led=[ledger(t, "withdraw", h=S.ZERO_HASH, usdc="346408.54"),
                                         ledger(t, "withdraw", h=S.ZERO_HASH, usdc="149899.0")])
        self.assertEqual(len(rows), 2)
        self.assertEqual(len({r["tx_hash"] for r in rows}), 2, "the two withdrawals must get distinct tx_hash")
        self.assertEqual(len(keys(rows)), len(set(keys(rows))))
        self.assertEqual({r["amount"] for r in rows}, {346408.54, 149899.0})

    def test_first_occurrence_keeps_the_hash_unchanged(self):
        # the common, non-colliding case must be byte-for-byte what it always was -
        # dup_seq only engages from the SECOND occurrence of the exact same
        # (hash, type, time) tuple, so no previously-written row's tx_hash moves
        t = 1_700_000_000_000
        solo, _ = map_all(WALLET_A, led=[ledger(t, "withdraw", h=S.ZERO_HASH, usdc="500")])
        first_of_two, _ = map_all(WALLET_A, led=[ledger(t, "withdraw", h=S.ZERO_HASH, usdc="500"),
                                                 ledger(t, "withdraw", h=S.ZERO_HASH, usdc="700")])
        self.assertEqual(solo[0]["tx_hash"], first_of_two[0]["tx_hash"])

    def test_three_way_collision_stays_pairwise_distinct(self):
        t = 1_700_000_000_000
        rows, _ = map_all(WALLET_A, led=[ledger(t, "withdraw", h=S.ZERO_HASH, usdc=str(v))
                                        for v in (1, 2, 3)])
        self.assertEqual(len({r["tx_hash"] for r in rows}), 3)


class AccountingInvariantTests(unittest.TestCase):
    def test_account_class_transfer_nets_to_zero(self):
        rows, _ = map_all(WALLET_A, led=[ledger(1_700_000_000_000, "accountClassTransfer",
                                                h=S.ZERO_HASH, usdc="201", toPerp=False)])
        self.assertEqual(len(rows), 2)
        self.assertAlmostEqual(sum(r["amount_signed"] for r in rows), 0.0)
        self.assertEqual({r["event_index"] for r in rows},
                         {S.LEG_LEDGER_INTERNAL_OUT, S.LEG_LEDGER_INTERNAL_IN})

    def test_non_usdc_quote_pair_books_real_quote_token(self):
        rows, _ = map_all(WALLET_A, fills=[spot_fill(7, 1_700_000_000_000, coin="@3",
                                                     fee="0", fee_token="FOO")])
        quote = next(r for r in rows if r["event_index"] == S.LEG_SPOT_QUOTE)
        self.assertEqual(quote["token_address"], "hlcore:token:201")  # BAR, not USDC
        self.assertEqual(quote["decimals"], 5)

    def test_standard_is_always_erc20(self):
        t = 1_700_000_000_000
        rows, _ = map_all(WALLET_A, fills=[perp_fill(1, t), spot_fill(2, t)],
                          fund=[funding(t, "BTC", "0.3")],
                          led=[ledger(t, "deposit", usdc="1")])
        self.assertTrue(rows)
        self.assertTrue(all(r["standard"] == "ERC20" for r in rows))

    def test_unknown_ledger_type_is_surfaced_not_silent(self):
        rows, unknown = map_all(WALLET_A, led=[ledger(1_700_000_000_000, "someNewType",
                                                      usdc="9")])
        self.assertEqual(unknown, {"someNewType": 1})
        self.assertEqual(rows[0]["event_type"], S.EVT_TRANSFER)

    def test_perp_pnl_is_not_flow(self):
        self.assertTrue(S.PERP_CASHFLOW_TYPES.isdisjoint(S.FLOW_EVENT_TYPES))
        self.assertIn(S.EVT_PERP_PNL, S.REALIZED_PNL_EVENT_TYPES)


class SpotPriceCtxTests(unittest.TestCase):
    """spotMetaAndAssetCtxs ctxs are NOT index-aligned with universe; prices must
    be matched on the ctx `coin` field (regression: 1M MAX tokens got another
    pair's price -> net worth off by $11.5M)."""

    def test_prices_keyed_by_ctx_coin_not_index(self):
        from blueblocx_hyperliquid import prices
        resolver = SpotMetaResolver(SPOT_META)
        # ctxs deliberately shuffled + padded with extra markets not in universe
        ctxs = [
            {"coin": "@999", "midPx": "0.000001"},
            {"coin": "FOO/USDC", "midPx": "2.5", "markPx": "2.4"},
            {"coin": "#84180", "midPx": "7"},
            {"coin": "HYPE/USDC", "midPx": "90.5", "markPx": "90.4"},
        ]
        px = prices.spot_symbol_prices(resolver, SPOT_META, ctxs, ["HYPE", "FOO", "BAR"])
        self.assertEqual(px["HYPE"], 90.4)   # markPx preferred over midPx
        self.assertEqual(px["FOO"], 2.4)
        self.assertNotIn("BAR", px)  # no USDC pair for BAR

    def test_midpx_fallback_when_no_mark(self):
        from blueblocx_hyperliquid import prices
        resolver = SpotMetaResolver(SPOT_META)
        px = prices.spot_symbol_prices(resolver, SPOT_META,
                                       [{"coin": "HYPE/USDC", "markPx": None, "midPx": "88"}],
                                       ["HYPE"])
        self.assertEqual(px["HYPE"], 88.0)


class NetWorthCompositionTests(unittest.TestCase):
    """net_worth = perp (minus portfolio-margin overlap) + spot + staked + vault.
    Verified live: unified wallet gap -0.001%, plain perp wallet gap 0.0."""

    def _analytics(self, spot_hold=None, vaults=None, staked="0", account_value="500"):
        from blueblocx_hyperliquid import analytics
        usdc = {"coin": "USDC", "token": 0, "total": "2000", "hold": "0"}
        if spot_hold is not None:
            usdc["spotHold"] = spot_hold
        spot_state = {"balances": [usdc, {"coin": "HYPE", "token": 150, "total": "2"}]}
        chs = {"marginSummary": {"accountValue": account_value, "totalMarginUsed": "100"},
               "assetPositions": []}
        return analytics.compute_analytics(
            [], [], chs, spot_state, 1_700_000_000_000, NOW,
            spot_price_by_symbol={"HYPE": 100.0},
            staking={"delegated": staked}, token_usd_price={}, vault_equities=vaults or [])

    def test_separate_account_adds_perp_on_top_of_spot(self):
        # spot USDC with hold 0 and perp margin > 0 -> accounts are separate
        a = self._analytics()
        self.assertEqual(a["perp_spot_overlap_usd"], 0.0)
        self.assertEqual(a["perp_spot_overlap_rule"], "separate")
        self.assertAlmostEqual(a["net_worth"], 500 + 2000 + 200)

    def test_portfolio_margin_account_does_not_double_count_perp(self):
        a = self._analytics(spot_hold="500")  # spotHold == perp equity
        self.assertAlmostEqual(a["perp_spot_overlap_usd"], 500.0)
        self.assertEqual(a["perp_spot_overlap_rule"], "spotHold")
        self.assertAlmostEqual(a["net_worth"], 2000 + 200)

    def test_l4x_snapshot_carries_dex(self):
        from blueblocx_hyperliquid import prices
        chs = {"marginSummary": {"accountValue": "10"}, "assetPositions": [
            {"position": {"coin": "xyz:SP500", "szi": "1", "entryPx": "1", "positionValue": "1",
                          "unrealizedPnl": "0", "returnOnEquity": "0", "marginUsed": "1",
                          "leverage": {"type": "cross", "value": 1}, "cumFunding": {}}}]}
        rows = prices.clearinghouse_to_snapshot(chs, WALLET_A, 1_700_000_000_000, NOW, dex="xyz")
        self.assertEqual(rows[0]["dex"], "xyz")
        self.assertIn("dex", S.L4X_PERP_POSITION_SNAPSHOT_COLUMNS)

    def test_vault_and_staking_are_added(self):
        a = self._analytics(vaults=[{"equity": "1000"}, {"equity": "0.5"}], staked="3")
        self.assertAlmostEqual(a["vault_value_usd"], 1000.5)
        self.assertAlmostEqual(a["staked_value_usd"], 300.0)
        self.assertAlmostEqual(a["net_worth"], 500 + 2000 + 200 + 300 + 1000.5)

    def test_non_usdc_fee_leg_is_valued_not_summed_raw(self):
        from blueblocx_hyperliquid import analytics
        t = 1_700_000_000_000
        rows, _ = map_all(WALLET_A, fills=[spot_fill(5, t)])  # fee leg in HYPE
        chs = {"marginSummary": {"accountValue": "0"}, "assetPositions": []}
        a = analytics.compute_analytics(
            rows, [], chs, {"balances": []}, t + 1000, NOW,
            spot_price_by_symbol={}, staking={}, token_usd_price={"hlcore:token:150": 50.0})
        # fee 0.01 HYPE @ $50 -> -$0.50 realized (fee), not -0.01
        self.assertAlmostEqual(a["realized_pnl_24h"], -0.5)
        self.assertEqual(a["unvalued_ledger_rows"], 0)

    def test_unpriced_token_rows_are_counted_not_silently_summed(self):
        from blueblocx_hyperliquid import analytics
        t = 1_700_000_000_000
        rows, _ = map_all(WALLET_A, fills=[spot_fill(5, t)])
        chs = {"marginSummary": {"accountValue": "0"}, "assetPositions": []}
        a = analytics.compute_analytics(rows, [], chs, {"balances": []}, t + 1000, NOW,
                                        token_usd_price={})  # no HYPE price
        self.assertGreater(a["unvalued_ledger_rows"], 0)
        self.assertIn("hlcore:token:150", a["unvalued_token_addresses"])


class InternalMoveTests(unittest.TestCase):
    """Moves within the wallet's own books must net to zero (two legs), never
    appear as one-sided inflows. Shapes copied from live ledger data."""

    def _net_zero(self, rows, n_legs=2):
        self.assertEqual(len(rows), n_legs)
        self.assertAlmostEqual(sum(r["amount_signed"] for r in rows), 0.0)
        self.assertEqual(len(keys(rows)), len(set(keys(rows))))

    def test_self_send_between_dexes_nets_to_zero(self):
        # live shape: user == destination == wallet, sourceDex spot -> destinationDex ""
        rows, _ = map_all(WALLET_A, led=[ledger(
            1_700_000_000_000, "send", user=WALLET_A, destination=WALLET_A,
            sourceDex="spot", destinationDex="", token="USDC", amount="147.96",
            usdcValue="147.96", fee="0.0", nonce=1)])
        self._net_zero(rows)

    def test_real_send_to_other_wallet_is_one_sided(self):
        rows, _ = map_all(WALLET_A, led=[ledger(
            1_700_000_000_000, "send", user=WALLET_A, destination=WALLET_B,
            sourceDex="spot", destinationDex="spot", token="USDC", amount="720.46",
            usdcValue="720.46")])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["direction"], S.DIRECTION_OUT)

    def test_c_staking_transfer_nets_to_zero(self):
        rows, unknown = map_all(WALLET_A, led=[ledger(
            1_700_000_000_000, "cStakingTransfer", token="HYPE", amount="10.0", isDeposit=True)])
        self.assertEqual(unknown, {})
        self._net_zero(rows)
        self.assertEqual(rows[0]["token_address"], "hlcore:token:150")

    def test_borrow_lend_usdc_not_dropped_and_nets_to_zero(self):
        # live shape: no usdc field, plain amount on a USDC token — used to be dropped
        rows, unknown = map_all(WALLET_A, led=[ledger(
            1_700_000_000_000, "borrowLend", token="USDC", operation="supply",
            amount="296.6252397", interestAmount="0.0")])
        self.assertEqual(unknown, {})
        self._net_zero(rows)

    def test_borrow_lend_interest_is_income(self):
        rows, _ = map_all(WALLET_A, led=[ledger(
            1_700_000_000_000, "borrowLend", token="USDC", operation="supply",
            amount="100", interestAmount="0.25")])
        self.assertEqual(len(rows), 3)
        interest = [r for r in rows if r["event_type"] == S.EVT_INTEREST]
        self.assertEqual(len(interest), 1)
        self.assertAlmostEqual(interest[0]["amount_signed"], 0.25)
        self.assertEqual(interest[0]["event_index"], S.LEG_LEDGER_INTEREST)
        self.assertIn(S.EVT_INTEREST, S.REALIZED_PNL_EVENT_TYPES)


class SpotClosedPnlTests(unittest.TestCase):
    def test_spot_sell_with_closed_pnl_emits_no_perp_pnl_row(self):
        # live shape: HL reports closedPnl on spot sells (vs its cost basis); spot
        # realized PnL is the FIFO layer's job, so only base/quote(/fee) legs appear.
        f = spot_fill(9, 1_700_000_000_000, coin="FOO/USDC", side="A", fee="0.3",
                      fee_token="USDC")
        f["closedPnl"] = "-0.00599999"
        rows, _ = map_all(WALLET_A, fills=[f])
        self.assertEqual({r["event_type"] for r in rows}, {S.EVT_SPOT_FILL, S.EVT_PERP_FEE})
        self.assertFalse(any(r["event_type"] == S.EVT_PERP_PNL for r in rows))


class BuilderFeeOrdinalTests(unittest.TestCase):
    def test_spot_fill_with_fee_and_builder_fee_has_distinct_keys(self):
        f = spot_fill(5, 1_700_000_000_000)
        f["builderFee"] = "0.02"
        rows, _ = map_all(WALLET_A, fills=[f])
        self.assertEqual(len(rows), 4)
        self.assertEqual(len(keys(rows)), 4)
        self.assertIn(S.LEG_SPOT_BUILDER_FEE, {r["event_index"] for r in rows})

    def test_perp_fill_with_builder_fee(self):
        f = perp_fill(1, 1_700_000_000_000, builderFee="0.03")
        rows, _ = map_all(WALLET_A, fills=[f])
        self.assertEqual(sorted(r["event_index"] for r in rows),
                         [S.LEG_PERP_PNL, S.LEG_PERP_FEE, S.LEG_PERP_BUILDER_FEE])


class WeightBudgetTests(unittest.TestCase):
    """settle() must correct the CALLER's slot, not whichever slot is last."""

    def test_settle_by_handle_under_interleaving(self):
        from blueblocx_hyperliquid.info_client import _WeightBudget
        b = _WeightBudget(300)
        s1 = b.acquire(20)      # T1 small call
        s2 = b.acquire(270)     # T2 big candle call
        b.settle(s1, 20)        # T1 returns first — must NOT touch T2's 270
        used = sum(w for _, w in b._events)
        self.assertEqual(used, 290)
        b.settle(s2, 270)
        self.assertEqual(sum(w for _, w in b._events), 290)


class AnalyticsCoverageTests(unittest.TestCase):
    def test_windows_beyond_coverage_are_none_with_flag(self):
        from blueblocx_hyperliquid import analytics
        now = 1_700_000_000_000
        chs = {"marginSummary": {"accountValue": "0"}, "assetPositions": []}
        a = analytics.compute_analytics([], [], chs, {"balances": []}, now, NOW,
                                        window_start_ms=now - 2 * 86_400_000)
        self.assertTrue(a["realized_pnl_24h_complete"])
        self.assertFalse(a["realized_pnl_7d_complete"])
        self.assertIsNone(a["realized_pnl_7d"])
        self.assertIsNone(a["net_flow_30d"])
        self.assertAlmostEqual(a["coverage_days"], 2.0)

    def test_truncated_fills_invalidate_all_windows(self):
        from blueblocx_hyperliquid import analytics
        now = 1_700_000_000_000
        chs = {"marginSummary": {"accountValue": "0"}, "assetPositions": []}
        a = analytics.compute_analytics([], [], chs, {"balances": []}, now, NOW,
                                        window_start_ms=now - 40 * 86_400_000,
                                        fills_truncated=True)
        self.assertFalse(a["realized_pnl_24h_complete"])
        self.assertIsNone(a["realized_pnl_24h"])
        self.assertTrue(a["fills_truncated"])


if __name__ == "__main__":
    unittest.main()
