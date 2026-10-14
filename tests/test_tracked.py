"""The automatic wallet list (leaderboard whales + smart money, same rules as the TS
platform-backend), its cache/cursor, the soft deadline in extract.run and how the
daily runner and the summary report both. No network."""
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

from blueblocx_hyperliquid import daily, extract, ops, preflight, tracked
from blueblocx_hyperliquid import schema as S

P1 = "0x" + "11" * 20
S1 = "0x" + "aa" * 20
S2 = "0x" + "bb" * 20
W1 = "0x" + "cc" * 20
W2 = "0x" + "dd" * 20
W3 = "0x" + "ee" * 20


def _lb_row(addr, av, pnl=0, roi=0, vlm=0, **extra):
    d = {"ethAddress": addr, "accountValue": str(av), "prize": 0, "displayName": None,
         "windowPerformances": [["day", {"pnl": "1", "roi": "0.1", "vlm": "5"}],
                                ["month", {"pnl": str(pnl), "roi": str(roi), "vlm": str(vlm)}]]}
    d.update(extra)
    return d


def _payload(rows, indent=2):
    # the live payload is pretty-printed (newline after '[' and after every ','); keep that shape
    return json.dumps({"leaderboardRows": rows}, indent=indent)


def _clean_recon(wallets, deferred=(), truncated=()):
    by = {}
    for w in wallets:
        by[w] = {
            "fidelity_ok": True, "l1_ledger_rows": 12, "raw_fills": 5, "open_positions": 1,
            "fills_truncated_at_10k_cap": w in truncated, "fetch_errors": {}, "unmapped_ledger_types": {},
            "unpriced_held_symbols": [], "lending_lazy_pull": False,
            "oracle": {"checked": True, "account_value_matches": True, "networth_matches": True,
                       "pnl": {"checked": False, "reason": "no prior snapshot (~24h) yet"}},
        }
    return {"natural_key_ok": True, "by_wallet": by, "iceberg": None, "retention": None,
            "hip3_dexes_checked": [], "unmapped_ledger_types": {},
            "wallets": list(wallets), "deferred": list(deferred)}


class ParseAndSelectTests(unittest.TestCase):
    def test_parse_keeps_rows_above_floor_lowercased_and_survives_odd_strings(self):
        rows = [
            _lb_row(W1.upper(), 5e7, pnl=-1e6, roi=-0.02, vlm=1e9, displayName='}{"]} tricky \\" name'),
            _lb_row(S1, 2e5, pnl=1e6, roi=0.5, vlm=6e6),
            _lb_row(W2, 5e4),                                        # below the floor
            _lb_row(W3, "abc"),                                      # unparsable accountValue
            {"ethAddress": S2, "accountValue": "150000"},            # no windowPerformances
            {"ethAddress": "not-an-address", "accountValue": "9e9"},
            _lb_row(P1, 1e6, pnl="NaN", roi="Infinity", vlm="x"),  # non-finite -> 0
        ]
        kept, total = tracked.parse_leaderboard(_payload(rows))
        self.assertEqual(total, 7)
        by = {r.address: r for r in kept}
        self.assertEqual(set(by), {W1, S1, S2, P1})
        self.assertEqual(by[W1].account_value, 5e7)
        self.assertEqual((by[S1].month_pnl, by[S1].month_roi, by[S1].month_vlm), (1e6, 0.5, 6e6))
        self.assertEqual((by[S2].month_pnl, by[S2].month_roi, by[S2].month_vlm), (0.0, 0.0, 0.0))
        self.assertEqual((by[P1].month_pnl, by[P1].month_roi, by[P1].month_vlm), (0.0, 0.0, 0.0))

    def test_parse_compact_and_pretty_payloads_agree(self):
        rows = [_lb_row(W1, 5e7), _lb_row(S1, 2e5, pnl=1e6, roi=0.5, vlm=6e6), _lb_row(W2, 5e4)]
        compact = tracked.parse_leaderboard(_payload(rows, indent=None))
        pretty = tracked.parse_leaderboard(_payload(rows, indent=4))
        self.assertEqual(compact, pretty)
        self.assertEqual(compact[1], 3)
        self.assertEqual([r.address for r in compact[0]], [W1, S1])

    def test_parse_rejects_payload_without_rows(self):
        with self.assertRaises(ValueError):
            tracked.parse_leaderboard('{"something": []}')
        with self.assertRaises(ValueError):
            tracked.parse_leaderboard('{"leaderboardRows": [{"ethAddress": "0x"}')

    def test_select_mirrors_the_ts_rules(self):
        rows = [
            tracked.Row(W1, 9e8, month_pnl=-5e6, month_roi=-0.1, month_vlm=1e10),
            tracked.Row(W2, 3e8, month_pnl=1e4, month_roi=0.0, month_vlm=0.0),
            tracked.Row(W3, 1e8, month_pnl=2e6, month_roi=0.3, month_vlm=1e8),     # whale AND smart
            tracked.Row(S1, 2e5, month_pnl=5e5, month_roi=0.9, month_vlm=6e6),     # smart (lower pnl)
            tracked.Row(S2, 2e5, month_pnl=9e5, month_roi=0.25, month_vlm=6e6),    # smart (higher pnl)
            tracked.Row(P1, 2e5, month_pnl=9e9, month_roi=0.19, month_vlm=6e6),    # roi floor fails
            tracked.Row("0x" + "ff" * 20, 2e5, month_pnl=9e9, month_roi=0.9, month_vlm=4e6),  # vlm floor fails
            tracked.Row("0x" + "99" * 20, 9e4, month_pnl=9e9, month_roi=0.9, month_vlm=9e9),  # av floor fails
        ]
        sets = tracked.select_tracked(rows, whale_top_n=2, smart_top_n=2, smart_min_account_value=1e5,
                                      smart_min_month_roi=0.2, smart_min_month_vlm=5e6)
        self.assertEqual(sets["whales"], [W1, W2])                 # by account value, capped
        self.assertEqual(sets["smart"], [W3, S2])                  # by month pnl (not roi), capped
        sets = tracked.select_tracked(rows, whale_top_n=0, smart_top_n=10, smart_min_account_value=1e5,
                                      smart_min_month_roi=0.2, smart_min_month_vlm=5e6)
        self.assertEqual(sets["whales"], [])
        self.assertEqual(sets["smart"], [W3, S2, S1])


class PlanAndCursorTests(unittest.TestCase):
    SETS = {"smart": [S1, S2], "whales": [P1, W1, S1]}       # P1 pinned, S1 in both sets

    def test_pins_lead_then_smart_then_whales_deduped(self):
        order, auto = tracked.plan_wallets([P1, P1.upper()], self.SETS, 0)
        self.assertEqual(auto, [S1, S2, W1])
        self.assertEqual(order, [P1, S1, S2, W1])

    def test_rotation_and_cursor_advance(self):
        order, auto = tracked.plan_wallets([P1], self.SETS, 1)
        self.assertEqual(order, [P1, S2, W1, S1])
        # the run reached S2 and W1, deferred S1 -> next run starts at S1 (index 0)
        self.assertEqual(tracked.advance_cursor(auto, 1, {P1, S2, W1}), 0)
        # everything processed -> cursor unchanged
        self.assertEqual(tracked.advance_cursor(auto, 1, {P1, S2, W1, S1}), 1)
        # nothing of the auto part processed (deadline hit after the pins) -> unchanged
        self.assertEqual(tracked.advance_cursor(auto, 1, {P1}), 1)
        self.assertEqual(tracked.advance_cursor([], 5, {P1}), 0)
        self.assertEqual(tracked.rotate([], 3), [])

    def test_cursor_roundtrip(self):
        with tempfile.TemporaryDirectory() as d:
            ns = Path(d) / "hypercore"
            self.assertEqual(tracked.load_cursor(ns), 0)
            tracked.save_cursor(ns, 7)
            self.assertEqual(tracked.load_cursor(ns), 7)


class RefreshTests(unittest.TestCase):
    ROWS = [_lb_row(W1, 5e7), _lb_row(W2, 4e7), _lb_row(S1, 2e5, pnl=1e6, roi=0.5, vlm=6e6)]

    def test_fresh_fetch_writes_cache_and_keeps_cursor(self):
        with tempfile.TemporaryDirectory() as d:
            ns = Path(d) / "hypercore"
            tracked.save_cursor(ns, 2)
            sets, meta = tracked.refresh_tracked(ns, now=1_700_000_000.0, fetch=lambda url: _payload(self.ROWS),
                                                 whale_top_n=1, smart_top_n=5)
            self.assertEqual(sets, {"whales": [W1], "smart": [S1]})
            self.assertEqual(meta["source"], "fresh")
            self.assertEqual((meta["rows_total"], meta["rows_kept"]), (3, 3))
            cached = json.loads((ns / tracked.TRACKED_FILE).read_text(encoding="utf-8"))
            self.assertEqual(cached["cursor"], 2)
            self.assertEqual(cached["whales"], [W1])
            self.assertEqual(cached["fetched_at_ts"], 1_700_000_000.0)

    def test_fetch_failure_uses_fresh_cache_then_nothing(self):
        def boom(url):
            raise ConnectionError("stats host down")
        with tempfile.TemporaryDirectory() as d:
            ns = Path(d) / "hypercore"
            now = 1_700_000_000.0
            tracked.save_cached(ns, {"fetched_at": "x", "fetched_at_ts": now - 3600, "whales": [W1], "smart": [S1],
                                     "cursor": 1})
            sets, meta = tracked.refresh_tracked(ns, now=now, fetch=boom, max_age_h=48)
            self.assertEqual(sets, {"whales": [W1], "smart": [S1]})
            self.assertEqual(meta["source"], "cache")
            self.assertEqual(meta["age_h"], 1.0)
            self.assertIn("ConnectionError", meta["error"])
            sets, meta = tracked.refresh_tracked(ns, now=now + 100 * 3600, fetch=boom, max_age_h=48)
            self.assertEqual(sets, {"whales": [], "smart": []})
            self.assertEqual(meta["source"], "none")
            self.assertEqual(tracked.load_cursor(ns), 1)             # a failed refresh never touches the cursor
        with tempfile.TemporaryDirectory() as d:
            sets, meta = tracked.refresh_tracked(Path(d) / "hypercore", fetch=boom)
            self.assertEqual((sets, meta["source"]), ({"whales": [], "smart": []}, "none"))

    def test_empty_or_reshaped_payload_never_overwrites_the_cache(self):
        with tempfile.TemporaryDirectory() as d:
            ns = Path(d) / "hypercore"
            now = 1_700_000_000.0
            tracked.save_cached(ns, {"fetched_at": "x", "fetched_at_ts": now - 3600, "whales": [W1], "smart": [S1],
                                     "cursor": 3})
            before = (ns / tracked.TRACKED_FILE).read_text(encoding="utf-8")
            for payload in ('{"leaderboardRows": []}',                       # stats host rebuilding
                            _payload([_lb_row(W2, 5e4)]),                    # everything below the floor
                            '{"leaderboardRows": [{"address": "x", "value": "1"}]}'):   # reshaped rows
                sets, meta = tracked.refresh_tracked(ns, now=now, fetch=lambda url, p=payload: p)
                self.assertEqual((sets, meta["source"]), ({"whales": [W1], "smart": [S1]}, "cache"))
                self.assertIn("no rows above the floor", meta["error"])
            self.assertEqual((ns / tracked.TRACKED_FILE).read_text(encoding="utf-8"), before)

    def test_smart_floor_below_100k_lowers_the_scan_floor(self):
        rows = self.ROWS + [_lb_row(W3, 6e4, pnl=9e6, roi=0.9, vlm=9e7)]
        with tempfile.TemporaryDirectory() as d:
            ns = Path(d) / "hypercore"
            sets, _ = tracked.refresh_tracked(ns, fetch=lambda url: _payload(rows), smart_min_account_value=50_000)
            self.assertEqual(sets["smart"][0], W3)
            sets, _ = tracked.refresh_tracked(ns, fetch=lambda url: _payload(rows))
            self.assertNotIn(W3, sets["smart"])

    def test_terminated_is_not_swallowed_by_the_refresh(self):
        def fetch(url):
            daily._raise_terminated(15, None)
        with tempfile.TemporaryDirectory() as d:
            with self.assertRaises(daily.Terminated):
                tracked.refresh_tracked(Path(d) / "hypercore", fetch=fetch)

    def test_total_timeout_aborts_a_trickle(self):
        class R:
            def raise_for_status(self):
                pass

            def iter_content(self, n):
                for _ in range(100):
                    yield b"x"

            def close(self):
                pass
        clock = [0.0]

        def tick():
            clock[0] += 50.0
            return clock[0]
        with self.assertRaises(ValueError) as cm:
            tracked.fetch_leaderboard_text("u", get=lambda url, **kw: R(), clock=tick, total_timeout=120)
        self.assertIn("exceeded 120 s", str(cm.exception))

    def test_byte_cap_aborts_the_download(self):
        class R:
            def raise_for_status(self):
                pass

            def iter_content(self, n):
                yield b"x" * 10
                yield b"y" * 10

            def close(self):
                pass
        with self.assertRaises(ValueError):
            tracked.fetch_leaderboard_text("u", byte_cap=15, get=lambda url, **kw: R())
        self.assertEqual(tracked.fetch_leaderboard_text("u", byte_cap=50, get=lambda url, **kw: R()), "x" * 10 + "y" * 10)


class _ClientDown:
    """InfoClient stand-in: construction works, every call fails."""

    def __init__(self, *a, **kw):
        pass

    def __getattr__(self, name):
        raise RuntimeError(f"no network in tests ({name})")


class _FakeTime:
    """extract.time stand-in: a settable clock; everything else is the real module."""

    def __init__(self, t):
        self.t = t

    def time(self):
        return self.t

    def __getattr__(self, name):
        return getattr(time, name)


class _FakeClient:
    """InfoClient stand-in with empty-but-valid payloads. `fills[wallet]` is what the
    fills pull returns (a non-empty list is reported as API-capped); every fills pull
    advances the fake clock by `bump` seconds so a deadline can fall between wallets."""
    fills: dict = {}
    clock = None
    bump = 0.0
    _EMPTY_CHS = {"marginSummary": {"accountValue": "0", "totalNtlPos": "0", "totalRawUsd": "0", "totalMarginUsed": "0"},
                  "crossMarginSummary": {"accountValue": "0", "totalNtlPos": "0", "totalRawUsd": "0", "totalMarginUsed": "0"},
                  "withdrawable": "0", "assetPositions": [], "time": 0}

    def __init__(self, *a, **kw):
        pass

    def spot_meta_and_asset_ctxs(self):
        raise RuntimeError("no spot meta in tests")        # run() falls back to the empty resolver

    def user_fills_by_time(self, wallet, start_ms, end_ms):
        if self.clock is not None:
            self.clock.t += self.bump
        return list(self.fills.get(wallet, []))

    def user_fills_saturated(self, fills, start_ms, **kw):
        return bool(fills)

    def user_twap_slice_fills_by_time(self, wallet, start_ms, end_ms):
        return []

    def user_funding(self, wallet, start_ms, end_ms):
        return []

    def user_non_funding_ledger_updates(self, wallet, start_ms, end_ms):
        return []

    def clearinghouse_state(self, wallet, dex=None):
        return json.loads(json.dumps(self._EMPTY_CHS))

    def spot_clearinghouse_state(self, wallet):
        return {"balances": []}

    def delegator_summary(self, wallet):
        return {"delegated": "0", "undelegated": "0", "totalPendingWithdrawal": "0", "nPendingWithdrawals": 0}

    def portfolio(self, wallet):
        return []

    def user_vault_equities(self, wallet):
        return []

    def open_orders(self, wallet):
        return []


class SoftDeadlineTests(unittest.TestCase):
    def test_deadline_in_the_past_defers_everything_and_touches_nothing(self):
        with tempfile.TemporaryDirectory() as d, mock.patch.object(extract, "InfoClient", _ClientDown):
            out = Path(d)
            ns = out / S.NS_HYPERCORE
            ns.mkdir(parents=True)
            today = dt.datetime.now(tz=dt.timezone.utc)
            row = {c: "" for c in S.L4X_PERP_POSITION_SNAPSHOT_COLUMNS}
            row.update(as_of_date=today.strftime("%Y-%m-%d"), as_of_ts=today.isoformat(), wallet_address=W1,
                       coin="BTC", unrealized_pnl_usd="0")
            extract.upsert_csv(ns / "l4x_perp_position_snapshot.csv", S.L4X_PERP_POSITION_SNAPSHOT_COLUMNS, [row],
                               key_fn=lambda r: (r["as_of_date"], r["wallet_address"], r["coin"]),
                               sort_fn=lambda r: (r["as_of_date"], r["wallet_address"], r["coin"]))
            wm = {W1: {"through_ms": 123, "coverage_start_ms": 1, "fills_truncated": False}}
            (ns / "_watermarks.json").write_text(json.dumps(wm), encoding="utf-8")

            recon = extract.run([W1, W2], 2, out, incremental=True, hip3=False, deadline_ts=time.time() - 1)

            self.assertEqual(recon["deferred"], [W1, W2])
            self.assertEqual(recon["wallets"], [])
            self.assertEqual(recon["by_wallet"], {})
            self.assertEqual(json.loads((ns / "_watermarks.json").read_text(encoding="utf-8")), wm)
            with (ns / "l4x_perp_position_snapshot.csv").open(newline="", encoding="utf-8") as fh:
                rows = list(csv.DictReader(fh))
            self.assertEqual([(r["wallet_address"], r["coin"]) for r in rows], [(W1, "BTC")])

            summary = ops.build_summary(recon, [W1, W2], ops.utcnow(), ops.utcnow(), deferred=recon["deferred"])
            self.assertTrue(summary["ok"])
            self.assertEqual(summary["problems"], [])
            self.assertEqual(summary["warnings"], ["deferred=2"])
            self.assertEqual((summary["wallets_deferred"], summary["wallets_ok"]), (2, 0))
            self.assertEqual(summary["per_wallet"][W1], {"status": "deferred"})

    def test_no_deadline_keeps_the_old_behaviour(self):
        with tempfile.TemporaryDirectory() as d, mock.patch.object(extract, "InfoClient", _ClientDown):
            recon = extract.run([], 2, Path(d), hip3=False)
            self.assertEqual((recon["wallets"], recon["deferred"]), ([], []))

    def test_partial_run_defers_the_tail_and_accepts_the_hole_of_a_soft_wallet(self):
        T0 = 1_760_000_000.0                           # fixed clock: the test never straddles midnight
        fill_t = int((T0 - 3600) * 1000)
        clock = _FakeTime(T0)
        fake = _FakeClient
        fake.clock, fake.bump = clock, 700.0          # each fills pull costs 700 s of fake time
        fake.fills = {W1: [{"coin": "BTC", "px": "100", "sz": "1", "side": "B", "time": fill_t,
                            "startPosition": "0", "dir": "Close Long", "closedPnl": "1.5",
                            "hash": "0x" + "ab" * 32, "oid": 1, "crossed": True, "fee": "0.1",
                            "tid": 1, "feeToken": "USDC", "twapId": None}]}
        with tempfile.TemporaryDirectory() as d, mock.patch.object(extract, "InfoClient", fake), \
                mock.patch.object(extract, "time", clock):
            out = Path(d)
            ns = out / S.NS_HYPERCORE
            ns.mkdir(parents=True)
            day = dt.datetime.fromtimestamp(T0, tz=dt.timezone.utc)
            rows = []
            for w in (W1, W2):
                row = {c: "" for c in S.L4X_PERP_POSITION_SNAPSHOT_COLUMNS}
                row.update(as_of_date=day.strftime("%Y-%m-%d"), as_of_ts=day.isoformat(), wallet_address=w,
                           coin="ETH", unrealized_pnl_usd="0")
                rows.append(row)
            extract.upsert_csv(ns / "l4x_perp_position_snapshot.csv", S.L4X_PERP_POSITION_SNAPSHOT_COLUMNS, rows,
                               key_fn=lambda r: (r["as_of_date"], r["wallet_address"], r["coin"]),
                               sort_fn=lambda r: (r["as_of_date"], r["wallet_address"], r["coin"]))
            (ns / "_watermarks.json").write_text(json.dumps({W2: {"through_ms": 5, "coverage_start_ms": 1}}),
                                                 encoding="utf-8")

            recon = extract.run([W1, W2], 2, out, incremental=True, hip3=False,
                                deadline_ts=T0 + 600, soft_wallets={W1, W2})

            self.assertEqual((recon["wallets"], recon["deferred"]), ([W1], [W2]))
            r1 = recon["by_wallet"][W1]
            self.assertTrue(r1["fills_truncated_at_10k_cap"])
            self.assertTrue(r1["hole_accepted"])
            self.assertEqual(r1["coverage_start_ms"], fill_t)           # coverage restarts at the oldest fill
            wm = json.loads((ns / "_watermarks.json").read_text(encoding="utf-8"))
            self.assertEqual(wm[W1]["through_ms"], int(T0 * 1000))     # advanced despite the cap (soft wallet)
            self.assertTrue(wm[W1]["fills_truncated"])
            self.assertEqual(wm[W2], {"through_ms": 5, "coverage_start_ms": 1})   # deferred: untouched
            with (ns / "l4x_perp_position_snapshot.csv").open(newline="", encoding="utf-8") as fh:
                left = [(r["wallet_address"], r["coin"]) for r in csv.DictReader(fh)]
            self.assertEqual(left, [(W2, "ETH")])      # W1's stale same-day row replaced, W2's kept
            summary = ops.build_summary(recon, [W1, W2], ops.utcnow(), ops.utcnow(), deferred=recon["deferred"],
                                        soft_wallets={W1, W2})
            # the fake client has no portfolio, so the perp oracle cannot be read (a hard
            # problem by design); the cap itself must be a warning for a soft wallet
            self.assertEqual(summary["problems"], [f"{W1[:10]}:perp_oracle"])
            self.assertEqual(summary["warnings"], [f"{W1[:10]}:fills_truncated", "deferred=1"])
            self.assertEqual(summary["per_wallet"][W2], {"status": "deferred"})

    def test_pinned_wallet_keeps_the_repair_semantics_at_the_cap(self):
        T0 = 1_760_000_000.0
        clock = _FakeTime(T0)
        fake = _FakeClient
        fake.clock, fake.bump = clock, 0.0
        fake.fills = {W1: [{"coin": "BTC", "px": "100", "sz": "1", "side": "B", "time": int((T0 - 3600) * 1000),
                            "startPosition": "0", "dir": "Close Long", "closedPnl": "1.5",
                            "hash": "0x" + "ab" * 32, "oid": 1, "crossed": True, "fee": "0.1",
                            "tid": 1, "feeToken": "USDC", "twapId": None}]}
        with tempfile.TemporaryDirectory() as d, mock.patch.object(extract, "InfoClient", fake), \
                mock.patch.object(extract, "time", clock):
            recon = extract.run([W1], 2, Path(d), incremental=True, hip3=False, soft_wallets=set())
            wm = json.loads((Path(d) / S.NS_HYPERCORE / "_watermarks.json").read_text(encoding="utf-8"))
            self.assertIsNone(wm[W1]["through_ms"])                    # held for an S3 repair
            self.assertFalse(recon["by_wallet"][W1]["hole_accepted"])


class ReferentialGuardTests(unittest.TestCase):
    FILL = {"coin": "BTC", "px": "100", "sz": "1", "side": "B", "time": 1_760_000_000_000 - 3_600_000,
            "startPosition": "0", "dir": "Close Long", "closedPnl": "1.5", "hash": "0x" + "ab" * 32,
            "oid": 1, "crossed": True, "fee": "0.1", "tid": 1, "feeToken": "USDC", "twapId": None}

    def _fake(self):
        fake = _FakeClient
        fake.clock, fake.bump, fake.fills = _FakeTime(1_760_000_000.0), 0.0, {W1: [dict(self.FILL)]}
        return fake

    def test_duplicate_wallets_are_pulled_once_and_recon_reports_referential_ok(self):
        fake = self._fake()
        with tempfile.TemporaryDirectory() as d, mock.patch.object(extract, "InfoClient", fake), \
                mock.patch.object(extract, "time", fake.clock):
            recon = extract.run([W1, W1.upper()], 2, Path(d), hip3=False)
        self.assertEqual(recon["wallets"], [W1])
        self.assertTrue(recon["referential_ok"])
        self.assertTrue(recon["dimension_keys_distinct"])
        self.assertEqual(recon["ledger_tokens_without_metadata"], [])
        self.assertLess(recon["stamp_age_min"], 1.0)

    def test_run_fails_when_a_ledger_token_has_no_metadata_row(self):
        from blueblocx_hyperliquid import prices
        fake = self._fake()
        real = prices.token_metadata_rows

        def drop_usdc(items, now):
            return real({k: v for k, v in items.items() if k != S.USDC_ADDRESS}, now)
        with tempfile.TemporaryDirectory() as d, mock.patch.object(extract, "InfoClient", fake), \
                mock.patch.object(extract, "time", fake.clock), \
                mock.patch.object(extract.prices, "token_metadata_rows", side_effect=drop_usdc):
            with self.assertRaises(RuntimeError) as cm:
                extract.run([W1], 2, Path(d), hip3=False)
            self.assertIn("referential violation", str(cm.exception))
            ns = Path(d) / S.NS_HYPERCORE
            recon = json.loads((ns / "_reconciliation.json").read_text(encoding="utf-8"))
            self.assertFalse(recon["referential_ok"])
            self.assertEqual(recon["ledger_tokens_without_metadata"], [S.USDC_ADDRESS])
            self.assertFalse((ns / "l1_wallet_ledger.csv").exists())          # nothing staged


class SummaryTests(unittest.TestCase):
    def test_truncation_is_a_warning_for_auto_tracked_wallets_only(self):
        recon = _clean_recon([P1, W1])
        for w in (P1, W1):
            recon["by_wallet"][w]["fills_truncated_at_10k_cap"] = True
        summary = ops.build_summary(recon, [P1, W1], ops.utcnow(), ops.utcnow(), soft_wallets={W1})
        self.assertEqual(summary["problems"], [f"{P1[:10]}:fills_truncated"])
        self.assertEqual(summary["warnings"], [f"{W1[:10]}:fills_truncated"])
        self.assertEqual(summary["per_wallet"][W1]["status"], "ok")
        self.assertEqual(summary["per_wallet"][P1]["status"], "problem")

    def test_networth_mismatch_is_pending_when_the_lending_lookup_was_deferred(self):
        recon = _clean_recon([P1, W1])
        for w in (P1, W1):
            recon["by_wallet"][w]["oracle"]["networth_matches"] = False
        recon["by_wallet"][W1]["lending_deferred"] = True
        summary = ops.build_summary(recon, [P1, W1], ops.utcnow(), ops.utcnow())
        self.assertEqual(summary["problems"], [f"{P1[:10]}:networth_oracle"])
        self.assertEqual(summary["warnings"], [f"{W1[:10]}:networth_pending"])
        self.assertEqual(summary["per_wallet"][W1]["status"], "ok")

    def test_deferred_wallet_with_a_stale_entry_is_not_rejudged(self):
        recon = _clean_recon([W1], deferred=[W1])
        recon["by_wallet"][W1]["fidelity_ok"] = False       # stale, from an earlier run
        summary = ops.build_summary(recon, [W1], ops.utcnow(), ops.utcnow(), deferred=[W1])
        self.assertTrue(summary["ok"])
        self.assertEqual(summary["per_wallet"][W1], {"status": "deferred"})


class DailyWithTrackedSourceTests(unittest.TestCase):
    SETS = {"whales": [W1, P1], "smart": [S1]}

    def _run(self, d, *, pins, meta, recon_for=None, deferred=(), truncated=(), raises=None, env=None, extra=(),
             pins_file=True, sets=None):
        out = Path(d) / "out"
        wf = Path(d) / "wallets.txt"
        if pins_file:
            wf.write_text("\n".join(pins) + "\n", encoding="utf-8")
        captured = {}

        def fake_run(wallets, days, out_dir, **kw):
            captured.update(wallets=wallets, days=days, out_dir=out_dir, **kw)
            if raises:
                raise raises
            return _clean_recon(recon_for if recon_for is not None else wallets, deferred=deferred,
                                truncated=truncated)

        environ = {"HL_TRACKED_SOURCE": "leaderboard", "HL_RUN_SOFT_DEADLINE_MIN": "0"}
        environ.update(env or {})
        sets = self.SETS if sets is None else sets
        with mock.patch.dict(os.environ, environ, clear=False), \
                mock.patch.object(daily.extract, "run", side_effect=fake_run), \
                mock.patch.object(daily.tracked, "refresh_tracked", return_value=(sets, dict(meta))) as refresh:
            code = daily.main(["--wallets-file", str(wf), "--out", str(out), *extra])
        summary_path = out / "hypercore" / ops.SUMMARY_FILE
        summary = json.loads(summary_path.read_text(encoding="utf-8")) if summary_path.exists() else None
        return code, summary, captured, out, refresh

    def test_pins_then_smart_then_whales_and_cursor_saved(self):
        with tempfile.TemporaryDirectory() as d:
            code, summary, captured, out, refresh = self._run(d, pins=[P1], meta={"source": "fresh"})
            self.assertEqual(code, 0)
            self.assertEqual(captured["wallets"], [P1, S1, W1])
            self.assertIsNone(captured["deadline_ts"])
            self.assertEqual(refresh.call_count, 1)
            self.assertEqual(summary["tracked"], {"source": "fresh", "pins": 1, "auto": 2, "planned": 3, "cursor": 0})
            self.assertTrue(summary["ok"])
            self.assertEqual(tracked.load_cursor(out / "hypercore"), 0)      # all processed -> unchanged

    def test_deferred_tail_moves_the_cursor_and_is_only_a_warning(self):
        with tempfile.TemporaryDirectory() as d:
            # extract.run processed P1 and S1 and deferred W1 (soft deadline)
            code, summary, captured, out, _ = self._run(d, pins=[P1], meta={"source": "fresh"},
                                                        recon_for=[P1, S1], deferred=[W1])
            self.assertEqual(code, 0)
            self.assertTrue(summary["ok"])
            self.assertEqual(summary["warnings"], ["deferred=1"])
            self.assertEqual(summary["per_wallet"][W1], {"status": "deferred"})
            self.assertEqual(summary["wallets_deferred"], 1)
            self.assertEqual(tracked.load_cursor(out / "hypercore"), 1)     # next run starts at W1

    def test_wallet_not_reported_and_not_deferred_is_still_missing(self):
        with tempfile.TemporaryDirectory() as d:
            code, summary, _, _, _ = self._run(d, pins=[P1], meta={"source": "fresh"}, recon_for=[P1, S1])
            self.assertEqual(code, 1)
            self.assertEqual(summary["problems"], [f"{W1[:10]}:missing"])

    def test_deadline_option_is_passed_as_epoch_seconds(self):
        with tempfile.TemporaryDirectory() as d:
            t0 = time.time()
            code, _, captured, _, _ = self._run(d, pins=[P1], meta={"source": "fresh"}, extra=["--deadline-min", "35"])
            self.assertEqual(code, 0)
            self.assertAlmostEqual(captured["deadline_ts"], t0 + 35 * 60, delta=30)
        with tempfile.TemporaryDirectory() as d:
            _, _, captured, _, _ = self._run(d, pins=[P1], meta={"source": "fresh"}, env={"HL_RUN_SOFT_DEADLINE_MIN": "20"})
            self.assertAlmostEqual(captured["deadline_ts"], time.time() + 20 * 60, delta=30)

    def test_no_pins_file_is_fine_with_a_tracked_source(self):
        with tempfile.TemporaryDirectory() as d:
            code, summary, captured, _, _ = self._run(d, pins=[], meta={"source": "fresh"}, pins_file=False)
            self.assertEqual(code, 0)
            self.assertEqual(captured["wallets"], [S1, W1, P1])
            self.assertEqual(summary["tracked"]["pins"], 0)

    def test_cached_set_warns_and_unavailable_set_is_a_problem(self):
        with tempfile.TemporaryDirectory() as d:
            code, summary, _, _, _ = self._run(d, pins=[P1], meta={"source": "cache", "age_h": 3.5, "error": "x"})
            self.assertEqual(code, 0)
            self.assertIn("tracked_stale=3.5h", summary["warnings"])
        empty = {"whales": [], "smart": []}
        with tempfile.TemporaryDirectory() as d:
            code, summary, captured, _, _ = self._run(d, pins=[P1], meta={"source": "none", "error": "down"}, sets=empty)
            self.assertEqual(code, 1)
            self.assertEqual(captured["wallets"], [P1])
            self.assertIn("tracked_unavailable", summary["problems"])
            self.assertFalse(summary["ok"])
        with tempfile.TemporaryDirectory() as d:
            code, summary, captured, _, _ = self._run(d, pins=[], meta={"source": "none", "error": "down"}, sets=empty)
            self.assertEqual(code, 1)
            self.assertNotIn("wallets", captured)                      # never ran
            self.assertTrue(summary["problems"][0].startswith("error:RuntimeError: tracked set unavailable"))

    def test_soft_wallets_are_the_auto_part_and_only_a_pin_fails_on_truncation(self):
        with tempfile.TemporaryDirectory() as d:
            code, summary, captured, _, _ = self._run(d, pins=[P1], meta={"source": "fresh"}, truncated=[P1, S1])
            self.assertEqual(captured["soft_wallets"], {S1, W1})
            self.assertEqual(code, 1)
            self.assertEqual(summary["problems"], [f"{P1[:10]}:fills_truncated"])
            self.assertEqual(summary["warnings"], [f"{S1[:10]}:fills_truncated"])

    def test_cursor_is_not_saved_when_the_run_raises(self):
        with tempfile.TemporaryDirectory() as d:
            tracked.save_cursor(Path(d) / "out" / "hypercore", 1)
            code, summary, _, out, _ = self._run(d, pins=[P1], meta={"source": "fresh"}, raises=RuntimeError("boom"))
            self.assertEqual(code, 1)
            self.assertTrue(summary["problems"][0].startswith("error:RuntimeError: boom"))
            self.assertEqual(tracked.load_cursor(out / "hypercore"), 1)

    def test_unknown_source_is_a_config_error(self):
        with tempfile.TemporaryDirectory() as d, mock.patch.dict(os.environ, {"HL_TRACKED_SOURCE": "csv"}):
            self.assertEqual(daily.main(["--wallets-file", str(Path(d) / "w.txt"), "--out", d]), 2)


class PreflightTrackedTests(unittest.TestCase):
    def test_wallets_check_wording_follows_the_source(self):
        with tempfile.TemporaryDirectory() as d:
            empty = Path(d) / "w.txt"
            empty.write_text("# nothing\n", encoding="utf-8")
            with mock.patch.dict(os.environ, {"HL_TRACKED_SOURCE": ""}):
                self.assertEqual(preflight.check_wallets(None).status, "SKIP")
                self.assertEqual(preflight.check_wallets(Path(d) / "missing.txt").status, "FAIL")
                self.assertEqual(preflight.check_wallets(empty).status, "WARN")
            with mock.patch.dict(os.environ, {"HL_TRACKED_SOURCE": "leaderboard"}):
                self.assertEqual(preflight.check_wallets(None).status, "OK")
                self.assertEqual(preflight.check_wallets(Path(d) / "missing.txt").status, "WARN")
                self.assertEqual(preflight.check_wallets(empty).status, "OK")

    def test_tracked_check_offline_and_misconfigured(self):
        with tempfile.TemporaryDirectory() as d:
            with mock.patch.dict(os.environ, {"HL_TRACKED_SOURCE": ""}):
                self.assertEqual(preflight.check_tracked(offline=True, out_dir=Path(d)).status, "SKIP")
            with mock.patch.dict(os.environ, {"HL_TRACKED_SOURCE": "csv"}):
                self.assertEqual(preflight.check_tracked(offline=True, out_dir=Path(d)).status, "FAIL")
            with mock.patch.dict(os.environ, {"HL_TRACKED_SOURCE": "leaderboard", "HL_WHALE_TOP_N": "ten"}):
                self.assertEqual(preflight.check_tracked(offline=True, out_dir=Path(d)).status, "FAIL")
            with mock.patch.dict(os.environ, {"HL_TRACKED_SOURCE": "leaderboard"}):
                tracked.save_cached(Path(d) / S.NS_HYPERCORE, {"fetched_at_ts": time.time() - 7200,
                                                                "whales": [W1] * 3, "smart": [S1]})
                c = preflight.check_tracked(offline=True, out_dir=Path(d))
                self.assertEqual(c.status, "SKIP")
                self.assertIn("cache: 3 whales + 1 smart, 2.0 h old", c.detail)

    def test_tracked_check_online_never_fails_on_the_stats_host(self):
        class Head:
            def __init__(self, body, status=200):
                self.body, self.status = body, status

            def raise_for_status(self):
                if self.status >= 400:
                    raise RuntimeError(f"HTTP {self.status}")

            def iter_content(self, n):
                yield self.body

            def close(self):
                pass

        def boom(url, **kw):
            raise ConnectionError("down")
        with tempfile.TemporaryDirectory() as d, mock.patch.dict(os.environ, {"HL_TRACKED_SOURCE": "leaderboard"}):
            out = Path(d)
            c = preflight.check_tracked(offline=False, out_dir=out, get=lambda url, **kw: Head(b'{\n  "leaderboardRows": [\n'))
            self.assertEqual(c.status, "OK")
            c = preflight.check_tracked(offline=False, out_dir=out, get=boom)
            self.assertEqual(c.status, "WARN")
            self.assertIn("tracked_unavailable", c.detail)
            tracked.save_cached(out / S.NS_HYPERCORE, {"fetched_at_ts": time.time() - 3600, "whales": [W1], "smart": []})
            c = preflight.check_tracked(offline=False, out_dir=out, get=boom)
            self.assertEqual(c.status, "WARN")
            self.assertIn("tracked_stale", c.detail)
            c = preflight.check_tracked(offline=False, out_dir=out, get=lambda url, **kw: Head(b"<html>portal</html>"))
            self.assertEqual(c.status, "WARN")
            c = preflight.check_tracked(offline=False, out_dir=out, get=lambda url, **kw: Head(b"", status=503))
            self.assertEqual(c.status, "WARN")

    def test_soft_deadline_env_check(self):
        def status(val):
            with mock.patch.dict(os.environ, {"HL_RUN_SOFT_DEADLINE_MIN": val}):
                return {c.name: c for c in preflight.check_env(None)}["soft_deadline"].status
        self.assertEqual(status("0"), "OK")
        self.assertEqual(status("35"), "OK")
        self.assertEqual(status("50"), "WARN")
        self.assertEqual(status("-1"), "FAIL")
        self.assertEqual(status("soon"), "FAIL")


if __name__ == "__main__":
    unittest.main()
