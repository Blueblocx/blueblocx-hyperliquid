"""v1.6.0: l4_wallet_metadata over the accumulated history (wallet_meta), content-change
stamping of dimension rows (extract.content_merge + upsert_csv merge_fn), stable token
first_seen (TokenRegistry, token_metadata_rows, maintenance --fix-first-seen). No network."""
from __future__ import annotations

import csv
import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

from blueblocx_hyperliquid import extract, maintenance, mappers, prices, wallet_meta
from blueblocx_hyperliquid import schema as S
from tests.test_tracked import W1, _FakeClient, _FakeTime

H = 3_600_000
T0 = 1_760_000_000_000                    # 2025-10-09T08:53:20Z (ms); the day never straddles in these tests
NOW = "2026-10-12T01:00:00+00:00"


def _row(ts_ms: int, event_type: str = S.EVT_SPOT_FILL, **extra) -> dict:
    r = {"block_timestamp": wallet_meta._iso(ts_ms), "block_date": wallet_meta._day(ts_ms),
         "event_type": event_type, "tx_hash": f"h{ts_ms}", "event_index": 0, "direction": "in"}
    r.update(extra)
    return r


class WalletMetaStateTests(unittest.TestCase):
    def test_seed_counts_token_movements_only_and_distinct_utc_days(self):
        # v1.10.9: perp cashflow (fee here) and funding are not activity; only the two spot
        # fills count, and latest_tx is the latest token movement, not the later fee row
        rows = [_row(T0 - 30 * H), _row(T0 - 30 * H, S.EVT_FUNDING), _row(T0 - 2 * H), _row(T0 - H, S.EVT_PERP_FEE),
                _row(T0 - 40 * H, S.EVT_FUNDING)]                           # funding only earlier: not "first"
        st = wallet_meta.seed(rows, T0)
        self.assertEqual((st["count_tx"], st["first_ms"], st["latest_ms"], st["through_ms"]), (2, T0 - 30 * H, T0 - 2 * H, T0))
        self.assertEqual(len(st["days"]), 2)
        self.assertEqual(wallet_meta.seed([_row(T0, S.EVT_FUNDING)], T0)["count_tx"], 0)
        for evt in (S.EVT_PERP_PNL, S.EVT_PERP_FEE, S.EVT_STAKING_REWARD, S.EVT_INTEREST):
            self.assertEqual(wallet_meta.seed([_row(T0, evt)], T0)["count_tx"], 0, evt)
        for evt in (S.EVT_SPOT_FILL, S.EVT_TRANSFER, S.EVT_DEPOSIT, S.EVT_WITHDRAW):
            self.assertEqual(wallet_meta.seed([_row(T0, evt)], T0)["count_tx"], 1, evt)

    def test_update_folds_only_the_advanced_window_and_is_idempotent(self):
        st = wallet_meta.seed([_row(T0 - 2 * H)], T0)
        rows2 = [_row(T0 - 2 * H),                     # re-pulled overlap row: already counted
                 _row(T0),                             # exactly at since: counted by the previous window
                 _row(T0 + H // 2),                    # inside (since, now]
                 _row(T0 + 2 * H)]                     # beyond now: next window
        st2 = wallet_meta.update(st, rows2, since_ms=T0, now_ms=T0 + H)
        self.assertEqual((st2["count_tx"], st2["latest_ms"], st2["through_ms"]), (2, T0 + H // 2, T0 + H))
        self.assertEqual(st["count_tx"], 1)                                  # a copy, not in place
        self.assertEqual(wallet_meta.update(st2, rows2, T0, T0 + H), st2)   # same window again: no-op
        self.assertEqual(wallet_meta.update(st2, rows2, st2["through_ms"], T0 + H), st2)

    def test_advance_state_is_bound_to_the_watermark_rule(self):
        rows = [_row(T0 - 2 * H), _row(T0 - H // 2)]
        # no state, window capped, nothing covered before: stay without state
        self.assertIsNone(wallet_meta.advance_state(None, rows, None, T0, advance=False))
        # no state, window capped, prior watermark: seed only up to the prior watermark
        st = wallet_meta.advance_state(None, rows, T0 - H, T0, advance=False)
        self.assertEqual((st["count_tx"], st["through_ms"]), (1, T0 - H))
        # no state, complete window: seed up to now
        st = wallet_meta.advance_state(None, rows, None, T0, advance=True)
        self.assertEqual((st["count_tx"], st["through_ms"]), (2, T0))
        # a state and a capped window: kept as is (the window is re-pulled next run)
        self.assertEqual(wallet_meta.advance_state(st, [_row(T0 + H // 2)], T0, T0 + H, advance=False), st)
        # a state and a complete window: fold (prev.through, now]
        st2 = wallet_meta.advance_state(st, rows + [_row(T0 + H // 2)], T0, T0 + H, advance=True)
        self.assertEqual((st2["count_tx"], st2["through_ms"]), (3, T0 + H))

    def test_row_semantics(self):
        st = wallet_meta.seed([_row(T0 - 2 * H), _row(T0 - 50 * H), _row(T0 - 50 * H, S.EVT_FUNDING)], T0)
        row = wallet_meta.row(W1.upper(), st, T0, NOW)
        self.assertEqual(row["wallet_address"], W1)
        self.assertEqual(row["first_tx"], wallet_meta._iso(T0 - 50 * H))
        self.assertEqual(row["first_tx"], row["first_block_timestamp"])
        self.assertEqual(row["latest_tx"], wallet_meta._iso(T0 - 2 * H))
        self.assertEqual(row["latest_tx"], row["latest_block_timestamp"])
        self.assertEqual((row["count_tx"], row["active_days"], row["last_active"]), (2, 2, 0))
        self.assertIsNone(row["active_on_avg"])
        self.assertEqual(row["updated_at"], NOW)
        self.assertEqual(list(row), S.L4_WALLET_METADATA_COLUMNS)
        # last_active counts whole UTC days since the latest event
        st = wallet_meta.seed([_row(T0 - 26 * H)], T0)
        self.assertEqual(wallet_meta.row(W1, st, T0, NOW)["last_active"], 1)
        self.assertEqual(wallet_meta.row(W1, st, T0 + 24 * H, NOW)["last_active"], 2)
        # nothing covered / funding only: no row
        self.assertIsNone(wallet_meta.row(W1, None, T0, NOW))
        self.assertIsNone(wallet_meta.row(W1, wallet_meta.seed([_row(T0, S.EVT_FUNDING)], T0), T0, NOW))


class ContentMergeTests(unittest.TestCase):
    OLD = {"token_address": "hlcore:usdc", "token_group": "7", "is_active": "True", "whitelist_reason": "hypercore_token",
           "created_at": "2026-10-01T00:00:00+00:00", "updated_at": "2026-10-01T00:00:00+00:00"}
    NEW = {"token_address": "hlcore:usdc", "token_group": 7, "is_active": True, "whitelist_reason": "hypercore_token",
           "created_at": NOW, "updated_at": NOW}

    def test_unchanged_content_keeps_the_existing_row_and_stamp(self):
        merge = extract.content_merge(S.TOKEN_WHITELIST_COLUMNS)
        self.assertIs(merge(self.OLD, self.NEW), self.OLD)                 # typed vs string: same content
        out = merge(self.OLD, dict(self.NEW, whitelist_reason="changed"))
        self.assertEqual((out["whitelist_reason"], out["updated_at"]), ("changed", NOW))
        self.assertEqual(out["created_at"], self.OLD["created_at"])        # first ingestion preserved
        out = merge(dict(self.OLD, created_at=""), dict(self.NEW, is_active=False))
        self.assertEqual(out["created_at"], NOW)                           # nothing to preserve

    def test_min_cols_keep_the_earliest_first_seen(self):
        merge = extract.content_merge(S.L1_TOKEN_METADATA_COLUMNS, min_cols=("first_seen",))
        old = prices.token_metadata_rows({"a": {"symbol": "A", "decimals": 5, "is_stablecoin": False}},
                                         "2026-10-01T00:00:00+00:00")[0]
        old = {c: ("" if old.get(c) is None else str(old[c])) for c in S.L1_TOKEN_METADATA_COLUMNS}  # as staged
        new = prices.token_metadata_rows({"a": {"symbol": "A", "decimals": 5, "is_stablecoin": False}}, NOW)[0]
        self.assertIs(merge(old, new), old)                                # later first_seen: nothing changed
        earlier = prices.token_metadata_rows({"a": {"symbol": "A", "decimals": 5, "is_stablecoin": False,
                                                    "first_seen_ms": T0}}, NOW)[0]
        out = merge(old, earlier)
        self.assertEqual((out["first_seen"], out["updated_at"], out["created_at"]),
                         (wallet_meta._iso(T0), NOW, "2026-10-01T00:00:00+00:00"))
        self.assertEqual(merge(dict(old, first_seen=""), new)["first_seen"], NOW)

    def test_upsert_csv_with_merge_fn_end_to_end(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "wallet_watchlist.csv"
            cols = S.WALLET_WATCHLIST_COLUMNS
            merge = extract.content_merge(cols)
            kw = dict(key_fn=lambda r: (extract._s(r["wallet_address"]),), merge_fn=merge)
            base = {"wallet_address": W1, "wallet_group": S.addr_group(W1), "is_active": True,
                    "whitelist_reason": "hypercore_ingest"}
            extract.upsert_csv(p, cols, [dict(base, created_at="s1", updated_at="s1")], **kw)
            extract.upsert_csv(p, cols, [dict(base, created_at="s2", updated_at="s2")], **kw)
            with p.open(newline="", encoding="utf-8") as fh:
                row = list(csv.DictReader(fh))[0]
            self.assertEqual((row["created_at"], row["updated_at"]), ("s1", "s1"))
            extract.upsert_csv(p, cols, [dict(base, is_active=False, created_at="s3", updated_at="s3")], **kw)
            with p.open(newline="", encoding="utf-8") as fh:
                row = list(csv.DictReader(fh))[0]
            self.assertEqual((row["is_active"], row["created_at"], row["updated_at"]), ("False", "s1", "s3"))


class TokenFirstSeenTests(unittest.TestCase):
    def test_registry_keeps_the_earliest_seen_ms(self):
        reg = mappers.TokenRegistry()
        reg.note("a", "A", 5, False, seen_ms=200)
        reg.note("a", "A", 5, False, seen_ms=100)
        reg.note("a", "A", 5, False)
        reg.note("a", "A", 5, False, seen_ms=300)
        reg.note("b", "B", 0, False)
        reg.note("b", "B", 6, False, seen_ms=50)                            # decimals learnt later: first_seen kept
        items = reg.items()
        self.assertEqual((items["a"]["first_seen_ms"], items["a"]["decimals"]), (100, 5))
        self.assertEqual((items["b"]["first_seen_ms"], items["b"]["decimals"]), (50, 6))
        items["a"]["symbol"] = "mutated"
        self.assertEqual(reg.items()["a"]["symbol"], "A")                   # items() hands out copies

    def test_ledger_row_notes_the_block_timestamp(self):
        reg = mappers.TokenRegistry()
        mappers._ledger_row(block_timestamp_ms=T0 - H, tx_hash="h", event_index=0, event_type=S.EVT_SPOT_FILL,
                            wallet=W1, counterparty=None, token_address="hlcore:token:1", token_symbol="PURR",
                            decimals=5, amount=1.0, direction=S.DIRECTION_IN, now_iso=NOW, registry=reg)
        self.assertEqual(reg.items()["hlcore:token:1"]["first_seen_ms"], T0 - H)
        rows = prices.token_metadata_rows(reg.items(), NOW)
        self.assertEqual(rows[0]["first_seen"], wallet_meta._iso(T0 - H))
        self.assertEqual(prices.token_metadata_rows({"x": {"symbol": "X", "decimals": 1, "is_stablecoin": False}},
                                                    NOW)[0]["first_seen"], NOW)                # unknown: run stamp

    def test_fix_first_seen_moves_earlier_once_and_restamps(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            ns = root / S.NS_HYPERCORE
            ns.mkdir()
            ledger_rows = []
            for ts, addr in ((T0 - 50 * H, "hlcore:token:1"), (T0 - 2 * H, "hlcore:token:1"), (T0 - H, "hlcore:usdc")):
                r = {c: "" for c in S.L1_WALLET_LEDGER_COLUMNS}
                r.update(token_address=addr, block_timestamp=wallet_meta._iso(ts), tx_hash=f"h{ts}{addr}",
                         event_index="0", direction="in", wallet_address=W1)
                ledger_rows.append(r)
            with (ns / "l1_wallet_ledger.csv").open("w", newline="", encoding="utf-8") as fh:
                w = csv.DictWriter(fh, fieldnames=S.L1_WALLET_LEDGER_COLUMNS)
                w.writeheader()
                w.writerows(ledger_rows)
            meta = prices.token_metadata_rows({"hlcore:token:1": {"symbol": "PURR", "decimals": 5, "is_stablecoin": False,
                                                                  "first_seen_ms": T0 - 2 * H},
                                               "hlcore:usdc": {"symbol": "USDC", "decimals": 8, "is_stablecoin": True,
                                                               "first_seen_ms": T0 - 60 * H},
                                               "hlcore:token:9": {"symbol": "Z", "decimals": 0, "is_stablecoin": False}},
                                              "2026-10-01T00:00:00+00:00")
            with (ns / "l1_token_metadata.csv").open("w", newline="", encoding="utf-8") as fh:
                w = csv.DictWriter(fh, fieldnames=S.L1_TOKEN_METADATA_COLUMNS)
                w.writeheader()
                w.writerows([{c: ("" if r.get(c) is None else r[c]) for c in S.L1_TOKEN_METADATA_COLUMNS} for r in meta])
            rep = maintenance.fix_first_seen(ns, NOW)
            self.assertEqual((rep["tokens"], rep["changed"], rep["ledger_tokens"]), (3, 1, 2))
            with (ns / "l1_token_metadata.csv").open(newline="", encoding="utf-8") as fh:
                rows = {r["token_address"]: r for r in csv.DictReader(fh)}
            self.assertEqual(rows["hlcore:token:1"]["first_seen"], wallet_meta._iso(T0 - 50 * H))
            self.assertEqual(rows["hlcore:token:1"]["updated_at"], NOW)                        # re-stamped
            self.assertEqual(rows["hlcore:usdc"]["first_seen"], wallet_meta._iso(T0 - 60 * H))  # already earlier
            self.assertEqual(rows["hlcore:usdc"]["updated_at"], "2026-10-01T00:00:00+00:00")
            self.assertEqual(rows["hlcore:token:9"]["first_seen"], "2026-10-01T00:00:00+00:00")  # no ledger rows
            self.assertEqual(maintenance.fix_first_seen(ns, NOW)["changed"], 0)               # idempotent
            with redirect_stdout(io.StringIO()):
                self.assertEqual(maintenance.main(["--fix-first-seen", "--out", str(root)]), 0)
            self.assertEqual(maintenance.fix_first_seen(Path(d) / "nowhere", NOW), {"skipped": "no l1_token_metadata.csv"})


class ExtractRunStampingTests(unittest.TestCase):
    FILL = {"coin": "BTC", "px": "100", "sz": "1", "side": "B", "startPosition": "0", "dir": "Close Long",
            "closedPnl": "1.5", "hash": "0x" + "ab" * 32, "oid": 1, "crossed": True, "fee": "0.1",
            "feeToken": "USDC", "twapId": None}

    @staticmethod
    def _read(ns: Path, name: str) -> dict[str, dict]:
        key = "wallet_address" if "wallet" in name else "token_address"
        with (ns / name).open(newline="", encoding="utf-8") as fh:
            return {r[key]: r for r in csv.DictReader(fh)}

    def test_second_run_without_new_activity_keeps_stamps_and_the_aggregate_accumulates(self):
        T0s = T0 / 1000
        fill1 = dict(self.FILL, time=T0 - H, tid=1)
        clock = _FakeTime(T0s)
        fake = _FakeClient
        fake.clock, fake.bump, fake.fills = clock, 0.0, {W1: [fill1]}
        with tempfile.TemporaryDirectory() as d, mock.patch.object(extract, "InfoClient", fake), \
                mock.patch.object(extract, "time", clock):
            out = Path(d)
            ns = out / S.NS_HYPERCORE
            # run 1: the fake reports the window as API-capped; a soft wallet accepts the hole,
            # so the watermark (and the aggregate) advance
            extract.run([W1], 2, out, incremental=True, hip3=False, soft_wallets={W1})
            # v1.10.9: a perp fill books perp_realized_pnl + fee only - not token movements - so the
            # wallet has no l4_wallet_metadata row, but the aggregate still covers the window
            self.assertNotIn(W1, self._read(ns, "l4_wallet_metadata.csv"))
            wm1 = json.loads((ns / "_watermarks.json").read_text(encoding="utf-8"))[W1]
            self.assertEqual((wm1["meta"]["count_tx"], wm1["meta"]["through_ms"], wm1["through_ms"]), (0, T0, T0))
            watch1 = self._read(ns, "wallet_watchlist.csv")[W1]
            white1 = self._read(ns, "token_whitelist.csv")[S.USDC_ADDRESS]
            tok1 = self._read(ns, "l1_token_metadata.csv")[S.USDC_ADDRESS]
            self.assertEqual(tok1["first_seen"], wallet_meta._iso(T0 - H))  # the fill that referenced USDC

            # run 2, one hour later, same fill re-pulled: nothing changed -> every stamp kept
            clock.t = T0s + 3600
            extract.run([W1], 2, out, incremental=True, hip3=False, soft_wallets={W1})
            self.assertNotIn(W1, self._read(ns, "l4_wallet_metadata.csv"))
            wm2 = json.loads((ns / "_watermarks.json").read_text(encoding="utf-8"))[W1]
            self.assertEqual((wm2["meta"]["count_tx"], wm2["meta"]["through_ms"]), (0, T0 + H))
            self.assertEqual(self._read(ns, "wallet_watchlist.csv")[W1], watch1)
            self.assertEqual(self._read(ns, "token_whitelist.csv")[S.USDC_ADDRESS], white1)
            self.assertEqual(self._read(ns, "l1_token_metadata.csv")[S.USDC_ADDRESS], tok1)

            # run 3, a new fill inside the advanced window: the aggregate grows, the row is re-stamped
            clock.t = T0s + 7200
            fake.fills = {W1: [fill1, dict(self.FILL, time=T0 + H + H // 2, tid=2, hash="0x" + "cd" * 32, oid=2)]}
            extract.run([W1], 2, out, incremental=True, hip3=False, soft_wallets={W1})
            self.assertNotIn(W1, self._read(ns, "l4_wallet_metadata.csv"))              # still perp-only
            wm3 = json.loads((ns / "_watermarks.json").read_text(encoding="utf-8"))[W1]
            self.assertEqual((wm3["meta"]["count_tx"], wm3["meta"]["through_ms"]), (0, T0 + 2 * H))
            self.assertEqual(self._read(ns, "l1_token_metadata.csv")[S.USDC_ADDRESS], tok1)   # still unchanged


if __name__ == "__main__":
    unittest.main()
