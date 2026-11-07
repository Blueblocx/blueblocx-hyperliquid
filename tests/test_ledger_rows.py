"""extract.open_ledger_rows (v1.10.13): the analyse phase's per-wallet view of the staged
ledger. The DuckDB-backed store (csv_merge.WalletRowStore) must hand every consumer the
same rows, strings, types and order as the dict path (_load_ledger_rows_by_wallet), the
threshold must pick the path, and an incremental extract.run must produce the same
state and dimension files on either path."""
from __future__ import annotations

import csv
import datetime as dt
import json
import random
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from blueblocx_hyperliquid import analytics, csv_merge, extract, wallet_meta
from blueblocx_hyperliquid import schema as S
from tests.test_tracked import W1, W2, _FakeClient, _FakeTime
from tests.test_upsert_stream import _Env

A = "0x" + "aa" * 20
B = "0x" + "bb" * 20
C = "0x" + "cc" * 20          # asked for, no rows
D = "0x" + "99" * 20          # has rows, not asked for (distinct from test_tracked's W1/W2)
T0 = 1_760_000_000_000
H = 3_600_000


def _iso(ms: int) -> str:
    return dt.datetime.fromtimestamp(ms / 1000, tz=dt.timezone.utc).isoformat()


def _row(wallet, i, ts_ms, **over):
    r = {c: "" for c in S.L1_WALLET_LEDGER_COLUMNS}
    r.update(block_number=str(i), block_timestamp=_iso(ts_ms), block_date=_iso(ts_ms)[:10], tx_hash=f"0x{i:04x}",
             tx_index="0", event_index=str(i % 3), event_type="deposit", wallet_address=wallet, wallet_group="w",
             counterparty_address="", token_address=S.USDC_ADDRESS, token_group="t", direction="in", amount_raw=str(i),
             decimals="8", standard="hlcore", amount=str(i / 10), amount_signed=str(i / 10),
             created_at=_iso(T0), updated_at=_iso(T0))
    r.update(over)
    return r


def _write(path: Path, rows):
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=S.L1_WALLET_LEDGER_COLUMNS)
        w.writeheader()
        w.writerows(rows)


def _ledger_rows():
    rows = []
    for i in range(60):
        wallet = (A, B, D)[i % 3]
        rows.append(_row(wallet, i, T0 - (60 - i) * H))
    # awkward values: quoted comma, embedded newline, quote, unicode, empty amount / index,
    # leading / trailing blanks, a huge event_index
    rows.append(_row(A, 100, T0 - 2 * H, counterparty_address="x,y", amount="1,5"))
    rows.append(_row(A, 101, T0 - 2 * H, counterparty_address="l1\nl2", event_type="withdraw"))
    rows.append(_row(B, 102, T0 - 2 * H, counterparty_address='q"z', wallet_group=" lead/trail "))
    rows.append(_row(B, 103, T0 - 2 * H, counterparty_address="ü€ 漢", token_group=""))
    rows.append(_row(A, 104, T0 - H, amount_signed="", event_index=""))
    rows.append(_row(B, 105, T0 - H, event_index=str(10 ** 12), amount_signed="-0.25"))
    random.Random(7).shuffle(rows)                    # the file is NOT sorted: order = file order
    return rows


class WalletRowStoreParityTests(unittest.TestCase):
    def setUp(self):
        self.dir = Path(tempfile.mkdtemp(prefix="hl-ledger-rows-"))
        self.addCleanup(shutil.rmtree, self.dir, True)
        self.path = self.dir / "l1_wallet_ledger.csv"
        self.rows = _ledger_rows()
        _write(self.path, self.rows)
        self.wanted = {A, B, C}

    def test_duckdb_store_matches_the_dict_path_rows_strings_types_and_order(self):
        expect = extract._load_ledger_rows_by_wallet(self.path, self.wanted)
        with _Env(HL_UPSERT_STREAM_MIN_BYTES="0"), \
                self.assertLogs("blueblocx_hyperliquid.extract", level="INFO") as logs:
            src = extract.open_ledger_rows(self.path, self.wanted)
        self.assertEqual(src.kind, "duckdb")
        self.assertIn("DuckDB wallet store", "\n".join(logs.output))
        try:
            for w in (A, B, C):
                got = src.rows(w)
                self.assertEqual(got, expect[w], w)                       # same dicts, same order
                self.assertEqual([type(v) for r in got for v in r.values()],
                                 [type(v) for r in expect[w] for v in r.values()])
            self.assertEqual(len(src.rows(A)), 20 + 3)
            self.assertEqual(src.rows(C), [])                              # asked for, no rows
            self.assertEqual(src.rows(D), [])                              # not asked for
            self.assertEqual(src.rows("nope"), [])
            a = {r["tx_hash"]: r for r in src.rows(A)}
            self.assertEqual(a["0x0064"]["counterparty_address"], "x,y")
            self.assertEqual(a["0x0065"]["counterparty_address"], "l1\nl2")
            self.assertEqual(a["0x0068"]["amount_signed"], 0.0)            # '' -> float(0)
            self.assertEqual(a["0x0068"]["event_index"], 0)                # '' -> int(0)
            b = {r["tx_hash"]: r for r in src.rows(B)}
            self.assertEqual(b["0x0066"]["counterparty_address"], 'q"z')
            self.assertEqual(b["0x0066"]["wallet_group"], " lead/trail ")
            self.assertEqual(b["0x0067"]["counterparty_address"], "ü€ 漢")
            self.assertEqual(b["0x0069"]["event_index"], 10 ** 12)
            self.assertEqual(b["0x0069"]["amount_signed"], -0.25)
            # file order within a wallet, whatever the file's sort
            file_order = [r["tx_hash"] for r in self.rows if r["wallet_address"] == B]
            self.assertEqual([r["tx_hash"] for r in src.rows(B)], file_order)
            db = src._store.db_path
            self.assertTrue(db.exists())
        finally:
            src.close()
        self.assertFalse(db.exists())                                     # scratch database removed
        self.assertEqual(src.rows(A), [])                                 # closed store serves nothing

    def test_consumers_of_the_rows_agree_on_both_paths(self):
        """What the analyse loop feeds with the rows: merge_rows_by_key (accumulated ledger),
        wallet_meta.advance_state / row (l4_wallet_metadata), compute_analytics."""
        # this run's rows arrive typed from the mappers (float amount_signed, int event_index):
        # one new row, one replacing the staged 0x0064
        new = [extract._type_ledger_row(_row(A, 200, T0 - H // 2)), extract._type_ledger_row(_row(A, 100, T0 - 2 * H, amount="9"))]
        with _Env(HL_UPSERT_STREAM_MIN_BYTES="0"):
            duck = extract.open_ledger_rows(self.path, self.wanted)
        with _Env(HL_UPSERT_STREAM_MIN_BYTES=str(self.path.stat().st_size + 1)):
            dict_ = extract.open_ledger_rows(self.path, self.wanted)
        self.assertEqual((duck.kind, dict_.kind), ("duckdb", "dict"))
        try:
            for w in (A, B, C):
                acc_duck = extract.merge_rows_by_key(duck.rows(w), [r for r in new if r["wallet_address"] == w])
                acc_dict = extract.merge_rows_by_key(dict_.rows(w), [r for r in new if r["wallet_address"] == w])
                self.assertEqual(acc_duck, acc_dict)
                st_duck = wallet_meta.advance_state(None, acc_duck, None, T0, True)
                st_dict = wallet_meta.advance_state(None, acc_dict, None, T0, True)
                self.assertEqual(st_duck, st_dict)
                self.assertEqual(wallet_meta.row(w, st_duck, T0, _iso(T0)), wallet_meta.row(w, st_dict, T0, _iso(T0)))
                prev = {"first_ms": T0 - 100 * H, "latest_ms": T0 - 50 * H, "count_tx": 3, "days": [], "through_ms": T0 - 30 * H}
                self.assertEqual(wallet_meta.advance_state(prev, acc_duck, T0 - 30 * H, T0, True),
                                 wallet_meta.advance_state(prev, acc_dict, T0 - 30 * H, T0, True))
                chs = {"marginSummary": {"accountValue": "0"}, "assetPositions": []}
                wa_duck = analytics.compute_analytics(acc_duck, [], chs, {"balances": []}, T0, _iso(T0),
                                                      token_usd_price={S.USDC_ADDRESS: 1.0}, window_start_ms=T0 - 70 * H)
                wa_dict = analytics.compute_analytics(acc_dict, [], chs, {"balances": []}, T0, _iso(T0),
                                                      token_usd_price={S.USDC_ADDRESS: 1.0}, window_start_ms=T0 - 70 * H)
                self.assertEqual(wa_duck, wa_dict)
            self.assertEqual(wallet_meta.row(C, wallet_meta.advance_state(None, duck.rows(C), None, T0, True), T0, _iso(T0)),
                             None)
        finally:
            duck.close()
            dict_.close()

    def test_threshold_switches_the_path_and_edge_files(self):
        size = self.path.stat().st_size
        with _Env(HL_UPSERT_STREAM_MIN_BYTES=str(size + 1)), \
                self.assertLogs("blueblocx_hyperliquid.extract", level="INFO") as logs:
            src = extract.open_ledger_rows(self.path, self.wanted)
        self.assertEqual(src.kind, "dict")
        self.assertIn("dict path", "\n".join(logs.output))
        self.assertEqual(len(src.rows(A)), 23)
        src.close()
        self.assertEqual(src.rows(A), [])
        with _Env(HL_UPSERT_STREAM_MIN_BYTES=str(size)):
            src = extract.open_ledger_rows(self.path, self.wanted)
        self.assertEqual(src.kind, "duckdb")
        src.close()
        with _Env(HL_UPSERT_STREAM_MIN_BYTES=None):                       # the default: 256 MiB
            self.assertEqual(extract.open_ledger_rows(self.path, self.wanted).kind, "dict")
        # no wallets / no file: the empty dict path, no DuckDB
        with _Env(HL_UPSERT_STREAM_MIN_BYTES="0"):
            self.assertEqual(extract.open_ledger_rows(self.path, set()).kind, "dict")
            self.assertEqual(extract.open_ledger_rows(self.dir / "missing.csv", {A}).rows(A), [])
            self.assertEqual(extract.open_ledger_rows(self.dir / "missing.csv", {A}).kind, "dict")
            # an EMPTY file (0 bytes) is served by the dict path (no header to read)
            empty = self.dir / "empty.csv"
            empty.write_bytes(b"")
            self.assertEqual(extract.open_ledger_rows(empty, {A}).kind, "dict")
            # header only: the DuckDB store holds zero rows
            hdr = self.dir / "hdr.csv"
            _write(hdr, [])
            src = extract.open_ledger_rows(hdr, {A})
            self.assertEqual((src.kind, src.rows(A), src._store.row_count), ("duckdb", [], 0))
            src.close()
            # a file without the wallet column: nothing to serve, nothing raised
            other = self.dir / "other.csv"
            other.write_text("a,b\n1,2\n", encoding="utf-8")
            store = csv_merge.WalletRowStore(other, {A})
            self.assertEqual((store.rows(A), store.row_count), ([], 0))
            store.close()
            # the scratch database lives under the spill dir next to the file
            src = extract.open_ledger_rows(self.path, {A})
            self.assertEqual(src._store.db_path.parent, self.dir / ".duckdb" / "upsert")
            src.close()
            self.assertEqual([p for p in (self.dir / ".duckdb" / "upsert").iterdir()], [])

    def test_a_duckdb_written_ledger_reads_the_same(self):
        """The staged file is normally written by csv_merge (\\n line ends): both paths read it alike."""
        with _Env(HL_UPSERT_STREAM_MIN_BYTES="0"):
            extract.upsert_csv(self.path, S.L1_WALLET_LEDGER_COLUMNS, [_row(C, 300, T0 - H)],
                               key_fn=lambda r: (r["tx_hash"], str(r["event_index"]), r["direction"]),
                               sort_fn=lambda r: (r["block_timestamp"], r["tx_hash"], int(r["event_index"])),
                               key_cols=extract.LEDGER_KEY_COLS, sort_cols=extract.LEDGER_SORT_COLS)
            with extract.open_ledger_rows(self.path, self.wanted) as duck:
                expect = extract._load_ledger_rows_by_wallet(self.path, self.wanted)
                for w in (A, B, C):
                    self.assertEqual(duck.rows(w), expect[w])
                self.assertEqual(len(duck.rows(C)), 1)


class ExtractRunParityTests(unittest.TestCase):
    """An incremental run over a seeded staged ledger: the dict path and the DuckDB path
    leave identical watermarks (incl. the l4_wallet_metadata aggregate), dimension rows,
    analytics contribution and ledger rows."""
    FILL = {"coin": "BTC", "px": "100", "sz": "1", "side": "B", "startPosition": "0", "dir": "Close Long",
            "closedPnl": "1.5", "hash": "0x" + "ab" * 32, "oid": 1, "crossed": True, "fee": "0.1",
            "feeToken": "USDC", "twapId": None}

    @staticmethod
    def _csv(ns: Path, name: str) -> list[dict]:
        with (ns / name).open(newline="", encoding="utf-8") as fh:
            return list(csv.DictReader(fh))

    def _run(self, root: Path, threshold: str | None) -> dict:
        ns = root / S.NS_HYPERCORE
        ns.mkdir(parents=True)
        rows = [_row(W1, i, T0 - (40 - i) * H) for i in range(30)]       # 30 deposits over the last 40 h
        rows += [_row(W2, 50 + i, T0 - (10 - i) * H, counterparty_address="x,y\nz") for i in range(5)]
        rows += [_row(D, 90, T0 - H)]                                   # a wallet not in the run
        _write(ns / "l1_wallet_ledger.csv", rows)
        clock = _FakeTime(T0 / 1000)
        fake = _FakeClient
        fake.clock, fake.bump, fake.fills = clock, 0.0, {W1: [dict(self.FILL, time=T0 - H, tid=1)]}
        with _Env(HL_UPSERT_STREAM_MIN_BYTES=threshold), mock.patch.object(extract, "InfoClient", fake), \
                mock.patch.object(extract, "time", clock), \
                self.assertLogs("blueblocx_hyperliquid.extract", level="INFO") as logs:
            recon = extract.run([W1, W2], 2, root, incremental=True, hip3=False, soft_wallets={W1, W2})
        self.assertEqual(recon["wallets"], [W1, W2])
        self.assertEqual(recon["by_wallet"][W1]["analysis_rows"], 30 + 2)     # 30 staged + fill legs
        self.assertEqual(recon["by_wallet"][W2]["analysis_rows"], 5)
        watermarks = json.loads((ns / "_watermarks.json").read_text(encoding="utf-8"))
        self.stamps.append(watermarks[W1]["updated_at"])   # the run's wall-clock stamp (not under the fake clock)
        return {
            "log": "\n".join(logs.output),
            "watermarks": watermarks,
            "meta": self._csv(ns, "l4_wallet_metadata.csv"),
            "watchlist": self._csv(ns, "wallet_watchlist.csv"),
            "ledger": self._csv(ns, "l1_wallet_ledger.csv"),
            "contrib": json.loads((ns / "wallet_analytics_contrib.json").read_text(encoding="utf-8")),
            "recon": {w: {k: v for k, v in r.items() if k != "fetch_errors"} for w, r in recon["by_wallet"].items()},
            "scratch": sorted(p.name for p in (ns / ".duckdb" / "upsert").iterdir()) if (ns / ".duckdb" / "upsert").exists() else [],
        }

    def test_both_paths_leave_the_same_state_and_rows(self):
        self.stamps = []
        with tempfile.TemporaryDirectory() as d:
            a = self._run(Path(d) / "dict", None)
            b = self._run(Path(d) / "duck", "0")
        self.assertIn("dict path", a["log"])
        self.assertNotIn("DuckDB wallet store", a["log"])
        self.assertIn("DuckDB wallet store", b["log"])
        self.assertEqual(b["scratch"], [])                                 # scratch database removed before write
        for key in ("watermarks", "meta", "watchlist", "ledger", "contrib", "recon"):
            # created_at / updated_at carry each run's real wall-clock instant: the one expected difference
            self.assertEqual(json.dumps(a[key], sort_keys=True).replace(self.stamps[0], "<now>"),
                             json.dumps(b[key], sort_keys=True).replace(self.stamps[1], "<now>"), key)
        self.assertEqual({r["wallet_address"] for r in a["meta"]}, {W1, W2})
        m1 = {r["wallet_address"]: r for r in a["meta"]}
        self.assertEqual(m1[W1]["count_tx"], "30")                          # the perp fill legs do not count
        self.assertEqual(m1[W2]["count_tx"], "5")
        self.assertEqual(a["watermarks"][W1]["meta"]["count_tx"], 30)
        self.assertEqual(len(a["ledger"]), 30 + 5 + 1 + 2)


if __name__ == "__main__":
    unittest.main()
