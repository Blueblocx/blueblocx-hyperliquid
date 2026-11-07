"""extract.upsert_csv's bounded-memory DuckDB path (csv_merge, v1.10.11) against the
dict path: same rows, same strings, same order; threshold switching; DictReader
round trip of what DuckDB writes."""
from __future__ import annotations

import csv
import datetime as dt
import os
import shutil
import tempfile
import unittest
from pathlib import Path

from blueblocx_hyperliquid import csv_merge, extract, iceberg_load, schema as S

COLS = ["block_timestamp", "tx_hash", "event_index", "direction", "amount", "note"]
KEY_COLS = ["tx_hash", "event_index", "direction"]
SORT_COLS = ["block_timestamp", "tx_hash", ("event_index", "int")]


def _s(v):
    return "" if v is None else str(v)


def key_fn(r):
    return (_s(r["tx_hash"]), _s(r["event_index"]), _s(r["direction"]))


def sort_fn(r):
    return (_s(r["block_timestamp"]), _s(r["tx_hash"]), int(r["event_index"]))


def read(path: Path) -> list[dict]:
    with path.open("r", newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def row(ts, tx, idx, direction, amount, note):
    return {"block_timestamp": ts, "tx_hash": tx, "event_index": idx, "direction": direction, "amount": amount,
            "note": note}


OLD = [
    row("2026-11-01T00:00:00+00:00", "0xa", 0, "in", 1.5, "x,y"),                    # quoted comma
    row("2026-11-01T00:00:00+00:00", "0xa", 0, "out", -1.5, None),                   # None -> ''
    row("2026-11-02T00:00:00+00:00", "0xb", 10 ** 12, "in", "", 'q"z'),              # large index, empty, quote
    row("2026-11-02T00:00:00+00:00", "0xb", 2, "in", 0, "l1\nl2"),                   # newline in a value
    row("2026-11-03T00:00:00+00:00", "0xc", 1, "in", 3, "ü€ 漢"),                     # unicode
    row("2026-11-03T00:00:00+00:00", "0xc", 1, "out", -3, " lead/trail "),
]
NEW = [
    row("2026-11-01T00:00:00+00:00", "0xa", 0, "in", 9.0, "replaced"),               # new wins
    row("2026-10-31T00:00:00+00:00", "0xz", 0, "in", 7, None),
    row("2026-10-31T00:00:00+00:00", "0xz", 0, "in", 8, "last new row wins"),         # duplicate key among new rows
    row("2026-11-04T00:00:00+00:00", "0xd", 5, "in", None, ""),
]


class _Env:
    """Temporarily set environment variables."""

    def __init__(self, **kv):
        self.kv = kv
        self.saved = {}

    def __enter__(self):
        for k, v in self.kv.items():
            self.saved[k] = os.environ.get(k)
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
        return self

    def __exit__(self, *a):
        for k, v in self.saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


class StreamingUpsertParityTests(unittest.TestCase):
    def setUp(self):
        self.dir = Path(tempfile.mkdtemp(prefix="hl-upsert-"))
        self.addCleanup(shutil.rmtree, self.dir, True)
        self.a = self.dir / "dict.csv"
        self.b = self.dir / "duck.csv"
        extract.upsert_csv(self.a, COLS, OLD, key_fn=key_fn, sort_fn=sort_fn)
        shutil.copy(self.a, self.b)

    def test_duckdb_merge_matches_the_dict_path(self):
        with self.assertLogs("blueblocx_hyperliquid.extract", level="INFO") as logs:
            extract.upsert_csv(self.a, COLS, NEW, key_fn=key_fn, sort_fn=sort_fn)
            with _Env(HL_UPSERT_STREAM_MIN_BYTES="0"):
                n = extract.upsert_csv(self.b, COLS, NEW, key_fn=key_fn, sort_fn=sort_fn,
                                       key_cols=KEY_COLS, sort_cols=SORT_COLS)
        text = "\n".join(logs.output)
        self.assertIn("(dict path)", text)
        self.assertIn("(duckdb path)", text)
        ra, rb = read(self.a), read(self.b)
        self.assertEqual(n, len(rb))
        self.assertEqual(len(ra), len(OLD) + 2)                       # 0xa/in replaced, 0xz once, 0xd new
        self.assertEqual(ra, rb)                                      # same rows, same strings, same order
        by_key = {key_fn(r): r for r in rb}
        self.assertEqual(by_key[("0xa", "0", "in")]["amount"], "9.0")
        self.assertEqual(by_key[("0xz", "0", "in")]["note"], "last new row wins")
        self.assertEqual(by_key[("0xa", "0", "out")]["note"], "")
        self.assertEqual(by_key[("0xd", "5", "in")]["amount"], "")
        self.assertEqual(by_key[("0xb", "2", "in")]["note"], "l1\nl2")
        self.assertEqual(by_key[("0xb", "1000000000000", "in")]["note"], 'q"z')
        self.assertEqual(by_key[("0xc", "1", "in")]["note"], "ü€ 漢")
        self.assertEqual(by_key[("0xc", "1", "out")]["note"], " lead/trail ")
        # sort order = sort_fn (strings, strings, int): 2 before 1000000000000 within 0xb
        self.assertEqual([sort_fn(r) for r in rb], sorted(sort_fn(r) for r in rb))
        self.assertEqual([r["event_index"] for r in rb if r["tx_hash"] == "0xb"], ["2", "1000000000000"])

    def test_threshold_switches_the_path(self):
        size = self.b.stat().st_size
        with _Env(HL_UPSERT_STREAM_MIN_BYTES=str(size + 1)), \
                self.assertLogs("blueblocx_hyperliquid.extract", level="INFO") as logs:
            extract.upsert_csv(self.b, COLS, NEW[:1], key_fn=key_fn, sort_fn=sort_fn,
                               key_cols=KEY_COLS, sort_cols=SORT_COLS)
        self.assertIn("(dict path)", "\n".join(logs.output))
        size = self.b.stat().st_size
        with _Env(HL_UPSERT_STREAM_MIN_BYTES=str(size)), \
                self.assertLogs("blueblocx_hyperliquid.extract", level="INFO") as logs:
            extract.upsert_csv(self.b, COLS, NEW[1:], key_fn=key_fn, sort_fn=sort_fn,
                               key_cols=KEY_COLS, sort_cols=SORT_COLS)
        self.assertIn("(duckdb path)", "\n".join(logs.output))
        extract.upsert_csv(self.a, COLS, NEW, key_fn=key_fn, sort_fn=sort_fn)
        self.assertEqual(read(self.a), read(self.b))
        # the default threshold is 256 MiB; a missing file or no key_cols never streams
        with _Env(HL_UPSERT_STREAM_MIN_BYTES=None):
            self.assertEqual(csv_merge.stream_threshold_bytes(), 256 * 1024 * 1024)
        fresh = self.dir / "fresh.csv"
        with _Env(HL_UPSERT_STREAM_MIN_BYTES="0"), \
                self.assertLogs("blueblocx_hyperliquid.extract", level="INFO") as logs:
            extract.upsert_csv(fresh, COLS, NEW, key_fn=key_fn, sort_fn=sort_fn, key_cols=KEY_COLS, sort_cols=SORT_COLS)
            extract.upsert_csv(self.b, COLS, NEW, key_fn=key_fn, sort_fn=sort_fn)
        self.assertNotIn("(duckdb path)", "\n".join(logs.output))

    def test_replace_scope_and_merge_fn_always_take_the_dict_path(self):
        with _Env(HL_UPSERT_STREAM_MIN_BYTES="0"), \
                self.assertLogs("blueblocx_hyperliquid.extract", level="INFO") as logs:
            extract.upsert_csv(self.b, COLS, NEW, key_fn=key_fn, sort_fn=sort_fn, key_cols=KEY_COLS, sort_cols=SORT_COLS,
                               replace_scope_fn=lambda r: False)
            extract.upsert_csv(self.b, COLS, NEW, key_fn=key_fn, sort_fn=sort_fn, key_cols=KEY_COLS, sort_cols=SORT_COLS,
                               merge_fn=lambda old, new: new)
        self.assertNotIn("(duckdb path)", "\n".join(logs.output))

    def test_round_trip_through_dict_reader_and_downstream_readers(self):
        with _Env(HL_UPSERT_STREAM_MIN_BYTES="0"):
            extract.upsert_csv(self.b, COLS, NEW, key_fn=key_fn, sort_fn=sort_fn, key_cols=KEY_COLS, sort_cols=SORT_COLS)
        rows = read(self.b)
        # what DuckDB wrote reads back unchanged through the csv module ...
        again = self.dir / "again.csv"
        with again.open("w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=COLS)
            w.writeheader()
            w.writerows(rows)
        self.assertEqual(read(again), rows)
        # ... and through the readers that consume staged files
        self.assertEqual(iceberg_load.read_csv_rows(self.b), rows)
        # a second DuckDB merge over a DuckDB-written file (\n line ends) is a no-op on content
        with _Env(HL_UPSERT_STREAM_MIN_BYTES="0"):
            extract.upsert_csv(self.b, COLS, [], key_fn=key_fn, sort_fn=sort_fn, key_cols=KEY_COLS, sort_cols=SORT_COLS)
        self.assertEqual(read(self.b), rows)
        # the spill directory lives next to the file, created on demand
        self.assertTrue((self.dir / ".duckdb" / "upsert").is_dir())

    def test_columns_added_or_dropped_follow_dict_writer_semantics(self):
        with _Env(HL_UPSERT_STREAM_MIN_BYTES="0"):
            extract.upsert_csv(self.b, COLS + ["extra"], NEW, key_fn=key_fn, sort_fn=sort_fn, key_cols=KEY_COLS,
                               sort_cols=SORT_COLS)
        extract.upsert_csv(self.a, COLS + ["extra"], NEW, key_fn=key_fn, sort_fn=sort_fn)
        self.assertEqual(read(self.a), read(self.b))
        self.assertTrue(all(r["extra"] == "" for r in read(self.b)))
        with _Env(HL_UPSERT_STREAM_MIN_BYTES="0"):
            extract.upsert_csv(self.b, COLS[:-1], [], key_fn=key_fn, sort_fn=sort_fn, key_cols=KEY_COLS, sort_cols=SORT_COLS)
        extract.upsert_csv(self.a, COLS[:-1], [], key_fn=key_fn, sort_fn=sort_fn)
        self.assertEqual(read(self.a), read(self.b))
        self.assertNotIn("note", read(self.b)[0])

    def test_ledger_columns_and_settings(self):
        # the production call: the full ledger column set, the real key / sort columns
        self.assertEqual(extract.LEDGER_KEY_COLS, ["tx_hash", "event_index", "direction"])
        self.assertEqual(extract.LEDGER_SORT_COLS, ["block_timestamp", "tx_hash", ("event_index", "int")])
        base = {c: None for c in S.L1_WALLET_LEDGER_COLUMNS}
        old = [dict(base, block_timestamp="2026-11-01T00:00:00+00:00", tx_hash="0x1", event_index=i, direction="in",
                    wallet_address="0xw", amount=i, amount_signed=float(i)) for i in range(3)]
        new = [dict(base, block_timestamp="2026-11-01T00:00:00+00:00", tx_hash="0x1", event_index=1, direction="in",
                    wallet_address="0xw", amount=99, amount_signed=99.0)]
        a, b = self.dir / "la.csv", self.dir / "lb.csv"
        extract.upsert_csv(a, S.L1_WALLET_LEDGER_COLUMNS, old, key_fn=key_fn, sort_fn=sort_fn)
        shutil.copy(a, b)
        extract.upsert_csv(a, S.L1_WALLET_LEDGER_COLUMNS, new, key_fn=key_fn, sort_fn=sort_fn)
        n = csv_merge.merge_csv_duckdb(b, S.L1_WALLET_LEDGER_COLUMNS, new, key_cols=extract.LEDGER_KEY_COLS,
                                       sort_cols=extract.LEDGER_SORT_COLS, memory_limit="256MB", threads=1)
        self.assertEqual(n, 3)
        self.assertEqual(read(a), read(b))
        self.assertEqual(read(b)[1]["amount"], "99")
        with _Env(HL_UPSERT_DUCKDB_MEMORY=None):
            self.assertEqual(csv_merge.duckdb_memory_limit(), "16GB")
        with _Env(HL_UPSERT_DUCKDB_MEMORY="not a size"):
            with self.assertRaises(ValueError):
                csv_merge.duckdb_memory_limit()


class EdgeCaseTests(unittest.TestCase):
    """v1.10.12: the two parity gaps found in review (an empty existing file, duplicate
    keys already in the OLD file) and the retention prune's bounded-memory paths."""

    def setUp(self):
        self.dir = Path(tempfile.mkdtemp(prefix="hl-upsert-"))
        self.addCleanup(shutil.rmtree, self.dir, True)

    def test_zero_byte_existing_file_streams_like_a_missing_one(self):
        a, b = self.dir / "dict.csv", self.dir / "duck.csv"
        a.touch()
        b.touch()
        self.assertEqual(b.stat().st_size, 0)
        with _Env(HL_UPSERT_STREAM_MIN_BYTES="0"), \
                self.assertLogs("blueblocx_hyperliquid.extract", level="INFO") as logs:
            extract.upsert_csv(a, COLS, NEW, key_fn=key_fn, sort_fn=sort_fn)
            n = extract.upsert_csv(b, COLS, NEW, key_fn=key_fn, sort_fn=sort_fn, key_cols=KEY_COLS, sort_cols=SORT_COLS)
        self.assertIn("(duckdb path)", "\n".join(logs.output))
        self.assertEqual(n, 3)
        self.assertEqual(read(a), read(b))
        # header-only file: the same
        c = self.dir / "header.csv"
        c.write_text(",".join(COLS) + "\r\n", encoding="utf-8")
        with _Env(HL_UPSERT_STREAM_MIN_BYTES="0"):
            extract.upsert_csv(c, COLS, NEW, key_fn=key_fn, sort_fn=sort_fn, key_cols=KEY_COLS, sort_cols=SORT_COLS)
        self.assertEqual(read(c), read(a))

    def test_duplicate_keys_in_the_old_file_collapse_like_the_dict_path(self):
        dup = [row("2026-11-01T00:00:00+00:00", "0xa", 0, "in", 1, "first"),
               row("2026-11-02T00:00:00+00:00", "0xa", 0, "in", 2, "second (later, last in file order)"),
               row("2026-11-03T00:00:00+00:00", "0xb", 1, "out", 3, "unique")]
        a, b = self.dir / "dict.csv", self.dir / "duck.csv"
        for p in (a, b):
            with p.open("w", newline="", encoding="utf-8") as fh:
                w = csv.DictWriter(fh, fieldnames=COLS)
                w.writeheader()
                for r in dup:
                    w.writerow({c: ("" if r.get(c) is None else r[c]) for c in COLS})
        # no new rows: the dict path keeps the LAST occurrence of 0xa/0/in
        extract.upsert_csv(a, COLS, [], key_fn=key_fn, sort_fn=sort_fn)
        with _Env(HL_UPSERT_STREAM_MIN_BYTES="0"), \
                self.assertLogs("blueblocx_hyperliquid.csv_merge", level="WARNING") as logs:
            n = extract.upsert_csv(b, COLS, [], key_fn=key_fn, sort_fn=sort_fn, key_cols=KEY_COLS, sort_cols=SORT_COLS)
        self.assertIn("collapsing the duplicates", "\n".join(logs.output))
        self.assertEqual(n, 2)
        self.assertEqual(read(a), read(b))
        self.assertEqual([r["note"] for r in read(b)], ["second (later, last in file order)", "unique"])
        # with a new row for the duplicated key both paths keep only the new row
        new = [row("2026-11-04T00:00:00+00:00", "0xa", 0, "in", 9, "new")]
        extract.upsert_csv(a, COLS, new, key_fn=key_fn, sort_fn=sort_fn)
        with _Env(HL_UPSERT_STREAM_MIN_BYTES="0"):
            extract.upsert_csv(b, COLS, new, key_fn=key_fn, sort_fn=sort_fn, key_cols=KEY_COLS, sort_cols=SORT_COLS)
        self.assertEqual(read(a), read(b))
        self.assertEqual([r["note"] for r in read(b)], ["unique", "new"])

    # --- retention prune ------------------------------------------------------------------
    PRUNE_ROWS = [
        row("2026-09-01T00:00:00+00:00", "0xold", 0, "in", 1, "old, dropped"),
        row("2026-09-30T23:59:59.999000+00:00", "0xold", 1, "in", 1, "just before the cutoff"),
        row("2026-10-01T00:00:00+00:00", "0xedge", 0, "in", 1, "at the cutoff: kept"),
        row("2026-10-02T00:00:00Z", "0xz", 0, "in", 1, "Z suffix, kept"),
        row("not a timestamp", "0xbad", 0, "in", 1, "unparseable: kept"),
        row("", "0xempty", 0, "in", 1, "empty: kept"),
        row("2026-10-03T00:00:00+00:00", "0xq", 0, "in", 1, 'quoted "value", with\nnewline and ü'),
        row("2026-10-04T00:00:00+00:00", "0xn", 5, "out", None, None),
    ]
    CUTOFF_MS = int(dt.datetime(2026, 10, 1, tzinfo=dt.timezone.utc).timestamp() * 1000)

    def _write(self, p: Path, rows):
        with p.open("w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=COLS)
            w.writeheader()
            for r in rows:
                w.writerow({c: ("" if r.get(c) is None else r[c]) for c in COLS})

    def _reference_prune(self, rows, cutoff_ms):
        """What the pre-v1.10.12 list-based prune_csv kept, in file order."""
        kept = []
        for r in rows:
            ts = r["block_timestamp"]
            try:
                old = bool(ts) and extract._iso_to_ms(ts) < cutoff_ms
            except ValueError:
                old = False
            if not old:
                kept.append({c: ("" if r.get(c) is None else str(r[c])) for c in COLS})
        return kept

    def test_prune_streaming_and_duckdb_match_the_reference(self):
        ref = self._reference_prune(self.PRUNE_ROWS, self.CUTOFF_MS)
        self.assertEqual(len(ref), len(self.PRUNE_ROWS) - 2)
        a, b = self.dir / "stream.csv", self.dir / "duck.csv"
        self._write(a, self.PRUNE_ROWS)
        self._write(b, self.PRUNE_ROWS)
        with _Env(HL_UPSERT_STREAM_MIN_BYTES=str(10 ** 12)):                   # never DuckDB
            ra = extract.prune_csv(a, "block_timestamp", self.CUTOFF_MS)
        with _Env(HL_UPSERT_STREAM_MIN_BYTES="0"), \
                self.assertLogs("blueblocx_hyperliquid.extract", level="INFO") as logs:   # always DuckDB
            rb = extract.prune_csv(b, "block_timestamp", self.CUTOFF_MS)
        self.assertIn("DuckDB prune", "\n".join(logs.output))
        self.assertEqual(ra, {"kept": len(ref), "dropped": 2})
        self.assertEqual(rb, ra)
        self.assertEqual(read(a), ref)                                            # rows, strings and ORDER
        self.assertEqual(read(b), ref)
        self.assertEqual(iceberg_load.read_csv_rows(b), ref)
        self.assertFalse((self.dir / "stream.csv.tmp").exists())
        self.assertFalse((self.dir / "duck.csv.tmp").exists())

    def test_prune_leaves_the_file_untouched_when_nothing_is_old(self):
        a, b = self.dir / "stream.csv", self.dir / "duck.csv"
        self._write(a, self.PRUNE_ROWS)
        self._write(b, self.PRUNE_ROWS)
        before_a, before_b = a.read_bytes(), b.read_bytes()
        with _Env(HL_UPSERT_STREAM_MIN_BYTES=str(10 ** 12)):
            ra = extract.prune_csv(a, "block_timestamp", 0)
        with _Env(HL_UPSERT_STREAM_MIN_BYTES="0"):
            rb = extract.prune_csv(b, "block_timestamp", 0)
        self.assertEqual(ra, {"kept": len(self.PRUNE_ROWS), "dropped": 0})
        self.assertEqual(rb, ra)
        self.assertEqual(a.read_bytes(), before_a)                                 # byte-identical: no rewrite
        self.assertEqual(b.read_bytes(), before_b)
        self.assertFalse((self.dir / "stream.csv.tmp").exists())
        # a column the file lacks, a missing file and an empty file: nothing dropped
        with _Env(HL_UPSERT_STREAM_MIN_BYTES="0"):
            self.assertEqual(extract.prune_csv(b, "no_such_col", 10 ** 15), {"kept": len(self.PRUNE_ROWS), "dropped": 0})
            self.assertEqual(extract.prune_csv(self.dir / "missing.csv", "block_timestamp", 1), {"kept": 0, "dropped": 0})
            empty = self.dir / "empty.csv"
            empty.touch()
            self.assertEqual(extract.prune_csv(empty, "block_timestamp", 1), {"kept": 0, "dropped": 0})
        with _Env(HL_UPSERT_STREAM_MIN_BYTES=str(10 ** 12)):
            self.assertEqual(extract.prune_csv(b, "no_such_col", 10 ** 15), {"kept": len(self.PRUNE_ROWS), "dropped": 0})

    def test_prune_the_real_ledger_columns_both_ways(self):
        base = {c: None for c in S.L1_WALLET_LEDGER_COLUMNS}
        rows = [dict(base, block_timestamp=f"2026-10-{d:02d}T06:29:57.038000+00:00", block_date=f"2026-10-{d:02d}",
                     tx_hash=f"hlcore:0xabc:tid:{d}", event_index=0, event_type="perp_realized_pnl", wallet_address="0xabc",
                     wallet_group=3234, counterparty_address="imx", token_address="hlcore:usdc", direction="in",
                     amount_raw=d * 1e6, decimals=6, amount=d, amount_signed=float(d),
                     created_at="2026-11-01T00:00:00+00:00", updated_at="2026-11-01T00:00:00+00:00") for d in range(1, 11)]
        cutoff = extract._iso_to_ms("2026-10-05T00:00:00+00:00")
        a, b = self.dir / "la.csv", self.dir / "lb.csv"
        for p in (a, b):
            with p.open("w", newline="", encoding="utf-8") as fh:
                w = csv.DictWriter(fh, fieldnames=S.L1_WALLET_LEDGER_COLUMNS)
                w.writeheader()
                for r in rows:
                    w.writerow({c: ("" if r.get(c) is None else r[c]) for c in S.L1_WALLET_LEDGER_COLUMNS})
        with _Env(HL_UPSERT_STREAM_MIN_BYTES=str(10 ** 12)):
            ra = extract.prune_csv(a, "block_timestamp", cutoff)
        rb = csv_merge.prune_csv_duckdb(b, "block_timestamp", cutoff, memory_limit="256MB", threads=1)
        self.assertEqual(ra, {"kept": 6, "dropped": 4})
        self.assertEqual(rb, ra)
        self.assertEqual(read(a), read(b))
        self.assertEqual([r["block_date"] for r in read(b)], [f"2026-10-{d:02d}" for d in range(5, 11)])


if __name__ == "__main__":
    unittest.main()
