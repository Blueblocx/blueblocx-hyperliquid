"""extract.upsert_csv's bounded-memory DuckDB path (csv_merge, v1.10.11) against the
dict path: same rows, same strings, same order; threshold switching; DictReader
round trip of what DuckDB writes."""
from __future__ import annotations

import csv
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


if __name__ == "__main__":
    unittest.main()
