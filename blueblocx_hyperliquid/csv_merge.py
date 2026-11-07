"""
Bounded-memory upsert of a LARGE staged CSV (v1.10.11).

`extract.upsert_csv` merges a run's new rows into the staged file by natural key.
Its original implementation reads the whole file into a dict of Python dicts
(~3.5 KB per ledger row): on 2026-11-06 the staged `l1_wallet_ledger.csv` had
grown to 8.5 GB / 28 M rows (retention only prunes after a SUCCESSFUL lift) and the
merge peaked at ~98 GB, above the unit's 96 GiB cgroup cap - every hourly run was
OOM-killed, nothing was lifted, the file never shrank.

This module does the same merge in DuckDB with a hard `memory_limit` and a spill
directory, so the peak is bounded by HL_UPSERT_DUCKDB_MEMORY (16GB) whatever the file
size. Semantics mirror the dict path exactly where it matters:

  * both files are read as VARCHAR (`all_varchar=true`): every value stays the byte
    string the csv module would have produced; an empty field is NULL inside DuckDB
    and written back as an empty field (the csv module writes '' for None and '');
  * the natural key is compared as strings with NULL == '' (the dict path keys on
    `_s(v)`), new rows win, and among new rows carrying the same key the LAST one wins
    (dict assignment order);
  * the result is ordered by the sort columns (string or BIGINT comparison per
    column, as `sort_fn` does with `s(...)` / `int(...)`), ties broken by the key
    columns so the output is deterministic (the dict path breaks ties by file order);
  * the output has exactly `columns` in that order; a column the old file lacks is
    written empty, a column it has but `columns` does not is dropped (DictWriter
    `extrasaction="ignore"` / `r.get(c)`).

The output is written by DuckDB (`COPY ... (FORMAT CSV, HEADER)`): comma delimiter,
`"` quote and escape, fields quoted only when they contain a delimiter / quote /
newline, `\\n` line ends (the csv module writes `\\r\\n`; `csv.DictReader`, the one
reader of these files - iceberg_load.read_csv_rows, extract.prune_csv,
extract._load_ledger_rows_by_wallet - accepts both, see tests/test_upsert_stream.py).
"""
from __future__ import annotations

import csv
import logging
import os
import re
import time
from pathlib import Path

from . import config

log = logging.getLogger("blueblocx_hyperliquid.csv_merge")

# sort column spec: a bare name sorts as a string; (name, "int") as a BIGINT
SortSpec = str | tuple[str, str]

_MEM_RE = re.compile(r"\d+(\.\d+)?\s*[KMGT]i?B", flags=re.IGNORECASE)


def stream_threshold_bytes() -> int:
    """HL_UPSERT_STREAM_MIN_BYTES: the staged-file size from which the merge runs in
    DuckDB (default 256 MiB). 0 = every upsert of an existing file."""
    return max(0, config.env_int("HL_UPSERT_STREAM_MIN_BYTES", 256 * 1024 * 1024))


def duckdb_memory_limit() -> str:
    mem = (config.env("HL_UPSERT_DUCKDB_MEMORY") or "16GB").strip()
    if not _MEM_RE.fullmatch(mem):
        raise ValueError(f"HL_UPSERT_DUCKDB_MEMORY must look like '16GB', got {mem!r}")
    return mem


def _q(ident: str) -> str:
    """A quoted SQL identifier."""
    return '"' + ident.replace('"', '""') + '"'


def _lit(s: str) -> str:
    """A quoted SQL string literal."""
    return "'" + s.replace("'", "''") + "'"


def _header(path: Path) -> list[str]:
    with path.open("r", newline="", encoding="utf-8") as fh:
        return next(csv.reader(fh), [])


def _read_csv_expr(path: Path, columns: list[str]) -> str:
    """read_csv(...) with an explicit all-VARCHAR schema (no sniffing, no sampling):
    the file's own header names the columns, every field stays a string."""
    cols = ", ".join(f"{_lit(c)}: 'VARCHAR'" for c in columns)
    return (f"read_csv({_lit(str(path))}, header=true, delim=',', quote='\"', escape='\"', "
            f"all_varchar=true, columns={{{cols}}})")


def _sort_sql(sort_cols: list[SortSpec], key_cols: list[str], alias: str = "") -> str:
    """ORDER BY mirroring sort_fn: `s(v)` -> string compare of COALESCE(v, ''),
    `int(v)` -> CAST AS BIGINT; then the key columns as the deterministic tie-break."""
    terms: list[str] = []
    seen: set[str] = set()
    for spec in sort_cols:
        name, kind = (spec, "str") if isinstance(spec, str) else (spec[0], spec[1])
        col = f"{alias}{_q(name)}"
        if kind == "int":
            terms.append(f"CAST({col} AS BIGINT)")
        elif kind == "str":
            terms.append(f"COALESCE({col}, '')")
        else:
            raise ValueError(f"sort column kind must be 'str' or 'int', got {kind!r} for {name}")
        seen.add(name)
    for k in key_cols:
        if k not in seen:
            terms.append(f"COALESCE({alias}{_q(k)}, '')")
    return ", ".join(terms)


def merge_csv_duckdb(path: Path, columns: list[str], rows: list[dict], *, key_cols: list[str],
                     sort_cols: list[SortSpec] | None = None, memory_limit: str | None = None,
                     threads: int = 4, temp_dir: Path | None = None) -> int:
    """Merge `rows` (new rows win) into the CSV at `path` by `key_cols`, sorted by
    `sort_cols`, with DuckDB under `memory_limit` (default HL_UPSERT_DUCKDB_MEMORY),
    spilling under `temp_dir` (default <path.parent>/.duckdb/upsert). Writes
    `path` atomically (tmp file + os.replace). Returns the merged row count."""
    import duckdb
    path = Path(path)
    for k in key_cols:
        if k not in columns:
            raise ValueError(f"key column {k!r} is not in columns")
    memory_limit = memory_limit or duckdb_memory_limit()
    temp_dir = Path(temp_dir) if temp_dir is not None else path.parent / ".duckdb" / "upsert"
    temp_dir.mkdir(parents=True, exist_ok=True)
    t0 = time.monotonic()
    # 1) the new rows, exactly as the csv module would write them, plus their position
    #    (the last of two new rows with the same key wins, like dict assignment)
    new_tmp = path.with_name(path.name + ".new.tmp")
    with new_tmp.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(columns + ["_rn"])
        for i, r in enumerate(rows):
            w.writerow([("" if r.get(c) in (None, "") else str(r[c])) for c in columns] + [str(i)])
    out_tmp = path.with_name(path.name + ".tmp")
    con = duckdb.connect(database=":memory:")
    try:
        con.execute(f"SET memory_limit = {_lit(memory_limit)}")
        con.execute(f"SET threads = {max(1, int(threads))}")
        con.execute(f"SET temp_directory = {_lit(str(temp_dir))}")
        con.execute("SET preserve_insertion_order = false")
        sel = ", ".join(_q(c) for c in columns)
        key_eq = " AND ".join(f"COALESCE(o.{_q(k)}, '') = COALESCE(n.{_q(k)}, '')" for k in key_cols)
        part = ", ".join(f"COALESCE({_q(k)}, '')" for k in key_cols)
        new_rel = (f"SELECT {sel} FROM {_read_csv_expr(new_tmp, columns + ['_rn'])} "
                   f"QUALIFY row_number() OVER (PARTITION BY {part} ORDER BY CAST(\"_rn\" AS BIGINT) DESC) = 1")
        con.execute(f"CREATE TEMP TABLE new_rows AS {new_rel}")
        if path.exists():
            old_cols = _header(path)
            old_sel = ", ".join((_q(c) if c in old_cols else f"CAST(NULL AS VARCHAR) AS {_q(c)}") for c in columns)
            old_rel = (f"SELECT {old_sel} FROM {_read_csv_expr(path, old_cols)} o "
                       f"WHERE NOT EXISTS (SELECT 1 FROM new_rows n WHERE {key_eq})")
            merged = f"{old_rel} UNION ALL SELECT {sel} FROM new_rows"
        else:
            merged = f"SELECT {sel} FROM new_rows"
        order = f" ORDER BY {_sort_sql(sort_cols, key_cols)}" if sort_cols else ""
        con.execute(f"COPY (SELECT {sel} FROM ({merged}) m{order}) TO {_lit(str(out_tmp))} "
                    "(FORMAT CSV, HEADER true, DELIMITER ',', QUOTE '\"', ESCAPE '\"', NULLSTR '')")
        total = con.execute(f"SELECT count(*) FROM {_read_csv_expr(out_tmp, columns)}").fetchone()[0]
    finally:
        con.close()
        try:
            new_tmp.unlink()
        except OSError:
            pass
    os.replace(out_tmp, path)
    log.info("csv_merge: %s merged in DuckDB (memory_limit %s, spill %s) in %.1fs -> %d rows",
             path.name, memory_limit, temp_dir, time.monotonic() - t0, total)
    return int(total)
