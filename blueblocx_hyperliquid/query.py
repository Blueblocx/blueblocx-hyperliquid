"""
Read-only SQL over the lake — pyiceberg resolves the tables, DuckDB runs the SQL.

    python -m blueblocx_hyperliquid.query --list
    python -m blueblocx_hyperliquid.query "select event_type, count(*) n from l1_wallet_ledger group by 1 order by 2 desc"
    python -m blueblocx_hyperliquid.query --days 90 -f report.sql --format csv > report.csv
    python -m blueblocx_hyperliquid.query                     # interactive prompt when stdin is a terminal
    hypercore-sql ...                                         # the same against the deployment (deploy/hypercore-sql)

How it works: pyiceberg opens the catalog the environment selects (REST / SQL /
local, exactly like the lift) and resolves the COMMITTED snapshot of every table
through it. Each table a statement names is scanned once — with partition pruning
on its date column when a time bound is set — and registered in an in-memory
DuckDB, which then runs the SQL. The catalog is the only correct entry point:
pyiceberg writes no version-hint.text, and plain Parquet globs over the warehouse
would double count (pre-replace files of l4x and the dimension tables stay on
disk until swept).

Nothing here writes. The catalog is opened read-only (init_catalog_tables=false:
a mistyped URI cannot create catalog tables; no directory is created), DuckDB
holds copies in memory with file access disabled (no COPY TO a file, no
read_csv, no ATTACH / INSTALL — export with `--format csv > file` instead), and
the env file is loaded in-process and never printed (error messages are scrubbed
of secret values). Reading neither blocks nor breaks the hourly lift: Iceberg
readers take no locks and the lift's commit is one optimistic UPDATE of the
catalog pointer. DuckDB may spill an oversized query to a temp directory under
the system temp dir, never into the checkout or the state directory.

Bounded by default: the fact and snapshot tables are read for the last --days
days (30) on block_date / as_of_date / bucket_date, because the filtered scan is
materialised in memory (~250 bytes per ledger row). --days 0 reads everything;
--until alone ends the window there. Dimension tables and
control.table_watermarks are read whole. SQL names are the bare table names
(l1_wallet_ledger, table_watermarks, ...); the namespace-qualified forms
(hypercore.l1_wallet_ledger, control.table_watermarks) work too.

Reading the ledger correctly (docs/HYPERCORE_LEDGER_SPEC.md §3): amount_signed is
in the row's token (token_address), not USD — sum stablecoin rows directly, price
the rest; realized PnL (lake part) = Σ usd(row) over event_type in
(perp_realized_pnl, funding, fee, interest, staking_reward — the last is reserved,
not emitted yet); net_flow = Σ usd(row) over (deposit, withdraw, transfer) only;
one spot fill is several rows sharing tx_hash (COUNT(DISTINCT tx_hash)).
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import decimal
import io
import json
import logging
import math
import os
import re
import shutil
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

from . import config
from . import iceberg_load as il
from . import schema as S

log = logging.getLogger("blueblocx_hyperliquid.query")

CONTROL_TABLE = "table_watermarks"
# the identity-partitioned DATE column a time bound prunes on (fact / snapshot tables)
DATE_COLUMN = {"l1_wallet_ledger": "block_date", "l3_token_price_usd_1m": "bucket_date",
               "l4x_perp_position_snapshot": "as_of_date", "l4x_spot_balance_snapshot": "as_of_date",
               "l4_wallet_balance_per_tx": "block_date", "l4_wallet_balance_1d_dense": "snapshot_date",
               "l4_wallet_networth_1d_dense": "snapshot_date", "l4_wallet_pnl_realized_1d": "snapshot_date",
               "wallet_overview_pnl_percentile_1d": "computed_date", "l4_token_wallet_sharpe_1d": "snapshot_date",
               "wallet_performance_30d": "as_of_date"}
DEFAULT_DAYS = 30
FORMATS = ("table", "csv", "json")
_IDENT = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
# string literals and comments: table names inside them are not references
_NOT_CODE = re.compile(r"'(?:[^']|'')*'|--[^\n]*|/\*.*?\*/", re.S)


# --- time bound ---------------------------------------------------------------------

@dataclass(frozen=True)
class Bound:
    """Inclusive [since, until] on a table's DATE partition column; None = open."""
    since: dt.date | None = None
    until: dt.date | None = None

    @classmethod
    def from_args(cls, days: int | None, since: str | None, until: str | None,
                  today: dt.date | None = None) -> "Bound":
        """--since wins; else a --days window ending at --until (or today); days <= 0 = open."""
        lo = dt.date.fromisoformat(since) if since else None
        hi = dt.date.fromisoformat(until) if until else None
        if lo is None and days is not None and days > 0:
            anchor = hi or today or dt.datetime.now(tz=dt.timezone.utc).date()
            lo = anchor - dt.timedelta(days=days)
        if lo is not None and hi is not None and lo > hi:
            raise ValueError(f"--since {lo} is after --until {hi}")
        return cls(lo, hi)

    def row_filter(self, col: str) -> str | None:
        parts = []
        if self.since is not None:
            parts.append(f"{col} >= '{self.since.isoformat()}'")
        if self.until is not None:
            parts.append(f"{col} <= '{self.until.isoformat()}'")
        return " and ".join(parts) or None

    def describe(self) -> str:
        if self.since is None and self.until is None:
            return "all rows"
        return " ".join(p for p in (f"since {self.since}" if self.since else "",
                                    f"until {self.until}" if self.until else "") if p)


# --- the session ----------------------------------------------------------------------

def _ms_iso(ms: int | None) -> str | None:
    if ms is None:
        return None
    return dt.datetime.fromtimestamp(ms / 1000, tz=dt.timezone.utc).isoformat(timespec="seconds")


def _bare(ident) -> str:
    return ident[-1] if isinstance(ident, tuple) else str(ident).split(".")[-1]


class Lake:
    """One read-only session: a catalog, a namespace, an in-memory DuckDB and the
    tables loaded into it so far. Tables load lazily, when a statement names them."""

    def __init__(self, catalog, namespace: str = S.NS_HYPERCORE, bound: Bound = Bound(),
                 mode: str = "?", connection=None):
        import duckdb
        self.catalog = catalog
        self.namespace = namespace
        self.bound = bound
        self.mode = mode
        self.con = connection or duckdb.connect(database=":memory:")
        # spill dir for an oversized query: under the system temp dir, never the cwd
        # (which is the production checkout under the wrapper, or the working copy)
        self.temp_dir = os.path.join(tempfile.gettempdir(), f"hypercore-sql-{os.getpid()}")
        self.con.execute(f"SET temp_directory = '{self.temp_dir.replace(chr(39), chr(39) * 2)}'")
        self.con.execute("SET TimeZone = 'UTC'")        # timestamptz values display in UTC, like the lake
        # read-only in practice: no COPY TO a file, read_csv, ATTACH, INSTALL from user SQL;
        # locked so a statement cannot switch it back on
        self.con.execute("SET enable_external_access = false")
        self.con.execute("SET lock_configuration = true")
        self.loaded: dict[str, dict] = {}
        self._arrow: dict[str, object] = {}             # keep the registered Arrow tables alive
        self._idents: dict[str, str] | None = None

    # -- catalog ---------------------------------------------------------------------
    def idents(self) -> dict[str, str]:
        """bare table name -> catalog identifier: the namespace's tables plus
        control.table_watermarks (all namespaces' rows; filter on its namespace column)."""
        if self._idents is None:
            from pyiceberg.exceptions import NoSuchNamespaceError
            names: dict[str, str] = {}
            for ns in (self.namespace, il.CONTROL_NS):
                try:
                    found = self.catalog.list_tables(ns)
                except NoSuchNamespaceError:
                    continue
                for t in found:
                    name = _bare(t)
                    if ns == il.CONTROL_NS and name != CONTROL_TABLE:
                        continue
                    names.setdefault(name, f"{ns}.{name}")
            self._idents = names
        return self._idents

    def resolve(self, name: str) -> str | None:
        """Bare table name for `name` as a user typed it: bare, "quoted", or
        namespace-qualified (hypercore.l1_wallet_ledger); None when unknown."""
        bare = name.strip().strip('"').split(".")[-1].strip('"').lower()
        for known in self.idents():
            if known.lower() == bare:
                return known
        return None

    @staticmethod
    def _tokens(sql: str) -> set[str]:
        return {t.lower() for t in _IDENT.findall(_NOT_CODE.sub(" ", sql))}

    def referenced(self, sql: str) -> list[str]:
        """Known table names that appear as identifiers in `sql` (case-insensitive;
        names inside string literals or comments do not count)."""
        toks = self._tokens(sql)
        return [n for n in self.idents() if n.lower() in toks]

    def unknown(self, sql: str) -> list[str]:
        """DDL table names used in `sql` that are not in the lake (never lifted yet)."""
        toks = self._tokens(sql)
        return [n for n in il.TABLE_PLAN if n in toks and n not in self.idents()]

    # -- loading ---------------------------------------------------------------------
    def load(self, name: str, force: bool = False) -> dict:
        """Scan one table (bounded on its date column) into DuckDB as `name`, also
        reachable as <namespace>.<name>. Returns what was loaded."""
        if name in self.loaded and not force:
            return self.loaded[name]
        ident = self.idents().get(name)
        if ident is None:
            raise KeyError(name)
        t0 = time.monotonic()
        table = self.catalog.load_table(ident)
        col = DATE_COLUMN.get(name)
        flt = self.bound.row_filter(col) if col else None
        scan = table.scan(row_filter=flt) if flt else table.scan()
        arrow = scan.to_arrow()
        if name in self._arrow:
            self.con.unregister(name)
        self.con.register(name, arrow)
        self._arrow[name] = arrow
        ns = ident.split(".", 1)[0]
        self.con.execute(f'CREATE SCHEMA IF NOT EXISTS "{ns}"')
        self.con.execute(f'CREATE OR REPLACE VIEW "{ns}"."{name}" AS SELECT * FROM main."{name}"')
        snap = table.current_snapshot()
        info = {"table": ident, "rows": arrow.num_rows, "filter": flt or "all rows",
                "seconds": round(time.monotonic() - t0, 2),
                "snapshot_id": snap.snapshot_id if snap else None,
                "committed_at": _ms_iso(snap.timestamp_ms) if snap else None,
                "table_rows": il._table_row_count(table)}
        self.loaded[name] = info
        log.info("loaded %s: %d rows (%s) in %.1fs", ident, arrow.num_rows, info["filter"], info["seconds"])
        return info

    def describe(self) -> tuple[list[str], list[list]]:
        """One row per table: identifier, rows in the lake (metadata), partition
        columns, last commit, the bound this session applies, rows loaded so far."""
        cols = ["table", "rows", "partition", "last_commit", "bound", "loaded"]
        rows = []
        for name, ident in sorted(self.idents().items()):
            table = self.catalog.load_table(ident)
            snap = table.current_snapshot()
            col = DATE_COLUMN.get(name)
            rows.append([ident, il._table_row_count(table),
                         ",".join(f.name for f in table.spec().fields) or "-",
                         _ms_iso(snap.timestamp_ms) if snap else None,
                         (self.bound.row_filter(col) if col else None) or "all rows",
                         self.loaded.get(name, {}).get("rows")])
        return cols, rows

    # -- running ---------------------------------------------------------------------
    def run(self, sql: str) -> tuple[list[str], list[list]]:
        """Load the tables `sql` names, execute it, return (columns, rows). Values
        come back through Arrow (timestamptz as UTC datetimes; no pytz needed)."""
        for name in self.referenced(sql):
            self.load(name)
        cur = self.con.execute(sql)
        if cur.description is None:
            return [], []
        cols = [d[0] for d in cur.description]
        arrow = cur.to_arrow_table() if hasattr(cur, "to_arrow_table") else cur.fetch_arrow_table()
        data = arrow.to_pydict()
        return cols, [list(r) for r in zip(*(data[c] for c in cols))] if cols else []

    def close(self) -> None:
        try:
            self.con.close()
        finally:
            try:
                close_catalog(self.catalog)
            finally:
                shutil.rmtree(self.temp_dir, ignore_errors=True)


def close_catalog(catalog) -> None:
    """Release the catalog's database connection (pyiceberg >= 0.10 has close())."""
    if hasattr(catalog, "close"):
        catalog.close()
    elif getattr(catalog, "engine", None) is not None:
        catalog.engine.dispose()


# --- SQL text --------------------------------------------------------------------------

def _split(text: str) -> tuple[list[str], str, str | None]:
    """(complete statements, unterminated tail, what is still open at the end:
    a quote character, '/*' for a block comment, or None)."""
    out: list[str] = []
    buf: list[str] = []
    quote: str | None = None
    i, n = 0, len(text)
    while i < n:
        ch = text[i]
        if quote:
            buf.append(ch)
            if ch == quote:
                if i + 1 < n and text[i + 1] == quote:          # doubled quote inside a literal
                    buf.append(text[i + 1])
                    i += 1
                else:
                    quote = None
        elif ch in ("'", '"'):
            quote = ch
            buf.append(ch)
        elif ch == "-" and text.startswith("--", i):
            j = text.find("\n", i)
            i = n if j < 0 else j                                # keep the newline itself
            continue
        elif ch == "/" and text.startswith("/*", i):
            j = text.find("*/", i + 2)
            if j < 0:
                return out, "".join(buf).strip(), "/*"           # unterminated block comment
            i = j + 2
            continue
        elif ch == ";":
            stmt = "".join(buf).strip()
            if stmt:
                out.append(stmt)
            buf = []
        else:
            buf.append(ch)
        i += 1
    return out, "".join(buf).strip(), quote


def split_statements(text: str) -> list[str]:
    """Statements separated by ';' outside quotes ('--' and '/* */' comments
    dropped); a final statement without ';' counts too."""
    out, tail, _ = _split(text)
    if tail:
        out.append(tail)
    return out


def complete(text: str) -> bool:
    """True when the buffered input ends with a terminated statement: nothing after
    the last ';' but comments/whitespace, and no quote or block comment left open."""
    out, tail, open_ = _split(text)
    return bool(out) and not tail and open_ is None


# --- output ----------------------------------------------------------------------------

def _is_number(v) -> bool:
    return isinstance(v, (int, float, decimal.Decimal)) and not isinstance(v, bool)


def _cell(v) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, dt.datetime):
        return v.isoformat(sep=" ", timespec="seconds" if v.microsecond == 0 else "auto")
    if isinstance(v, (bytes, bytearray)):
        return v.hex()
    return str(v)


def _json_value(v):
    if isinstance(v, decimal.Decimal):                           # HUGEINT / DECIMAL results
        return int(v) if v == v.to_integral_value() else float(v)
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None                                               # NaN/inf are not JSON
    if isinstance(v, (dt.datetime, dt.date)):
        return v.isoformat()
    if isinstance(v, (bytes, bytearray)):
        return v.hex()
    if isinstance(v, (list, tuple)):
        return [_json_value(x) for x in v]
    if isinstance(v, dict):
        return {str(k): _json_value(x) for k, x in v.items()}
    return v


def _csv_value(v):
    if v is None:
        return ""
    if isinstance(v, (dt.datetime, dt.date)):
        return v.isoformat()
    if isinstance(v, (bytes, bytearray)):
        return v.hex()
    if isinstance(v, (list, tuple, dict)):
        return json.dumps(_json_value(v), ensure_ascii=False)
    return v


def format_rows(cols: list[str], rows: list[list], fmt: str = "table", max_rows: int = 0) -> str:
    """csv / json lines / an aligned text table (numbers right-aligned, `max_rows`
    shown with a '... N more' note when non-zero)."""
    if fmt == "json":
        return "\n".join(json.dumps({c: _json_value(v) for c, v in zip(cols, r)}, default=str, ensure_ascii=False)
                         for r in rows)
    if fmt == "csv":
        buf = io.StringIO()
        w = csv.writer(buf, lineterminator="\n")
        w.writerow(cols)
        for r in rows:
            w.writerow([_csv_value(v) for v in r])
        return buf.getvalue().rstrip("\n")
    if fmt != "table":
        raise ValueError(fmt)
    if not cols:
        return "(ok)"
    shown = rows if max_rows <= 0 else rows[:max_rows]
    text = [[_cell(v) for v in r] for r in shown]
    widths = [max([len(c)] + [len(r[i]) for r in text]) for i, c in enumerate(cols)]
    numeric = [all(_is_number(r[i]) for r in shown if r[i] is not None) for i in range(len(cols))]

    def line(cells):
        return " | ".join((c.rjust(widths[i]) if numeric[i] else c.ljust(widths[i]))
                          for i, c in enumerate(cells)).rstrip()
    out = [line(cols), "-+-".join("-" * w for w in widths), *[line(r) for r in text]]
    n = len(rows)
    tail = f"({n} row{'s' if n != 1 else ''})"
    if len(shown) < n:
        tail += f"  ... {n - len(shown)} more not shown (--max-rows 0 for all, --format csv, or .rows N at the prompt)"
    out.append(tail)
    return "\n".join(out)


def scrub(text: str) -> str:
    """Hide secret values (and the password part of the catalog URL) in a message."""
    out = str(text)
    for name in config.SECRET_VARS:
        v = os.environ.get(name)
        if v:
            out = out.replace(v, f"<{name}>")
    uri = os.environ.get("HL_CATALOG_URI", "")
    if uri:
        try:
            from sqlalchemy.engine import make_url
            pw = make_url(uri).password
        except Exception:  # noqa: BLE001
            pw = None
        if pw:
            out = out.replace(pw, "***")
    return out


# --- CLI ---------------------------------------------------------------------------------

def execute(lake: Lake, sql: str, fmt: str, max_rows: int, out=None, err=None) -> int:
    """Run one statement and print its result: 0 ok, 1 query error, 130 interrupted."""
    out = out or sys.stdout
    err = err or sys.stderr
    try:
        cols, rows = lake.run(sql)
    except KeyboardInterrupt:
        print("interrupted", file=err)
        return 130
    except Exception as e:  # noqa: BLE001 — DuckDB binder/catalog errors, pyiceberg read errors
        msg = scrub(f"{type(e).__name__}: {e}").strip()
        missing = lake.unknown(sql)
        if missing:
            msg += f"\n  (not in the lake yet: {', '.join(missing)} - nothing lifted into it so far)"
        print(f"error: {msg}", file=err)
        return 1
    print(format_rows(cols, rows, fmt, max_rows), file=out)
    return 0


def _print_list(lake: Lake, fmt: str, out=None) -> None:
    cols, rows = lake.describe()
    print(format_rows(cols, rows, fmt), file=out or sys.stdout)


REPL_HELP = "commands: .tables   .schema NAME   .load NAME   .rows N   .quit   (end a statement with ';')"


def repl(lake: Lake, fmt: str, max_rows: int) -> int:
    try:
        import readline  # noqa: F401 — line editing + history when available
    except ImportError:
        pass
    print(f"hypercore-sql: namespace {lake.namespace}, catalog {lake.mode}, {lake.bound.describe()}; "
          f"tables: {', '.join(sorted(lake.idents())) or 'none'}", file=sys.stderr)
    print(REPL_HELP, file=sys.stderr)
    buf: list[str] = []
    while True:
        try:
            line = input("hl> " if not buf else "...> ")
        except EOFError:
            print(file=sys.stderr)
            return 0
        except KeyboardInterrupt:
            buf = []
            print(file=sys.stderr)
            continue
        cmd = line.strip()
        if not buf and cmd.startswith("."):
            word, _, arg = cmd.partition(" ")
            arg = arg.strip()
            if word in (".quit", ".exit", ".q"):
                return 0
            if word == ".tables":
                _print_list(lake, fmt)
            elif word in (".schema", ".load") and arg:
                name = lake.resolve(arg)
                if name is None:
                    print(f"error: unknown table {arg} (see .tables)", file=sys.stderr)
                elif word == ".schema":
                    execute(lake, f'DESCRIBE "{name}"', fmt, 0)
                else:
                    try:
                        info = lake.load(name, force=True)
                        print(f"loaded {info['table']}: {info['rows']} rows ({info['filter']}) in {info['seconds']}s",
                              file=sys.stderr)
                    except KeyboardInterrupt:
                        print("interrupted", file=sys.stderr)
                    except Exception as e:  # noqa: BLE001
                        print("error: " + scrub(f"{type(e).__name__}: {e}"), file=sys.stderr)
            elif word == ".rows" and arg.lstrip("-").isdigit():
                max_rows = max(0, int(arg))
                print(f"showing {'all' if max_rows == 0 else max_rows} rows per result", file=sys.stderr)
            else:
                print(REPL_HELP, file=sys.stderr)
            continue
        buf.append(line)
        text = "\n".join(buf)
        if complete(text):
            for stmt in split_statements(text):
                if execute(lake, stmt, fmt, max_rows) == 130:
                    break
            buf = []


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        prog="hypercore-sql",
        description="Read-only SQL over the lake: pyiceberg resolves each table's committed snapshot, "
                    "DuckDB runs the SQL. Nothing is written.",
        epilog="examples:  hypercore-sql --list   |   hypercore-sql \"select event_type, count(*) n from "
               "l1_wallet_ledger group by 1\"   |   hypercore-sql --days 90 --format csv -f report.sql > report.csv")
    ap.add_argument("sql", nargs="?", help="SQL to run (several statements separated by ';'). Omitted: "
                                           "-f FILE, else stdin, else an interactive prompt on a terminal")
    ap.add_argument("-f", "--file", help="read the SQL from this file (relative to the current directory)")
    ap.add_argument("--list", action="store_true",
                    help="list the tables: rows in the lake, partitioning, last commit, and the time bound in "
                         "effect (--days / --since / --until); then exit")
    ap.add_argument("--days", type=int, default=DEFAULT_DAYS,
                    help=f"read the fact/snapshot tables for the last N days only (partition pruning on "
                         f"block_date / as_of_date / bucket_date); the window ends at --until when given; "
                         f"0 = everything. Default {DEFAULT_DAYS}")
    ap.add_argument("--since", help="lower bound YYYY-MM-DD (replaces --days)")
    ap.add_argument("--until", help="upper bound YYYY-MM-DD, inclusive")
    ap.add_argument("--namespace", default=S.NS_HYPERCORE, help="hypercore (default) | hyperevm")
    ap.add_argument("--format", choices=FORMATS, default="table",
                    help="table = aligned text (default), csv = header + rows, json = one object per line")
    ap.add_argument("--max-rows", type=int, default=200,
                    help="rows printed per result in table format (0 = all); csv/json always print everything")
    ap.add_argument("-v", "--verbose", action="store_true", help="log every table load (rows, filter, seconds)")
    args = ap.parse_args(argv)
    if args.sql and args.file:
        ap.error("give SQL either as the argument or with -f, not both")
    if args.list and (args.sql or args.file):
        ap.error("--list takes no SQL")
    try:
        import duckdb  # noqa: F401
    except ImportError:
        print("error: duckdb is not installed - pip install -r requirements.txt -c constraints.txt", file=sys.stderr)
        return 2

    try:
        config.load_env()
    except OSError as e:
        print("error: cannot read the env file: " + scrub(f"{type(e).__name__}: {e}")
              + " (hypercore group membership applies after you log in again)", file=sys.stderr)
        return 2
    config.setup_logging(level="INFO" if args.verbose else "WARNING", json_lines=False)
    # the loader's INFO line names the catalog URI (redacted) and warehouse: not this tool's business
    logging.getLogger("blueblocx_hyperliquid.iceberg_load").setLevel(logging.WARNING)
    try:
        bound = Bound.from_args(args.days, args.since, args.until)
    except ValueError as e:
        print(f"error: {e}", file=sys.stderr)
        return 2
    try:
        catalog, mode = il.load_catalog_from_env(read_only=True)
    except Exception as e:  # noqa: BLE001
        print("error: cannot open the Iceberg catalog: " + scrub(f"{type(e).__name__}: {e}"), file=sys.stderr)
        return 2
    lake = None
    try:
        lake = Lake(catalog, namespace=args.namespace, bound=bound, mode=mode)
        if args.list:
            _print_list(lake, args.format)
            return 0
        text = args.sql
        if text is None and args.file:
            text = Path(args.file).read_text(encoding="utf-8")
        if text is None:
            if sys.stdin.isatty():
                return repl(lake, args.format, args.max_rows)
            text = sys.stdin.read()
        stmts = split_statements(text)
        if not stmts:
            print("error: no SQL given (try --list)", file=sys.stderr)
            return 2
        rc = 0
        for stmt in stmts:
            code = execute(lake, stmt, args.format, args.max_rows)
            if code == 130:
                return 130
            rc = max(rc, code)
        return rc
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr)
        return 130
    except Exception as e:  # noqa: BLE001 — never let a traceback print the environment
        print("error: " + scrub(f"{type(e).__name__}: {e}"), file=sys.stderr)
        return 2
    finally:
        if lake is not None:
            lake.close()
        else:
            close_catalog(catalog)


if __name__ == "__main__":
    sys.exit(main())
