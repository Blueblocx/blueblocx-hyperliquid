"""
Rollups: the wallet-level L4 layer of the ETL contract, computed from the lake
after every lift (v1.9.0, phase A of docs/HYPERCORE_FULL_CONTRACT_PLAN.md).

    python -m blueblocx_hyperliquid.rollups                 # what the hourly run does, by hand
    python -m blueblocx_hyperliquid.rollups --full          # every tracked wallet, not only the changed ones
    python -m blueblocx_hyperliquid.rollups --tables l4_wallet_balance_per_tx,l4_wallet_balance_1d_dense
    python -m blueblocx_hyperliquid.rollups --dry-run       # compute, print counts, write nothing

Inputs are the COMMITTED Iceberg tables (l1_wallet_ledger, l1_token_metadata,
l3_token_price_usd_1d / _1h, l4x_perp_position_snapshot), read through the catalog
exactly like the ETL does (pyiceberg resolves the snapshot, DuckDB runs the SQL).
Outputs are the 13 phase-A tables (docs/PROPOSED_SCHEMA.sql, "v1.9.0 ROLLUPS"),
written with the same idempotent writer as the lift (iceberg_load.load_table):
one Iceberg commit per table per run, a reader never sees a half-replaced table.

How incrementality works. Ledger rows are immutable and stamped with the lift that
landed them (updated_at). The rollup keeps `_rollup_state.json` with the ledger
stamp it has processed through; a run recomputes the per-wallet tables for every
wallet that received ledger rows since then (a late S3 backfill or a deferred
wallet therefore simply re-triggers that wallet - no running total is ever
patched, Rule 9b of the atlas repo), and recomputes the small cross-wallet tables
whole. Per-wallet tables are partitioned by wallet_address and replaced per
wallet in one transaction.

Formulas: docs/ATLAS_WALLET_TABLE_FORMULAS.md (the atlas Rust/Trino definitions
the ETL already consumes for ethereum), ported one module per table family:
  balances.py   l4_wallet_balance_per_tx, l4_wallet_balance_1d_dense
  networth.py   l4_wallet_networth_1d_dense
  fifo.py       l4_wallet_pnl_state_fifo, l4_wallet_pnl_realized_1d
  positions.py  wallet_token_stats, wallet_trade_summary
  activity.py   wallet_whale_moves_24h, token_netflow_24h
  ranks.py      wallet_overview_pnl_percentile_1d, wallet_performance_30d, l4_token_wallet_sharpe_1d
  categories.py l1_token_categories
v1.10.0 (phases B and C, the token / market tables, from the native 1m/1h/1d candles,
the tokenDetails supply in l1_token_metadata, token_whitelist and the tracked ledger):
  market.py     market_volume_24h, market_cap_latest, market_fear_index
  tokens.py     token_stats, token_price_gainers, token_new_rank, token_beta_volatility_sharp,
                token_activity_trending, token_active_address, token_summary
  signals.py    token_market_signals          quality.py  token_quality
  pairs.py      dex_pair_volume_1d            flows.py    exchange_flow

Scope: wallet-derived aggregates cover the TRACKED wallet set (the lake holds only
those wallets' ledgers); the run summary and control.table_watermarks say so.

Time budget: HL_ROLLUP_TIME_BUDGET_MIN (8). A table whose turn comes after the
budget is spent is SKIPPED and reported (run summary `rollups.skipped`, a warning),
never silently; the next run picks it up. The per-wallet chain runs in wallet
BATCHES (HL_ROLLUP_WALLET_BATCH_ROWS ledger rows, v1.10.12) and records every
finished batch in `_rollup_state.json[wallets_through]`, so a backlog the budget
cannot absorb in one hour drains across runs (run() has the model; before, the
marker only moved when the whole chain fit the budget and a backlog never drained).
Engine ORDER (v1.10.11): the cheap global groups
(categories, market, tokens, signals, quality, pairs, flows, activity: seconds
each, no wallet inputs) run first, the heavy per-wallet chain (balances ->
networth -> fifo -> positions -> ranks: minutes) last, so a run that cannot afford
everything skips wallet tables, never market_cap_latest (_engines() has the
dependency rationale).

Memory (v1.10.11): the run's DuckDB is capped at HL_ROLLUP_DUCKDB_MEMORY (32GB,
spilling to <HL_OUT_DIR>/.duckdb/rollups) on HL_ROLLUP_DUCKDB_THREADS (4) threads,
and the per-wallet engines process wallets in chunks of about
HL_ROLLUP_BALANCE_CHUNK_ROWS (2,000,000) ledger rows, converting each chunk to Arrow
before the next one is read - the Python-dict peak is one chunk, not the whole
ledger.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import re
import sys
import time
import uuid
from pathlib import Path
from typing import Callable

import pyarrow as pa

from .. import config
from .. import iceberg_load as il
from .. import schema as S
from ..ops import LockHeld, RunLock, atomic_write_text, utcnow

log = logging.getLogger("blueblocx_hyperliquid.rollups")

STATE_FILE = "_rollup_state.json"
# how far back the lake is read for the windowed tables (performance_30d needs 30 days
# of networth, sharpe 30 days of balances + 7 of prices; a generous floor)
READ_DAYS = 400

# (table, natural key, write mode, scope columns, business-date column)
#   wallet  = recomputed per changed wallet, replaced per wallet (scope = wallet_address)
#   global  = recomputed whole, replaced whole
#   dated   = recomputed for a set of dates, replaced per date (scope = the date column)
PLAN: dict[str, dict] = {
    "l1_token_categories":              dict(key=["token_address", "token_group", "category"], kind="global", date=None),
    "l4_wallet_balance_per_tx":         dict(key=["wallet_address", "token_address", "tx_hash", "event_index", "direction"], kind="wallet", date="block_date"),
    "l4_wallet_balance_1d_dense":       dict(key=["wallet_address", "token_address", "snapshot_date"], kind="wallet", date="snapshot_date"),
    "l4_wallet_networth_1d_dense":      dict(key=["wallet_address", "snapshot_date"], kind="wallet", date="snapshot_date"),
    "l4_wallet_pnl_state_fifo":         dict(key=["wallet_address", "token_address"], kind="wallet", date=None),
    "l4_wallet_pnl_realized_1d":        dict(key=["wallet_address", "snapshot_date"], kind="wallet", date="snapshot_date"),
    "wallet_token_stats":               dict(key=["wallet_address", "token_address"], kind="wallet", date=None),
    "wallet_trade_summary":             dict(key=["wallet_address", "token_address"], kind="wallet", date="last_activity_date"),
    "wallet_whale_moves_24h":           dict(key=["wallet_address", "token_address", "direction"], kind="global", date=None),
    "token_netflow_24h":                dict(key=["token_address"], kind="global", date=None),
    "wallet_overview_pnl_percentile_1d": dict(key=["wallet_address", "computed_date"], kind="dated", date="computed_date"),
    "l4_token_wallet_sharpe_1d":        dict(key=["token_address", "wallet_address", "snapshot_date"], kind="dated", date="snapshot_date"),
    "wallet_performance_30d":           dict(key=["wallet_address", "as_of_date"], kind="dated", date="as_of_date"),
    # v1.10.0 (phases B and C): token / market tables
    "market_volume_24h":                dict(key=["hour_ts"], kind="global", date=None),
    "market_cap_latest":                dict(key=["token_address", "as_of_date"], kind="dated", date="as_of_date"),
    "market_fear_index":                dict(key=["as_of_date"], kind="dated", date="as_of_date"),
    "token_stats":                      dict(key=["token_address"], kind="global", date=None),
    "token_price_gainers":              dict(key=["token_address", "timeframe", "current_ts"], kind="global", date=None),
    "token_new_rank":                   dict(key=["token_address", "current_ts"], kind="global", date=None),
    "token_beta_volatility_sharp":      dict(key=["token_address"], kind="global", date=None),
    "token_activity_trending":          dict(key=["token_address", "activity_date"], kind="dated", date="activity_date"),
    "token_active_address":             dict(key=["token_address"], kind="global", date=None),
    "token_summary":                    dict(key=["token_address"], kind="global", date=None),
    "token_market_signals":             dict(key=["global_flag", "token_address", "bucket_minute"], kind="dated", date="bucket_date"),
    "token_quality":                    dict(key=["as_of_date", "quality_version", "token_address"], kind="dated", date="as_of_date"),
    "dex_pair_volume_1d":               dict(key=["pair_address", "bucket_date"], kind="dated", date="bucket_date"),
    "exchange_flow":                    dict(key=["bucket_date"], kind="dated", date="bucket_date"),
}
# NOT NULL by construction (checked before any commit, like the lift)
REQUIRED: dict[str, list[str]] = {name: list(p["key"]) + ["updated_at"] for name, p in PLAN.items()}


class Budget:
    def __init__(self, minutes: float, started: float | None = None):
        self.started = started if started is not None else time.monotonic()
        self.seconds = max(0.0, float(minutes or 0)) * 60.0

    def left(self) -> float:
        return self.seconds - (time.monotonic() - self.started) if self.seconds else float("inf")

    def exhausted(self) -> bool:
        return self.seconds > 0 and self.left() <= 0


class Context:
    """One rollup run: the catalog, a DuckDB with the lake tables registered lazily,
    the instant everything is stamped with, and the wallets to recompute."""

    def __init__(self, catalog, namespace: str, now: dt.datetime, wallets: list[str] | None,
                 all_wallets: bool, read_days: int = READ_DAYS, read_catalog=None):
        import duckdb
        self.catalog = catalog                 # where the rollup tables are written
        self.read_catalog = read_catalog or catalog   # where the lake inputs are read (same in production)
        self.namespace = namespace
        self.now = now
        self.now_iso = now.isoformat()
        self.today = now.date()
        self.wallets = wallets                 # None = every wallet in the lake
        self.all_wallets = all_wallets
        self.read_since = self.today - dt.timedelta(days=read_days)
        self.con = duckdb.connect(database=":memory:")
        self.con.execute("SET TimeZone = 'UTC'")
        self._configure_duckdb()               # before external access is locked: temp_directory is frozen by it
        self.con.execute("SET enable_external_access = false")
        self._loaded: dict[str, int] = {}
        self._arrow: dict[str, pa.Table] = {}
        self.outputs: dict[str, pa.Table] = {}  # tables computed this run (inputs for later engines)

    # -- lake reads ----------------------------------------------------------------
    def _table(self, name: str):
        cat = self.catalog if name in PLAN else self.read_catalog
        return cat.load_table(f"{self.namespace}.{name}")

    def load(self, name: str, *, date_col: str | None = None, wallet_col: str | None = "wallet_address",
             since: dt.date | None = None, alias: str | None = None) -> int:
        """Scan one lake table into DuckDB as `alias or name` (idempotent per alias).
        Bounded on `date_col` >= since (default the read floor) and, for per-wallet
        tables, on the wallets being recomputed. Returns the row count."""
        alias = alias or name
        if alias in self._loaded:
            return self._loaded[alias]
        from pyiceberg.expressions import AlwaysTrue, And, GreaterThanOrEqual, In
        table = self._table(name)
        flt = AlwaysTrue()
        if date_col:
            flt = And(flt, GreaterThanOrEqual(date_col, (since or self.read_since).isoformat()))
        if wallet_col and self.wallets is not None and wallet_col in {f.name for f in table.schema().fields}:
            if not self.wallets:
                arrow = table.scan(row_filter=flt, limit=0).to_arrow()
            else:
                flt = And(flt, In(wallet_col, list(self.wallets)))
                arrow = table.scan(row_filter=flt).to_arrow()
        else:
            arrow = table.scan(row_filter=flt).to_arrow()
        self.register(alias, arrow)
        log.info("rollups: loaded %s as %s: %d rows", name, alias, arrow.num_rows)
        return arrow.num_rows

    def _configure_duckdb(self) -> None:
        """Memory controls (v1.10.11). DuckDB's default memory_limit is 80 % of the
        host's RAM (~98 GB on the 123 GB production host), above the unit's cgroup cap,
        so the kernel's OOM killer fired before DuckDB ever spilled (2026-11-07 11:10 UTC).
        The limit is HL_ROLLUP_DUCKDB_MEMORY (32GB), the spill directory lives under
        HL_OUT_DIR (the service user owns it; the system temp dir may be a small tmpfs),
        the thread count is HL_ROLLUP_DUCKDB_THREADS (4: the rollup SQL is small, the
        Python engines are single-threaded anyway) and insertion order is not preserved
        (every consumer sorts or groups; this frees DuckDB to stream)."""
        settings = self.duckdb_settings()
        self.temp_dir = settings["temp_directory"]
        try:
            Path(self.temp_dir).mkdir(parents=True, exist_ok=True)
        except OSError as e:  # noqa: BLE001 - a read-only HL_OUT_DIR: run without a spill dir
            log.warning("rollups: cannot create the DuckDB spill dir %s (%s); spilling disabled", self.temp_dir, e)
            self.temp_dir = None
        self.con.execute(f"SET memory_limit = '{settings['memory_limit']}'")
        self.con.execute(f"SET threads = {int(settings['threads'])}")
        if self.temp_dir:
            self.con.execute(f"SET temp_directory = '{self.temp_dir.replace(chr(39), chr(39) * 2)}'")
        self.con.execute("SET preserve_insertion_order = false")

    @staticmethod
    def duckdb_settings() -> dict:
        """The DuckDB settings a rollup Context applies, resolved from the environment
        (config.ENV_VARS defaults when a key is absent or empty)."""
        memory = (config.env("HL_ROLLUP_DUCKDB_MEMORY") or "32GB").strip()
        if not re.fullmatch(r"\d+(\.\d+)?\s*[KMGT]i?B", memory, flags=re.IGNORECASE):
            raise ValueError(f"HL_ROLLUP_DUCKDB_MEMORY must look like '32GB', got {memory!r}")
        threads = max(1, config.env_int("HL_ROLLUP_DUCKDB_THREADS", 4))
        return {"memory_limit": memory, "threads": threads,
                "temp_directory": str(config.out_root() / ".duckdb" / "rollups"),
                "preserve_insertion_order": False}

    def register(self, alias: str, arrow: pa.Table) -> None:
        if alias in self._arrow:
            self.con.unregister(alias)
        self.con.register(alias, arrow)
        self._arrow[alias] = arrow
        self._loaded[alias] = arrow.num_rows

    def unregister(self, alias: str) -> None:
        """Drop a registered Arrow table (a scratch input an engine no longer needs)."""
        if alias in self._arrow:
            try:
                self.con.unregister(alias)
            except Exception:  # noqa: BLE001
                pass
            self._arrow.pop(alias, None)
            self._loaded.pop(alias, None)

    def has_table(self, name: str) -> bool:
        try:
            self._table(name)
            return True
        except Exception:  # noqa: BLE001 - NoSuchTableError and friends
            return False

    def sql(self, query: str) -> pa.Table:
        cur = self.con.execute(query)
        return cur.to_arrow_table() if hasattr(cur, "to_arrow_table") else cur.fetch_arrow_table()

    def close(self) -> None:
        try:
            self.con.close()
        except Exception:  # noqa: BLE001
            pass


# --- state ------------------------------------------------------------------------------

def read_state(ns_dir: Path) -> dict:
    p = Path(ns_dir) / STATE_FILE
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001 - a corrupt marker just means a full recompute
        return {}


def write_state(ns_dir: Path, state: dict) -> None:
    atomic_write_text(Path(ns_dir) / STATE_FILE, json.dumps(state, indent=2, default=str))


def changed_wallets(ctx: Context, since_iso: str | None) -> tuple[list[str], str | None]:
    """Wallets with ledger rows stamped after `since_iso`, and the newest ledger stamp
    seen (the next marker). None since = every wallet in the ledger."""
    from pyiceberg.expressions import AlwaysTrue, GreaterThan
    table = ctx._table("l1_wallet_ledger")
    flt = GreaterThan("updated_at", since_iso) if since_iso else AlwaysTrue()
    arrow = table.scan(row_filter=flt, selected_fields=("wallet_address", "updated_at")).to_arrow()
    if arrow.num_rows == 0:
        return [], since_iso
    import pyarrow.compute as pc
    wallets = sorted(set(arrow.column("wallet_address").to_pylist()))
    newest = pc.max(arrow.column("updated_at")).as_py()
    return wallets, (newest.isoformat() if newest else since_iso)


# --- writing -----------------------------------------------------------------------------

def _stamp(arrow: pa.Table, now: dt.datetime, cols=("created_at", "updated_at")) -> pa.Table:
    """Set the write stamps on every row (created_at only when the table has it)."""
    n = arrow.num_rows
    for c in cols:
        if c in arrow.column_names:
            arrow = arrow.set_column(arrow.schema.get_field_index(c), c,
                                     pa.array([now] * n, pa.timestamp("us", tz="UTC")))
        elif c == "updated_at":
            arrow = arrow.append_column(c, pa.array([now] * n, pa.timestamp("us", tz="UTC")))
    return arrow


def write_table(ctx: Context, name: str, arrow: pa.Table, ddl: dict, *, dry_run: bool = False) -> dict:
    """Replace the rows this run recomputed: per wallet (kind=wallet), per date
    (kind=dated) or the whole table (kind=global), in ONE transaction, after the same
    NULL-key / duplicate / schema-drift guards as the lift."""
    plan = PLAN[name]
    spec = ddl[name]
    arrow = _stamp(arrow, ctx.now)
    # DDL column order, DDL types (the engines build typed tables from the DDL; this is
    # the safety net for a column an engine left out)
    cols = [c[0] for c in spec["columns"]]
    for c, t in spec["columns"]:
        want = il._arrow_type(t, True)
        if c not in arrow.column_names:
            arrow = arrow.append_column(c, pa.nulls(arrow.num_rows, want))
        elif arrow.schema.field(c).type != want:
            arrow = arrow.set_column(arrow.schema.get_field_index(c), c, arrow.column(c).cast(want))
    arrow = arrow.select(cols)
    il.check_batch(name, arrow, plan["key"], REQUIRED[name])
    if dry_run:
        return {"incoming": arrow.num_rows, "written": 0, "commits": 0, "dry_run": True}
    table = il.ensure_table(ctx.catalog, ctx.namespace, name, spec, tz=True, required=REQUIRED[name])
    drift = il.schema_drift(table, spec)
    if drift:
        raise il.SchemaDriftError(f"{ctx.namespace}.{name}: schema drift {drift} - run "
                                  f"`python -m blueblocx_hyperliquid.migrate --apply`")
    arrow = il.conform(arrow, table)
    from pyiceberg.expressions import AlwaysTrue, EqualTo, In
    kind = plan["kind"]
    if kind == "global" or (kind == "wallet" and ctx.all_wallets):
        preds = [AlwaysTrue()]
    elif kind == "wallet":
        wallets = sorted(set(arrow.column("wallet_address").to_pylist()) | set(ctx.wallets or []))
        preds = [In("wallet_address", wallets)] if wallets else []
    else:  # dated
        dates = sorted(set(arrow.column(plan["date"]).to_pylist()))
        preds = [In(plan["date"], dates)] if dates else []
    if arrow.num_rows == 0 and not preds:
        return {"incoming": 0, "written": 0, "commits": 0}
    if arrow.num_rows == 0:
        # nothing to write for these wallets/dates but their previous rows must go
        # (e.g. a wallet whose last position closed): delete-only transaction
        import warnings
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message=il._NO_MATCH_WARNING)
            with table.transaction() as tx:
                for p in preds:
                    tx.delete(p)
        table.refresh()
        return {"incoming": 0, "written": 0, "commits": 1, "deleted_scope": True}
    commits = il._replace(table, arrow, preds)
    table.refresh()
    return {"incoming": arrow.num_rows, "written": arrow.num_rows, "commits": commits,
            "table_total": il._table_row_count(table),
            "snapshot_id": (table.current_snapshot().snapshot_id if table.current_snapshot() else None)}


# --- engines --------------------------------------------------------------------------------

def _engines() -> list[tuple[str, list[str], Callable[[Context], dict[str, pa.Table]]]]:
    """(group, tables produced, compute) in run order.

    Order rationale (v1.10.11). The time budget skips whatever comes after it is
    spent, so the groups are ordered cheap-and-independent first, heavy last:

    1. the GLOBAL groups, seconds each, read only lake inputs (meta, candles, the
       whitelist, the last days of every wallet's ledger) and nothing another engine
       registers: categories, market (market_volume_24h, market_cap_latest,
       market_fear_index), tokens, signals, quality, pairs, flows, activity. They
       always land, whatever the per-wallet backlog costs that hour.
    2. the PER-WALLET chain, minutes for a large changed set, in dependency order:
       balances registers `balance_per_tx_new`, `balance_1d_new`, `balance_openings`
       -> networth reads `balance_1d_new`; fifo reads `balance_openings` (the
       pre-history opening lots) and registers `fifo_state_new`, `fifo_pair_realized`,
       `priced_statement` -> positions reads those three -> ranks reads the WRITTEN
       l4_wallet_pnl_realized_1d / l4_wallet_networth_1d_dense / l4_wallet_balance_*
       back from the lake, so it comes last.
    networth, fifo and positions call their producer themselves when its tables were
    not registered this run (a --tables subset), which keeps the subset path correct.
    run() executes the chain per wallet batch (WALLET_CHAIN, _run_wallet_chain).
    """
    from . import activity, balances, categories, fifo, flows, market, networth, pairs, positions, quality, ranks, signals, tokens
    return [
        # -- global groups: cheap, independent of every other engine --------------------
        ("categories", ["l1_token_categories"], categories.compute),
        ("market", ["market_volume_24h", "market_cap_latest", "market_fear_index"], market.compute),
        ("tokens", ["token_stats", "token_price_gainers", "token_new_rank", "token_beta_volatility_sharp",
                    "token_activity_trending", "token_active_address", "token_summary"], tokens.compute),
        ("signals", ["token_market_signals"], signals.compute),
        ("quality", ["token_quality"], quality.compute),
        ("pairs", ["dex_pair_volume_1d"], pairs.compute),
        ("flows", ["exchange_flow"], flows.compute),
        ("activity", ["wallet_whale_moves_24h", "token_netflow_24h"], activity.compute),
        # -- per-wallet chain: producers before consumers ---------------------------------
        ("balances", ["l4_wallet_balance_per_tx", "l4_wallet_balance_1d_dense"], balances.compute),
        ("networth", ["l4_wallet_networth_1d_dense"], networth.compute),
        ("fifo", ["l4_wallet_pnl_state_fifo", "l4_wallet_pnl_realized_1d"], fifo.compute),
        ("positions", ["wallet_token_stats", "wallet_trade_summary"], positions.compute),
        ("ranks", ["wallet_overview_pnl_percentile_1d", "wallet_performance_30d", "l4_token_wallet_sharpe_1d"],
         ranks.compute),
    ]


# the groups that read nothing another engine registers (tests pin the order)
GLOBAL_GROUPS = ("categories", "market", "tokens", "signals", "quality", "pairs", "flows", "activity")
# the per-wallet chain, run per wallet BATCH with the progress recorded after each batch
# (v1.10.12; run() has the rationale); ranks follows it, reading the written tables back
WALLET_CHAIN = ("balances", "networth", "fifo", "positions")
# aliases bound to the wallets of ONE batch (the ledger slice, its anchors and what the
# chain engines register for each other): dropped between batches
BATCH_ALIASES = ("ledger", "spot_snap", "balance_per_tx_new", "balance_1d_new", "balance_openings",
                 "networth_1d_new", "fifo_state_new", "fifo_pair_realized", "priced_statement")
# consumer group -> the producer groups that must have run before it
ENGINE_DEPENDENCIES: dict[str, tuple[str, ...]] = {
    "networth": ("balances",),
    "fifo": ("balances",),
    "positions": ("fifo",),
    "ranks": ("balances", "networth", "fifo"),
}


def wallet_batch_rows() -> int:
    """HL_ROLLUP_WALLET_BATCH_ROWS (8,000,000): about how many ledger rows of changed
    wallets the per-wallet chain processes between two progress marks (one batch =
    balances -> networth -> fifo -> positions for its wallets, written, then recorded
    in `_rollup_state.json[wallets_through]`)."""
    return max(1, config.env_int("HL_ROLLUP_WALLET_BATCH_ROWS", 8_000_000))


def _later(a: str | None, b: str | None) -> bool:
    """Stamp `a` is later than stamp `b` (None = never)."""
    if a is None:
        return False
    if b is None:
        return True
    try:
        return dt.datetime.fromisoformat(a) > dt.datetime.fromisoformat(b)
    except ValueError:
        return a > b


def changed_wallet_stamps(ctx: Context, since_iso: str | None) -> dict[str, str]:
    """wallet -> its newest ledger stamp (updated_at) among the rows stamped after
    `since_iso` (None since = every wallet in the ledger, with its newest stamp)."""
    from pyiceberg.expressions import AlwaysTrue, GreaterThan
    table = ctx._table("l1_wallet_ledger")
    flt = GreaterThan("updated_at", since_iso) if since_iso else AlwaysTrue()
    arrow = table.scan(row_filter=flt, selected_fields=("wallet_address", "updated_at")).to_arrow()
    if arrow.num_rows == 0:
        return {}
    agg = arrow.group_by("wallet_address").aggregate([("updated_at", "max")])
    return {w: s.isoformat() for w, s in zip(agg.column("wallet_address").to_pylist(),
                                             agg.column("updated_at_max").to_pylist()) if w is not None and s is not None}


def changed_wallets(ctx: Context, since_iso: str | None) -> tuple[list[str], str | None]:
    """Wallets with ledger rows stamped after `since_iso`, and the newest ledger stamp
    seen (the next marker). None since = every wallet in the ledger."""
    stamps = changed_wallet_stamps(ctx, since_iso)
    return sorted(stamps), newest_stamp(stamps, since_iso)


def newest_stamp(stamps: dict[str, str], default: str | None) -> str | None:
    newest = default
    for s in stamps.values():
        if _later(s, newest):
            newest = s
    return newest


def pending_wallets(stamps: dict[str, str], state: dict, full: bool) -> dict[str, str]:
    """The changed wallets the chain still has to process: those whose newest stamp is
    later than what `_rollup_state.json[wallets_through]` recorded for them (a batch
    written by an earlier run that could not finish the whole set). --full: all."""
    done = {} if full else (state.get("wallets_through") or {})
    return {w: s for w, s in stamps.items() if full or _later(s, done.get(w))}


def _merge_counts(tables: dict, name: str, counts: dict) -> None:
    """Accumulate a table's write counts across the wallet batches of one run."""
    cur = tables.get(name)
    if cur is None:
        tables[name] = dict(counts)
        return
    for k in ("incoming", "written", "commits", "seconds"):
        if counts.get(k) is not None:
            cur[k] = round((cur.get(k) or 0) + counts[k], 1) if k == "seconds" else (cur.get(k) or 0) + counts[k]
    for k in ("table_total", "snapshot_id", "dry_run", "deleted_scope"):
        if k in counts:
            cur[k] = counts[k]
    cur["batches"] = cur.get("batches", 1) + 1


def _run_group(ctx: Context, group: str, wanted: list[str], compute, *, ddl: dict, now: dt.datetime, dry_run: bool,
               report: dict, pending_wm: list, written_tables: list[str], t0: float | None = None) -> bool:
    """Compute one group and write its wanted tables. Returns True when every wanted
    table was written (dry run: computed); an exception in the engine or a writer is
    recorded in report['errors'] and never sinks the other groups."""
    t0 = time.monotonic() if t0 is None else t0
    try:
        outputs = compute(ctx)
    except Exception as e:  # noqa: BLE001 - one group must not sink the others
        log.exception("rollups: %s failed", group)
        report["errors"][group] = f"{type(e).__name__}: {str(e)[:300]}"
        for t in wanted:
            pending_wm.append((t, "updated_at", None, None, None, None, il.WM_STATUS_FAILED))
        return False
    ok = True
    for name in wanted:
        arrow = outputs.get(name)
        if arrow is None:
            ok = False
            continue
        ctx.outputs[name] = arrow
        try:
            counts = write_table(ctx, name, arrow, ddl, dry_run=dry_run)
        except Exception as e:  # noqa: BLE001
            log.exception("rollups: write %s failed", name)
            report["errors"][name] = f"{type(e).__name__}: {str(e)[:300]}"
            pending_wm.append((name, "updated_at", None, None, None, None, il.WM_STATUS_FAILED))
            ok = False
            continue
        counts["seconds"] = round(time.monotonic() - t0, 1)
        _merge_counts(report["tables"], name, counts)
        if not dry_run:
            if counts.get("commits"):
                written_tables.append(name)
                date_col = PLAN[name]["date"]
                wm_date = il._max_value(arrow, date_col) if date_col and arrow.num_rows else None
                if isinstance(wm_date, dt.datetime):
                    wm_date = wm_date.date()
                pending_wm.append((name, "updated_at", now, counts.get("table_total"), counts.get("snapshot_id"),
                                   wm_date, il.WM_STATUS_OK))
            else:
                ok = False
        log.info("rollups: %-34s rows=%7d commits=%d %.1fs", name, counts["incoming"], counts.get("commits", 0),
                 counts["seconds"])
    return ok


def _run_wallet_chain(ctx: Context, chain: list[tuple[str, list[str], Callable]], *, stamps: dict[str, str],
                      pending: dict[str, str], full: bool, budget: Budget, state: dict, ns_dir: Path, ddl: dict,
                      now: dt.datetime, dry_run: bool, record_progress: bool, report: dict, pending_wm: list,
                      written_tables: list[str], batch_rows: int | None = None) -> bool:
    """balances -> networth -> fifo -> positions for the pending wallets, one wallet
    BATCH at a time (about `batch_rows` ledger rows, a wallet never split): every batch
    is computed, written and - when `record_progress` - recorded in
    state['wallets_through'] before the next one starts, and the time budget is
    checked between batches. Returns True when every pending wallet was processed."""
    from .common import ledger_chunk, wallet_chunks
    wallets = sorted(pending)
    # the ledger of every pending wallet (READ_DAYS), loaded from the lake ONCE as Arrow
    # and sliced per batch below (a lake scan per batch would re-read every data file)
    ctx.wallets = None if (full and ctx.all_wallets) else wallets
    ctx.unregister("ledger_changed")
    ctx.load("l1_wallet_ledger", date_col="block_date", alias="ledger_changed")
    batches = wallet_chunks(ctx, "ledger_changed", batch_rows or wallet_batch_rows())
    seen = {w for b in batches for w in b}
    rest = [w for w in wallets if w not in seen]        # changed, but no ledger row inside READ_DAYS
    if rest:
        batches.append(rest)
    ctx.all_wallets = False                             # every write below is scoped to its batch's wallets
    report["wallet_batches"] = len(batches)
    report.setdefault("wallets_done", 0)
    report.setdefault("wallets_failed", 0)
    done_all = True
    for i, batch in enumerate(batches, 1):
        if budget.exhausted():
            left = sum(len(b) for b in batches[i - 1:])
            report["wallets_pending"] = left
            for _g, wanted, _c in chain:
                report["skipped"] += [t for t in wanted if t not in report["skipped"]]
            log.warning("rollups: time budget spent after %d of %d wallet batches; %d wallets wait for the next run",
                        i - 1, len(batches), left)
            done_all = False
            break
        t0 = time.monotonic()
        for alias in BATCH_ALIASES:
            ctx.unregister(alias)
        ctx.wallets = list(batch)
        ctx.register("ledger", ledger_chunk(ctx, list(batch), "SELECT * FROM ledger_changed"))
        ok = True
        for group, wanted, compute in chain:
            ok = _run_group(ctx, group, wanted, compute, ddl=ddl, now=now, dry_run=dry_run, report=report,
                            pending_wm=pending_wm, written_tables=written_tables) and ok
        if not ok:
            report["wallets_failed"] += len(batch)
            done_all = False
            log.error("rollups: wallet batch %d/%d (%d wallets) failed; its wallets stay pending", i, len(batches), len(batch))
            continue
        report["wallets_done"] += len(batch)
        if not dry_run and record_progress:
            through = state.setdefault("wallets_through", {})
            for w in batch:
                if w in stamps:
                    through[w] = stamps[w]
            write_state(ns_dir, state)
        log.info("rollups: wallet batch %d/%d (%d wallets, %d ledger rows) done in %.1fs", i, len(batches), len(batch),
                 ctx._loaded.get("ledger", 0), time.monotonic() - t0)
    for alias in BATCH_ALIASES + ("ledger_changed",):
        ctx.unregister(alias)
    return done_all


def run(ns_dir: Path, catalog, *, now: dt.datetime | None = None, budget_min: float | None = None,
        tables: list[str] | None = None, full: bool = False, dry_run: bool = False,
        namespace: str = S.NS_HYPERCORE, read_catalog=None, batch_rows: int | None = None) -> dict:
    """Compute and write the rollup tables. Returns the report the run summary embeds.
    `read_catalog` (tests / rehearsals): read the lake inputs from another catalog
    than the one the rollup tables are written to.

    Progress model (v1.10.12). The global groups run first (seconds each). The
    per-wallet chain then runs in wallet batches of about HL_ROLLUP_WALLET_BATCH_ROWS
    ledger rows; after each batch its wallets are recorded in
    `_rollup_state.json[wallets_through]` (wallet -> the newest ledger stamp folded),
    and the budget is checked BETWEEN batches. A run that runs out of budget leaves
    the finished batches durable; the next run's `changed_wallets` only sees wallets
    whose newest stamp is later than their recorded one, so a backlog drains across
    runs instead of being recomputed from scratch each hour (before v1.10.12 the
    marker `ledger_through` only moved when the WHOLE chain fit the budget: at ~60
    changed wallets it never did again, and every hour redid the same work). When
    every changed wallet is done, `ledger_through` becomes the newest stamp and
    `wallets_through` is cleared. Progress is recorded only when every wallet-kind
    table is in scope (no --tables subset of the chain) and never on --dry-run.
    `ranks` (cross-wallet, reads the written tables back) runs after the chain."""
    now = now or utcnow()
    budget = Budget(config.env_float("HL_ROLLUP_TIME_BUDGET_MIN") if budget_min is None else budget_min)
    state = read_state(ns_dir)
    since = None if full else state.get("ledger_through")
    report: dict = {"run_id": uuid.uuid4().hex[:12], "started_at": now.isoformat(), "since": since,
                    "full": full, "dry_run": dry_run, "tables": {}, "skipped": [], "errors": {}}
    ddl = il.parse_ddl()
    probe = Context(catalog, namespace, now, wallets=None, all_wallets=True, read_catalog=read_catalog)
    try:
        if not probe.has_table("l1_wallet_ledger"):
            report["skipped"] = list(PLAN)
            report["note"] = "no ledger in the lake yet"
            return report
        stamps = changed_wallet_stamps(probe, since)
    finally:
        probe.close()
    newest = newest_stamp(stamps, since)
    pending = pending_wallets(stamps, state, full)
    all_wallets = since is None
    report["changed_wallets"] = len(stamps)
    report["wallets_pending_before"] = len(pending)
    report["ledger_through"] = newest
    if not stamps and not full:
        report["note"] = "no new ledger rows since the previous rollup"
    elif not pending:
        report["note"] = "every changed wallet was already rolled up by an earlier run"
    wallet_tables = [t for t, p in PLAN.items() if p["kind"] == "wallet"]
    # the marker moves only when the whole chain is in scope: a --tables subset of it
    # must not mark wallets done for the tables it did not refresh
    record_progress = all(not tables or t in tables for t in wallet_tables)
    ctx = Context(catalog, namespace, now, wallets=(None if all_wallets else sorted(pending)), all_wallets=all_wallets,
                  read_catalog=read_catalog)
    pending_wm: list[tuple] = []
    written_tables: list[str] = []
    chain_done = True
    try:
        engines = _engines()
        chain = [(g, [t for t in p if not tables or t in tables], c) for g, p, c in engines if g in WALLET_CHAIN]
        chain = [(g, w, c) for g, w, c in chain if w]
        chain_ran = False
        for group, produced, compute in engines:
            if group in WALLET_CHAIN:
                if chain_ran or not chain:
                    continue
                chain_ran = True
                if not pending:
                    continue                           # nothing changed for the per-wallet tables
                if budget.exhausted():
                    for _g, wanted, _c in chain:
                        report["skipped"] += wanted
                    report["wallets_pending"] = len(pending)
                    chain_done = False
                    continue
                chain_done = _run_wallet_chain(ctx, chain, stamps=stamps, pending=pending, full=full, budget=budget,
                                               state=state, ns_dir=ns_dir, ddl=ddl, now=now, dry_run=dry_run,
                                               record_progress=record_progress, report=report, pending_wm=pending_wm,
                                               written_tables=written_tables, batch_rows=batch_rows)
                continue
            wanted = [t for t in produced if not tables or t in tables]
            if not wanted:
                continue
            if budget.exhausted():
                report["skipped"] += wanted
                continue
            _run_group(ctx, group, wanted, compute, ddl=ddl, now=now, dry_run=dry_run, report=report,
                       pending_wm=pending_wm, written_tables=written_tables)
        if pending_wm and not dry_run:
            last: dict[str, tuple] = {}
            for e in pending_wm:                       # one row per table: the last batch's entry
                last[e[0]] = e
            try:
                report["watermarks"] = il.write_watermarks(catalog, namespace, list(last.values()), report["run_id"])
            except Exception as e:  # noqa: BLE001 - never fail the run over watermark rows
                report["watermarks"] = {"error": f"{type(e).__name__}: {str(e)[:200]}"}
    finally:
        ctx.close()
    report["finished_at"] = utcnow().isoformat()
    report["seconds"] = round((utcnow() - now).total_seconds(), 1)
    # the marker advances only when every changed wallet went through the whole chain
    # (the batches an interrupted run finished are kept in wallets_through either way)
    if not dry_run and record_progress and chain_done and newest:
        state["ledger_through"] = newest
        state.pop("wallets_through", None)
        state["last_run_iso"] = now.isoformat()
        state["tables"] = {k: v.get("table_total") for k, v in report["tables"].items()}
        write_state(ns_dir, state)
        report["state_advanced"] = True
    else:
        report["state_advanced"] = False
    return report


def read_catalog_from_env_file(path: Path):
    """A READ-ONLY pyiceberg SQL catalog from a reader env file (HL_CATALOG_URI,
    WAREHOUSE) without touching the process environment."""
    from pyiceberg.catalog import load_catalog
    values: dict[str, str] = {}
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        values[k.strip()] = v.strip().strip('"').strip("'")
    uri = values.get("HL_CATALOG_URI")
    if not uri:
        raise ValueError(f"{path}: no HL_CATALOG_URI")
    props = {"type": "sql", "uri": uri, "warehouse": values.get("WAREHOUSE") or "",
             "py-io-impl": il.DEFAULT_PY_IO_IMPL, "init_catalog_tables": "false"}
    # the SQL catalog filters iceberg_tables by catalog_name: it must be the production
    # catalog's name ("hl", iceberg_load.load_catalog_from_env), not an arbitrary label
    return load_catalog("hl", **props)


def main(argv=None) -> int:
    config.load_env()
    config.setup_logging()
    ap = argparse.ArgumentParser(description="Compute the wallet-level L4 rollup tables from the lake")
    ap.add_argument("--out", default=str(config.out_root()))
    ap.add_argument("--namespace", default=S.NS_HYPERCORE)
    ap.add_argument("--tables", default=None, help="comma-separated subset")
    ap.add_argument("--full", action="store_true", help="recompute every wallet (ignore the state marker)")
    ap.add_argument("--dry-run", action="store_true", help="compute and report, write nothing")
    ap.add_argument("--budget-min", type=float, default=None, help="time budget (default HL_ROLLUP_TIME_BUDGET_MIN)")
    ap.add_argument("--local-dir", default=None)
    ap.add_argument("--read-env", default=None,
                    help="rehearsal: read the lake inputs from the catalog this env file describes "
                         "(HL_CATALOG_URI + WAREHOUSE, e.g. /etc/hypercore/reader.env, read-only) while "
                         "writing the rollup tables to the catalog of the current environment")
    args = ap.parse_args(argv)
    ns_dir = Path(args.out) / args.namespace
    tables = [t for t in args.tables.split(",") if t] if args.tables else None
    try:
        with RunLock(ns_dir / ".daily.lock"):
            catalog, _ = il.load_catalog_from_env(Path(args.local_dir) if args.local_dir else None)
            read_catalog = read_catalog_from_env_file(Path(args.read_env)) if args.read_env else None
            try:
                rep = run(ns_dir, catalog, budget_min=args.budget_min, tables=tables, full=args.full,
                          dry_run=args.dry_run, namespace=args.namespace, read_catalog=read_catalog)
            finally:
                il.dispose_catalog(catalog)
                if read_catalog is not None:
                    il.dispose_catalog(read_catalog)
    except LockHeld as e:
        log.error("%s - another run owns the state dir; retry when it has finished", e)
        return 3
    print(json.dumps(rep, indent=2, default=str))
    return 0 if not rep.get("errors") else 1


if __name__ == "__main__":
    sys.exit(main())
