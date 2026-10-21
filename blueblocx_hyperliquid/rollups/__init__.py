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

Scope: wallet-derived aggregates cover the TRACKED wallet set (the lake holds only
those wallets' ledgers); the run summary and control.table_watermarks say so.

Time budget: HL_ROLLUP_TIME_BUDGET_MIN (8). A table whose turn comes after the
budget is spent is SKIPPED and reported (run summary `rollups.skipped`, a warning),
never silently; the next run picks it up because the state marker only advances
for tables that were written.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
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

    def register(self, alias: str, arrow: pa.Table) -> None:
        if alias in self._arrow:
            self.con.unregister(alias)
        self.con.register(alias, arrow)
        self._arrow[alias] = arrow
        self._loaded[alias] = arrow.num_rows

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
    """(group, tables produced, compute) in dependency order."""
    from . import activity, balances, categories, fifo, networth, positions, ranks
    return [
        ("categories", ["l1_token_categories"], categories.compute),
        ("balances", ["l4_wallet_balance_per_tx", "l4_wallet_balance_1d_dense"], balances.compute),
        ("networth", ["l4_wallet_networth_1d_dense"], networth.compute),
        ("fifo", ["l4_wallet_pnl_state_fifo", "l4_wallet_pnl_realized_1d"], fifo.compute),
        ("positions", ["wallet_token_stats", "wallet_trade_summary"], positions.compute),
        ("activity", ["wallet_whale_moves_24h", "token_netflow_24h"], activity.compute),
        ("ranks", ["wallet_overview_pnl_percentile_1d", "wallet_performance_30d", "l4_token_wallet_sharpe_1d"],
         ranks.compute),
    ]


def run(ns_dir: Path, catalog, *, now: dt.datetime | None = None, budget_min: float | None = None,
        tables: list[str] | None = None, full: bool = False, dry_run: bool = False,
        namespace: str = S.NS_HYPERCORE, read_catalog=None) -> dict:
    """Compute and write the rollup tables. Returns the report the run summary embeds.
    `read_catalog` (tests / rehearsals): read the lake inputs from another catalog
    than the one the rollup tables are written to."""
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
        wallets, newest = changed_wallets(probe, since)
    finally:
        probe.close()
    all_wallets = since is None
    report["changed_wallets"] = len(wallets)
    report["ledger_through"] = newest
    if not wallets and not full:
        report["note"] = "no new ledger rows since the previous rollup"
        # cross-wallet 24h tables still move with the clock: recompute them
        wallets = None
    ctx = Context(catalog, namespace, now, wallets=(None if all_wallets else wallets), all_wallets=all_wallets,
                  read_catalog=read_catalog)
    pending_wm: list[tuple] = []
    written_tables: list[str] = []
    try:
        for group, produced, compute in _engines():
            wanted = [t for t in produced if not tables or t in tables]
            if not wanted:
                continue
            if budget.exhausted():
                report["skipped"] += wanted
                continue
            if ctx.wallets is not None and not ctx.wallets and all(PLAN[t]["kind"] == "wallet" for t in wanted):
                continue                               # nothing changed for per-wallet tables
            t0 = time.monotonic()
            try:
                outputs = compute(ctx)
            except Exception as e:  # noqa: BLE001 - one group must not sink the others
                log.exception("rollups: %s failed", group)
                report["errors"][group] = f"{type(e).__name__}: {str(e)[:300]}"
                for t in wanted:
                    pending_wm.append((t, "updated_at", None, None, None, None, il.WM_STATUS_FAILED))
                continue
            for name in wanted:
                arrow = outputs.get(name)
                if arrow is None:
                    continue
                ctx.outputs[name] = arrow
                try:
                    counts = write_table(ctx, name, arrow, ddl, dry_run=dry_run)
                except Exception as e:  # noqa: BLE001
                    log.exception("rollups: write %s failed", name)
                    report["errors"][name] = f"{type(e).__name__}: {str(e)[:300]}"
                    pending_wm.append((name, "updated_at", None, None, None, None, il.WM_STATUS_FAILED))
                    continue
                counts["seconds"] = round(time.monotonic() - t0, 1)
                report["tables"][name] = counts
                if not dry_run and counts.get("commits"):
                    written_tables.append(name)
                    date_col = PLAN[name]["date"]
                    wm_date = il._max_value(arrow, date_col) if date_col and arrow.num_rows else None
                    if isinstance(wm_date, dt.datetime):
                        wm_date = wm_date.date()
                    pending_wm.append((name, "updated_at", now, counts.get("table_total"), counts.get("snapshot_id"),
                                       wm_date, il.WM_STATUS_OK))
                log.info("rollups: %-34s rows=%7d commits=%d %.1fs", name, counts["incoming"], counts.get("commits", 0),
                         counts["seconds"])
        if pending_wm and not dry_run:
            try:
                report["watermarks"] = il.write_watermarks(catalog, namespace, pending_wm, report["run_id"])
            except Exception as e:  # noqa: BLE001 - never fail the run over watermark rows
                report["watermarks"] = {"error": f"{type(e).__name__}: {str(e)[:200]}"}
    finally:
        ctx.close()
    report["finished_at"] = utcnow().isoformat()
    report["seconds"] = round((utcnow() - now).total_seconds(), 1)
    # the marker advances only when every per-wallet table was written for the changed
    # wallets (a skipped or failed one must see those wallets again next run)
    wallet_tables = [t for t, p in PLAN.items() if p["kind"] == "wallet" and (not tables or t in tables)]
    complete = all(t in written_tables for t in wallet_tables) and not report["errors"]
    if not dry_run and complete and newest:
        state["ledger_through"] = newest
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
