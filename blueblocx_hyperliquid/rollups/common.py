"""Shared helpers for the rollup engines: input loading, price lookups, Arrow builders."""
from __future__ import annotations

import datetime as dt
import math
from typing import Iterable

import pyarrow as pa

from .. import schema as S

EPOCH = dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)
BALANCE_EPS = 1e-9          # wallet-networth BALANCE_EPS
QTY_EPSILON = 1e-12         # fifo-pnl QTY_EPSILON
ETH_SYMBOL = "UETH"         # decision 3A: the ETH benchmark is the UETH/USDC spot market

# the ledger columns every engine reads
LEDGER_COLS = ("wallet_address", "token_address", "counterparty_address", "block_timestamp", "block_date",
               "tx_hash", "event_index", "event_type", "direction", "amount_raw", "amount", "amount_signed",
               "decimals")


# the 1-minute candles an engine may read back (the tip-anchored views look 30 days
# back for the "24h ago" bucket; signals / quality need a few days of warm-up)
PX1M_DAYS = 31

# l1_token_metadata total supply in token units (the atlas normalization, shared by
# market cap, token summary and whale moves): tokenDetails fills `total_supply` already
# normalized, so the first two branches are the ones HyperCore takes
SUPPLY_EXPR = ("CASE WHEN total_supply_raw IS NOT NULL AND decimals IS NOT NULL AND decimals > 0 "
               "THEN total_supply_raw / power(10, decimals) "
               "WHEN total_supply IS NOT NULL AND total_supply < 1e15 THEN total_supply "
               "WHEN total_supply IS NOT NULL AND decimals IS NOT NULL AND decimals > 0 "
               "THEN total_supply / power(10, decimals) ELSE total_supply END")


def load_or_empty(ctx, name: str, alias: str, **kw) -> int:
    """ctx.load, or an EMPTY table with the DDL columns when the lake does not have the
    table yet (the 1m feed before its first lift): the engines then produce no rows
    instead of failing the whole group."""
    if alias in ctx._loaded:
        return ctx._loaded[alias]
    if not ctx.has_table(name):
        ctx.register(alias, table_from_rows([], ddl_columns(name)))
        return 0
    return ctx.load(name, alias=alias, **kw)


def ensure_inputs(ctx, *, ledger: bool = True, meta: bool = True, px1h: bool = False, px1d: bool = False,
                  ledger_all_days: int | None = None, px1m: bool = False, whitelist: bool = False) -> None:
    """Register the lake inputs an engine needs (idempotent). `ledger` is bounded to the
    wallets being recomputed; `ledger_all_<n>d` is every wallet's ledger for the last n
    days (the cross-wallet 24h tables); `px1m` the last PX1M_DAYS of 1-minute candles;
    `whitelist` the token gate."""
    if ledger:
        ctx.load("l1_wallet_ledger", date_col="block_date", alias="ledger")
    if meta:
        ctx.load("l1_token_metadata", wallet_col=None, alias="meta")
    if px1h:
        ctx.load("l3_token_price_usd_1h", date_col="bucket_date", wallet_col=None, alias="px1h")
    if px1d:
        ctx.load("l3_token_price_usd_1d", date_col="bucket_date", wallet_col=None, alias="px1d")
    if px1m:
        load_or_empty(ctx, "l3_token_price_usd_1m", "px1m", date_col="bucket_date", wallet_col=None,
                      since=ctx.today - dt.timedelta(days=PX1M_DAYS))
    if whitelist:
        load_or_empty(ctx, "token_whitelist", "whitelist", wallet_col=None)
    if ledger_all_days:
        ctx.load("l1_wallet_ledger", date_col="block_date", wallet_col=None,
                 since=ctx.today - dt.timedelta(days=ledger_all_days), alias=f"ledger_all_{ledger_all_days}d")


def eth_token(ctx) -> str | None:
    """The token_address of the ETH benchmark (UETH, decision 3A), None when the lake
    has never seen it."""
    rows = ctx.sql(f"SELECT token_address FROM meta WHERE upper(symbol) = '{ETH_SYMBOL}' LIMIT 1").to_pylist()
    return rows[0]["token_address"] if rows else None


def sample_stdev(values: list[float]) -> float | None:
    """Trino / DuckDB STDDEV (sample): None with fewer than 2 values."""
    vals = [v for v in values if v is not None]
    if len(vals) < 2:
        return None
    import statistics
    return statistics.stdev(vals)


def midrank_percentile(window: list[float], value: float) -> float:
    """(#strictly less + 0.5 * #equal) / n in [0, 1]: an all-tied window maps to 0.5."""
    less = sum(1 for x in window if x < value)
    equal = sum(1 for x in window if x == value)
    return (less + 0.5 * equal) / len(window)


def sort_key(row: dict) -> tuple:
    """The ledger order the atlas uses for running totals: block order, then event
    index, then direction ('in' < 'out'); tx_hash last so ties are deterministic."""
    return (row["block_timestamp"], row["event_index"] or 0, row["direction"] or "", row["tx_hash"] or "")


def group_rows(arrow: pa.Table, keys: tuple[str, ...]) -> dict[tuple, list[dict]]:
    """Rows of an Arrow table grouped by `keys` (python dicts; small per-wallet batches)."""
    out: dict[tuple, list[dict]] = {}
    for row in arrow.to_pylist():
        out.setdefault(tuple(row[k] for k in keys), []).append(row)
    return out


def as_date(value) -> dt.date | None:
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        return value.astimezone(dt.timezone.utc).date() if value.tzinfo else value.date()
    return value


def utc(ts) -> dt.datetime | None:
    if ts is None:
        return None
    return ts if ts.tzinfo else ts.replace(tzinfo=dt.timezone.utc)


def days_between(start: dt.date, end: dt.date) -> Iterable[dt.date]:
    d = start
    while d <= end:
        yield d
        d += dt.timedelta(days=1)


def asof(series: list[tuple], key, default=None):
    """series sorted ascending by its first element: the value of the last entry with
    first element <= key (the atlas BTreeMap::range(..=key).next_back())."""
    lo, hi = 0, len(series)
    while lo < hi:
        mid = (lo + hi) // 2
        if series[mid][0] <= key:
            lo = mid + 1
        else:
            hi = mid
    return series[lo - 1][1] if lo else default


def daily_prices(ctx, table: str = "px1d") -> dict[str, list[tuple[dt.date, float]]]:
    """token -> [(bucket_date, close_price_usd)] ascending from the 1d candles (the
    atlas 'daily price' is the vwap of the day's last 1m bucket == the close)."""
    arrow = ctx.sql(f"SELECT token_address, bucket_date, close_price_usd FROM {table} "
                    "WHERE close_price_usd IS NOT NULL ORDER BY token_address, bucket_date")
    out: dict[str, list[tuple[dt.date, float]]] = {}
    for tok, d, px in zip(*(arrow.column(c).to_pylist() for c in ("token_address", "bucket_date", "close_price_usd"))):
        if px is not None and math.isfinite(px):
            out.setdefault(tok, []).append((as_date(d), float(px)))
    return out


def hourly_prices(ctx, table: str = "px1h") -> dict[str, list[tuple[dt.datetime, float]]]:
    """token -> [(bucket_minute, close_price_usd)] ascending (ASOF price of a ledger row)."""
    arrow = ctx.sql(f"SELECT token_address, bucket_minute, close_price_usd FROM {table} "
                    "WHERE close_price_usd IS NOT NULL ORDER BY token_address, bucket_minute")
    out: dict[str, list[tuple[dt.datetime, float]]] = {}
    for tok, m, px in zip(*(arrow.column(c).to_pylist() for c in ("token_address", "bucket_minute", "close_price_usd"))):
        if px is not None and math.isfinite(px):
            out.setdefault(tok, []).append((utc(m), float(px)))
    return out


def stable_tokens(ctx) -> set[str]:
    arrow = ctx.sql("SELECT token_address FROM meta WHERE is_stablecoin")
    return set(arrow.column("token_address").to_pylist()) | {S.USDC_ADDRESS}


def table_from_rows(rows: list[dict], cols: list[tuple[str, str]]) -> pa.Table:
    """Arrow table with the DDL columns (typed), from python dicts; missing keys -> NULL."""
    from ..iceberg_load import _arrow_type
    data = {}
    for name, sql_type in cols:
        typ = _arrow_type(sql_type, True)
        vals = [r.get(name) for r in rows]
        if sql_type == "TIMESTAMP":
            vals = [utc(v) for v in vals]
        elif sql_type == "DATE":
            vals = [as_date(v) for v in vals]
        elif sql_type in ("INT", "BIGINT"):
            vals = [None if v is None else int(v) for v in vals]
        elif sql_type == "DOUBLE":
            vals = [None if v is None or (isinstance(v, float) and not math.isfinite(v)) else float(v) for v in vals]
        data[name] = pa.array(vals, type=typ)
    return pa.table(data)


def ddl_columns(name: str) -> list[tuple[str, str]]:
    from ..iceberg_load import parse_ddl
    return [(c[0], c[1]) for c in parse_ddl()[name]["columns"]]
