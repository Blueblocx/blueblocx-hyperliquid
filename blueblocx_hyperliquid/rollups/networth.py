"""l4_wallet_networth_1d_dense (atlas crate wallet-networth; formulas section 3).

Per (wallet, day): the dense token balances of the day valued at that day's ASOF
daily price (the 1d candle close; stables are 1.0 on the same grid). Tokens with
no price are counted, not summed. Spot/ledger networth only — the perp account
value lives in l4x_perp_position_snapshot and wallet_analytics, as on every
other chain.
"""
from __future__ import annotations

import pyarrow as pa

from .common import BALANCE_EPS, asof, daily_prices, ddl_columns, ensure_inputs, table_from_rows, utc


def _clip(x: float) -> float:
    return 0.0 if abs(x) < BALANCE_EPS else x


def compute(ctx) -> dict[str, pa.Table]:
    ensure_inputs(ctx, ledger=False, meta=False, px1d=True)
    if "balance_1d_new" not in ctx._loaded:
        from . import balances
        balances.compute(ctx)
    dense = ctx.sql("SELECT wallet_address, token_address, snapshot_date, opening_balance_amount, "
                    "closing_balance_amount, net_flow_amount, in_amount, out_amount, closing_block_timestamp "
                    "FROM balance_1d_new ORDER BY wallet_address, snapshot_date, token_address")
    prices = daily_prices(ctx)
    # the bucket_minute of a daily price = the day's 00:00Z candle open (what the 1d table stores)
    stamps = {tok: {d: None for d, _ in series} for tok, series in prices.items()}
    minute_rows = ctx.sql("SELECT token_address, bucket_date, bucket_minute FROM px1d")
    for tok, d, m in zip(*(minute_rows.column(c).to_pylist() for c in ("token_address", "bucket_date", "bucket_minute"))):
        if tok in stamps:
            stamps[tok][d] = utc(m)

    out: list[dict] = []
    cur_key = None
    acc: dict | None = None

    def flush():
        if acc is not None:
            out.append(acc)

    cols = ("wallet_address", "token_address", "snapshot_date", "opening_balance_amount", "closing_balance_amount",
            "net_flow_amount", "in_amount", "out_amount", "closing_block_timestamp")
    for w, tok, day, opening, closing, net, inn, outt, cts in zip(*(dense.column(c).to_pylist() for c in cols)):
        key = (w, day)
        if key != cur_key:
            flush()
            cur_key = key
            acc = {"wallet_address": w, "snapshot_date": day, "opening_networth_usd": 0.0, "closing_networth_usd": 0.0,
                   "net_flow_usd": 0.0, "in_usd": 0.0, "out_usd": 0.0, "priced_token_count": 0,
                   "missing_price_token_count": 0, "latest_price_bucket_minute": None, "closing_block_timestamp": None}
        cts = utc(cts)
        if cts is not None and (acc["closing_block_timestamp"] is None or cts > acc["closing_block_timestamp"]):
            acc["closing_block_timestamp"] = cts
        series = prices.get(tok)
        px_date = None
        px = None
        if series:
            # ASOF: latest daily price with bucket_date <= snapshot_date
            lo, hi = 0, len(series)
            while lo < hi:
                mid = (lo + hi) // 2
                if series[mid][0] <= day:
                    lo = mid + 1
                else:
                    hi = mid
            if lo:
                px_date, px = series[lo - 1]
        if px is None:
            acc["missing_price_token_count"] += 1
            continue
        acc["priced_token_count"] += 1
        acc["opening_networth_usd"] += _clip(float(opening or 0.0)) * px
        acc["closing_networth_usd"] += _clip(float(closing or 0.0)) * px
        acc["net_flow_usd"] += _clip(float(net or 0.0)) * px
        acc["in_usd"] += _clip(float(inn or 0.0)) * px
        acc["out_usd"] += _clip(float(outt or 0.0)) * px
        stamp = stamps.get(tok, {}).get(px_date)
        if stamp is not None and (acc["latest_price_bucket_minute"] is None or stamp > acc["latest_price_bucket_minute"]):
            acc["latest_price_bucket_minute"] = stamp
    flush()
    table = table_from_rows(out, ddl_columns("l4_wallet_networth_1d_dense"))
    ctx.register("networth_1d_new", table)
    return {"l4_wallet_networth_1d_dense": table}


__all__ = ["compute", "asof"]
