"""dex_pair_volume_1d (atlas l3_dex_pair_volume_1d) for HyperCore: one spot market =
one pair TOKEN/USDC = one pool. The day's USD volume and trade count come from the
market's 1d candle (market-wide); the direction split the EVM job derives from the
swap legs is not observable from candles, so the per-direction amounts, USD volumes
and counts are 0 and everything sits in volume_usd / swap_count_total (decision 7A).
The last PAIR_DAYS closed days are (re)written per run.
"""
from __future__ import annotations

import datetime as dt

import pyarrow as pa

from .. import schema as S
from .common import ddl_columns, ensure_inputs, table_from_rows

PAIR_DAYS = 7


def compute(ctx) -> dict[str, pa.Table]:
    ensure_inputs(ctx, ledger=False, meta=True, px1d=True)
    today = ctx.today.isoformat()
    arrow = ctx.sql(f"""
        SELECT p.bucket_date, p.token_address || '/' || '{S.USDC_ADDRESS}' AS pair_address,
               coalesce(m.symbol, p.token_address) || '/USDC' AS pair_symbol,
               p.token_address AS token0_address, '{S.USDC_ADDRESS}' AS token1_address,
               coalesce(m.symbol, p.token_address) AS token0_symbol, 'USDC' AS token1_symbol,
               0.0 AS amount_token0_to_token1, 0.0 AS amount_token1_to_token0,
               0.0 AS volume_usd_token0_to_token1, 0.0 AS volume_usd_token1_to_token0,
               coalesce(p.volume_usd, 0.0) AS volume_usd,
               0 AS swap_count_token0_to_token1, 0 AS swap_count_token1_to_token0,
               coalesce(p.num_trades, 0) AS swap_count_total, 1 AS pool_count
        FROM px1d p JOIN meta m ON m.token_address = p.token_address
        WHERE NOT coalesce(m.is_stablecoin, false)
          AND p.bucket_date >= DATE '{today}' - {PAIR_DAYS} AND p.bucket_date < DATE '{today}'
        ORDER BY 1, 2""")
    rows = arrow.to_pylist()
    for r in rows:
        r["created_at"] = ctx.now
    return {"dex_pair_volume_1d": table_from_rows(rows, ddl_columns("dex_pair_volume_1d"))}
