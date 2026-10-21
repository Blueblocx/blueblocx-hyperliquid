"""wallet_token_stats and wallet_trade_summary (atlas views vw_wallet_token_stats /
vw_wallet_trade_summary; formulas section 5).

Both are derived from the FIFO engine's outputs on the context (state, per-pair
realized total, the priced statement) plus token metadata and the latest 1h
close per token (no staleness cap, like the view).
"""
from __future__ import annotations

import pyarrow as pa

from .common import ddl_columns, ensure_inputs, table_from_rows


def compute(ctx) -> dict[str, pa.Table]:
    ensure_inputs(ctx, ledger=False, meta=True, px1h=True)
    if "fifo_state_new" not in ctx._loaded:
        from . import fifo
        fifo.compute(ctx)
    stats = ctx.sql("""
        WITH latest AS (
            SELECT token_address, close_price_usd AS price_usd
            FROM (SELECT token_address, close_price_usd,
                         row_number() OVER (PARTITION BY token_address ORDER BY bucket_minute DESC) rn
                  FROM px1h WHERE close_price_usd IS NOT NULL)
            WHERE rn = 1
        ),
        stable AS (SELECT token_address FROM meta WHERE is_stablecoin)
        SELECT f.wallet_address, f.token_address, m.symbol AS token_symbol, m.name AS token_name,
               f.open_qty AS position_qty, f.open_cost_usd AS position_cost_usd,
               f.open_cost_usd / nullif(f.open_qty, 0.0) AS avg_cost_usd,
               coalesce(lp.price_usd, CASE WHEN s.token_address IS NOT NULL THEN 1.0 END) AS price_usd,
               f.open_qty * coalesce(lp.price_usd, CASE WHEN s.token_address IS NOT NULL THEN 1.0 END) AS position_value_usd,
               coalesce(r.realized_pnl_usd, 0.0) AS realized_pnl_usd,
               CASE WHEN f.open_qty > 0 AND coalesce(lp.price_usd, CASE WHEN s.token_address IS NOT NULL THEN 1.0 END) IS NOT NULL
                    THEN f.open_qty * coalesce(lp.price_usd, CASE WHEN s.token_address IS NOT NULL THEN 1.0 END) - f.open_cost_usd
                    ELSE 0.0 END AS unrealized_pnl_usd,
               f.last_block_timestamp, CAST(f.last_block_timestamp AS DATE) AS last_block_date,
               f.last_tx_hash, f.last_event_index
        FROM fifo_state_new f
        JOIN meta m ON m.token_address = f.token_address
        LEFT JOIN latest lp ON lp.token_address = f.token_address
        LEFT JOIN stable s ON s.token_address = f.token_address
        LEFT JOIN fifo_pair_realized r ON r.wallet_address = f.wallet_address AND r.token_address = f.token_address
    """)
    rows = stats.to_pylist()
    for r in rows:
        r["total_pnl_usd"] = (r["realized_pnl_usd"] or 0.0) + (r["unrealized_pnl_usd"] or 0.0)
        r["total_return_pct"] = (r["total_pnl_usd"] / r["position_cost_usd"]) if r["position_cost_usd"] else None
    token_stats = table_from_rows(rows, ddl_columns("wallet_token_stats"))

    summary = ctx.sql("""
        WITH agg AS (
            SELECT wallet_address, token_address,
                   sum(CASE WHEN direction = 'in'  THEN amount ELSE 0 END)                  AS token_buy_qty,
                   sum(CASE WHEN direction = 'in'  THEN amount * price_usd END)             AS token_buy_value_usd,
                   sum(CASE WHEN direction = 'out' THEN amount ELSE 0 END)                  AS token_sell_qty,
                   sum(CASE WHEN direction = 'out' THEN amount * price_usd END)             AS token_sell_value_usd,
                   max(block_date)                                                          AS last_activity_date
            FROM priced_statement GROUP BY 1, 2)
        SELECT a.wallet_address, a.token_address, m.symbol AS token_symbol,
               a.token_buy_qty, a.token_buy_value_usd,
               a.token_buy_value_usd / nullif(a.token_buy_qty, 0)   AS avg_buy_price_usd,
               a.token_sell_qty, a.token_sell_value_usd,
               a.token_sell_value_usd / nullif(a.token_sell_qty, 0) AS avg_sell_price_usd,
               coalesce(r.realized_pnl_usd, 0.0)                     AS net_profit_usd,
               coalesce(r.realized_pnl_usd, 0.0) / nullif(a.token_buy_value_usd, 0) AS net_gain_pct,
               a.last_activity_date
        FROM agg a
        JOIN meta m ON m.token_address = a.token_address
        LEFT JOIN fifo_pair_realized r ON r.wallet_address = a.wallet_address AND r.token_address = a.token_address
    """)
    trade_summary = table_from_rows(summary.to_pylist(), ddl_columns("wallet_trade_summary"))
    return {"wallet_token_stats": token_stats, "wallet_trade_summary": trade_summary}
