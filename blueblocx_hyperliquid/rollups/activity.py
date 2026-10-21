"""wallet_whale_moves_24h and token_netflow_24h (atlas views; formulas sections 6, 7).

Cross-wallet, rolling 24h, recomputed whole every run from every tracked wallet's
ledger of the last 2 days. HyperCore has no market_cap_1d yet (phase B): the
supply / market-cap columns of whale moves are NULL until then.
"""
from __future__ import annotations

import pyarrow as pa

from .common import ddl_columns, ensure_inputs, table_from_rows


def compute(ctx) -> dict[str, pa.Table]:
    ensure_inputs(ctx, ledger=False, meta=True, px1h=True, px1d=True, ledger_all_days=3)
    now_iso = ctx.now_iso
    whale = ctx.sql(f"""
        WITH recent AS (
            SELECT l.wallet_address, l.token_address, l.direction, abs(l.amount) AS amount, l.block_timestamp
            FROM ledger_all_3d l JOIN meta m ON m.token_address = l.token_address
            WHERE l.block_timestamp >= TIMESTAMPTZ '{now_iso}' - INTERVAL 24 HOUR
              AND l.block_timestamp <  TIMESTAMPTZ '{now_iso}'),
        wallet_moves AS (
            SELECT wallet_address, token_address, direction, sum(amount) AS vol_24h, max(block_timestamp) AS max_block_timestamp
            FROM recent GROUP BY 1, 2, 3),
        token_totals AS (SELECT token_address, sum(amount) AS total_volume_token FROM recent GROUP BY 1),
        latest_price AS (
            SELECT token_address, close_price_usd FROM (
                SELECT token_address, close_price_usd, row_number() OVER (PARTITION BY token_address ORDER BY bucket_date DESC) rn
                FROM px1d WHERE bucket_date >= CAST(TIMESTAMPTZ '{now_iso}' AS DATE) - 7
                               AND bucket_date <  CAST(TIMESTAMPTZ '{now_iso}' AS DATE))
            WHERE rn = 1),
        supply AS (
            SELECT token_address,
                   CASE WHEN total_supply_raw IS NOT NULL AND decimals IS NOT NULL AND decimals > 0
                             THEN total_supply_raw / power(10, decimals)
                        WHEN total_supply IS NOT NULL AND total_supply < 1e15 THEN total_supply
                        WHEN total_supply IS NOT NULL AND decimals IS NOT NULL AND decimals > 0
                             THEN total_supply / power(10, decimals)
                        ELSE total_supply END AS total_supply
            FROM meta)
        SELECT wm.wallet_address, wm.token_address, wm.direction, wm.vol_24h,
               wm.vol_24h * coalesce(lp.close_price_usd, 0) AS wallet_volume_usd_24h,
               wm.max_block_timestamp,
               CASE WHEN s.total_supply BETWEEN 1 AND 1e15 THEN s.total_supply END AS total_supply,
               CASE WHEN s.total_supply BETWEEN 1 AND 1e15 AND lp.close_price_usd IS NOT NULL
                    THEN s.total_supply * lp.close_price_usd END AS total_market_cap,
               tt.total_volume_token,
               CASE WHEN s.total_supply BETWEEN 1 AND 1e15 THEN s.total_supply * 0.01 END AS brenchmark_supply,
               tt.total_volume_token * 0.01 AS brenchmark_volume_token,
               TIMESTAMPTZ '{now_iso}' AS create_at
        FROM wallet_moves wm
        JOIN token_totals tt ON tt.token_address = wm.token_address
        LEFT JOIN latest_price lp ON lp.token_address = wm.token_address
        LEFT JOIN supply s ON s.token_address = wm.token_address
        WHERE wm.vol_24h >= tt.total_volume_token * 0.01
    """)
    netflow = ctx.sql(f"""
        WITH b AS (SELECT date_trunc('hour', TIMESTAMPTZ '{now_iso}') AS hour_now),
        ledger_24h AS (
            SELECT l.token_address, l.amount
            FROM ledger_all_3d l, b
            WHERE l.direction = 'in' AND l.amount_raw IS NOT NULL
              AND l.block_timestamp >= b.hour_now - INTERVAL 24 HOUR AND l.block_timestamp < b.hour_now),
        latest_price AS (
            SELECT token_address, vwap_price_usd FROM (
                SELECT p.token_address, p.vwap_price_usd,
                       row_number() OVER (PARTITION BY p.token_address ORDER BY p.bucket_minute DESC) rn
                FROM px1h p, b
                WHERE p.bucket_date >= CAST(b.hour_now AS DATE) - 3 AND p.bucket_minute < b.hour_now
                  AND p.vwap_price_usd IS NOT NULL)
            WHERE rn = 1)
        SELECT l.token_address,
               sum(l.amount * lp.vwap_price_usd) AS inflow_usd_24h,
               sum(l.amount * lp.vwap_price_usd) AS outflow_usd_24h,
               0.0                                AS netflow_usd_24h,
               count(*)                           AS tx_in_count_24h,
               count(*)                           AS tx_out_count_24h,
               any_value(lp.vwap_price_usd)       AS latest_vwap_price_usd
        FROM ledger_24h l
        JOIN meta m ON m.token_address = l.token_address
        JOIN latest_price lp ON lp.token_address = l.token_address
        GROUP BY 1
    """)
    return {"wallet_whale_moves_24h": table_from_rows(whale.to_pylist(), ddl_columns("wallet_whale_moves_24h")),
            "token_netflow_24h": table_from_rows(netflow.to_pylist(), ddl_columns("token_netflow_24h"))}
