"""exchange_flow (atlas vw_exchange_flow) for HyperCore: the USDC the TRACKED wallets
bridged into HyperCore (ledger event_type 'deposit', incoming) and out of it
('withdraw', outgoing) per closed UTC day of the last 31; net = incoming - outgoing.
The EVM view measures the same thing on the WETH/stable pools of the chain.
"""
from __future__ import annotations

import pyarrow as pa

from .common import ddl_columns, ensure_inputs, table_from_rows

FLOW_DAYS = 31


def compute(ctx) -> dict[str, pa.Table]:
    ensure_inputs(ctx, ledger=False, meta=False, ledger_all_days=FLOW_DAYS)
    today = ctx.today.isoformat()
    arrow = ctx.sql(f"""
        SELECT block_date AS bucket_date,
               sum(CASE WHEN event_type = 'deposit'  THEN abs(coalesce(amount, 0)) ELSE 0 END) AS incoming_volume_usd,
               sum(CASE WHEN event_type = 'withdraw' THEN abs(coalesce(amount, 0)) ELSE 0 END) AS outgoing_volume_usd
        FROM ledger_all_{FLOW_DAYS}d
        WHERE event_type IN ('deposit', 'withdraw') AND amount_raw IS NOT NULL
          AND block_date <= DATE '{today}' - 1 AND block_date >= DATE '{today}' - {FLOW_DAYS}
        GROUP BY 1 ORDER BY 1""")
    rows = arrow.to_pylist()
    for r in rows:
        r["net_flow_usd"] = float(r["incoming_volume_usd"] or 0) - float(r["outgoing_volume_usd"] or 0)
    return {"exchange_flow": table_from_rows(rows, ddl_columns("exchange_flow"))}
