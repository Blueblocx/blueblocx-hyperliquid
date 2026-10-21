-- ============================================================================
-- Proposed additions to ICEBERG_SCHEMA.sql for Hyperliquid.
--
-- Namespaces mirror the ethereum/arbitrum convention: one per HL chain.
-- HyperCore skips raw_logs/l0/l2 (no EVM logs, no DEX pools) and writes
-- l3_token_price_usd_1m directly from native candles. HyperEVM reuses the full
-- EVM path unchanged. Both feed all_chain.wallet_analytics.
-- ============================================================================

CREATE NAMESPACE IF NOT EXISTS nessie.hypercore;
CREATE NAMESPACE IF NOT EXISTS nessie.hyperevm;

-- HyperCore populates the SAME DDL as ethereum for these tables (the concrete
-- nessie.hypercore.* CREATE TABLE statements are below). No column changes needed:
--   l1_wallet_ledger          (standard is ALWAYS "ERC20" so consumer gates pass;
--                              the HL coin symbol lives in l1_token_metadata.symbol)
--   l1_token_metadata         (symbol/decimals/is_stablecoin for spot + USDC)
--   l3_token_price_usd_1m     (native 1m candles, keyed by the ledger token_address;
--                              development-only output in production)
--   l3_token_price_usd_1h/_1d (native 1h / 1d candles for the ETL's token_price_usd_1h/1d - v1.7.0)
--   l4_wallet_metadata        (activity summary; first/latest by block_timestamp;
--                              first_tx/latest_tx are those INSTANTS, not hashes - v1.5.0)
--   token_whitelist           (consumer gate — seed is_active=true for HL tokens)
--   wallet_watchlist          (consumer gate — seed is_active=true for HL wallets)
-- L4 balance/FIFO/networth then run unchanged on the SPOT legs. Perp realized PnL
-- is added by a separate rollup (see note below).

-- ----------------------------------------------------------------------------
-- EXTENSION: perp open positions (unrealized PnL / margin / mark-to-market).
-- The spot token-balance model cannot represent a perp position (signed size +
-- entry + margin, valued at markPx). This snapshot feeds mark-to-market networth
-- and wallet_analytics.unrealized_pnl as an additive term on top of the USDC-
-- cashflow ledger balance.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS nessie.hypercore.l4x_perp_position_snapshot (
    as_of_ts                TIMESTAMP,
    as_of_date              DATE,
    wallet_address          STRING,
    wallet_group            INT,
    coin                    STRING,
    dex                     STRING,   -- main | HIP-3 dex (xyz, flx, ...); margin account per dex
    szi                     DOUBLE,   -- signed size: + long / - short
    entry_px                DOUBLE,   -- AVERAGE entry (not FIFO lots)
    position_value          DOUBLE,
    mark_px                 DOUBLE,
    unrealized_pnl_usd      DOUBLE,
    return_on_equity        DOUBLE,
    liquidation_px          DOUBLE,
    margin_used_usd         DOUBLE,
    leverage_type           STRING,   -- cross | isolated
    leverage_value          DOUBLE,
    cum_funding_all_time    DOUBLE,
    cum_funding_since_open  DOUBLE,   -- resets to 0 when the position closes
    account_value_usd       DOUBLE,   -- marginSummary.accountValue of THIS dex's margin account
    withdrawable_usd        DOUBLE,
    created_at              TIMESTAMP
) USING iceberg
PARTITIONED BY (as_of_date)          -- one snapshot per day builds the MTM history
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

ALTER TABLE nessie.hypercore.l4x_perp_position_snapshot
    WRITE ORDERED BY wallet_address ASC NULLS LAST, coin ASC NULLS LAST;

-- ----------------------------------------------------------------------------
-- CONCRETE DDL: nessie.hypercore.* copies of the ethereum tables HyperCore writes.
-- Generated from ICEBERG_SCHEMA.sql (same columns/partitioning; only namespace
-- differs). All l1_wallet_ledger columns are nullable in Iceberg — HyperCore
-- leaves block_number / tx_index NULL (no EVM block). Pre-create these with this
-- DDL rather than letting a writer auto-create (auto-create yields required
-- block_number/tx_index and an unpartitioned table).
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS nessie.hypercore.l1_wallet_ledger (
    block_number         BIGINT,
    block_timestamp      TIMESTAMP,
    block_date           DATE,
    tx_hash              STRING,
    tx_index             INT,
    event_index          INT,
    event_type           STRING,
    wallet_address       STRING,
    wallet_group         INT,
    counterparty_address STRING,
    token_address        STRING,
    token_group          INT,
    direction            STRING,
    amount_raw           DOUBLE,
    decimals             INT,
    standard             STRING,
    amount               DOUBLE,
    amount_signed        DOUBLE,
    created_at           TIMESTAMP,
    updated_at           TIMESTAMP
) USING iceberg
PARTITIONED BY (block_date, event_type)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

ALTER TABLE nessie.hypercore.l1_wallet_ledger WRITE ORDERED BY wallet_group ASC NULLS LAST, wallet_address ASC NULLS LAST, token_address ASC NULLS LAST, block_timestamp ASC NULLS LAST;

CREATE TABLE IF NOT EXISTS nessie.hypercore.l1_token_metadata (
    token_address       STRING,
    token_group         INT,
    standard            STRING,
    symbol              STRING,
    name                STRING,
    decimals            INT,
    first_seen          TIMESTAMP,
    source              STRING,
    logo_url            STRING,
    total_supply_raw    DOUBLE,
    total_supply        DOUBLE,
    supply_as_of        TIMESTAMP,
    supply_source       STRING,
    supply_confidence   STRING,
    category            STRING,
    is_stablecoin       BOOLEAN,
    is_native_wrapper   BOOLEAN,
    coingecko_id        STRING,
    token_created_at    TIMESTAMP,
    is_watchlist        BOOLEAN,
    created_at          TIMESTAMP,
    updated_at          TIMESTAMP
) USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

ALTER TABLE nessie.hypercore.l1_token_metadata WRITE ORDERED BY token_group ASC NULLS LAST, token_address ASC NULLS LAST;

CREATE TABLE IF NOT EXISTS nessie.hypercore.l3_token_price_usd_1m (
    token_address       STRING,
    token_group         INT,
    bucket_minute       TIMESTAMP,
    bucket_date         DATE,
    open_price_usd      DOUBLE,
    high_price_usd      DOUBLE,
    low_price_usd       DOUBLE,
    close_price_usd     DOUBLE,
    vwap_price_usd      DOUBLE,
    volume_token        DOUBLE,
    volume_usd          DOUBLE,
    num_trades          INT,
    created_at          TIMESTAMP,
    updated_at          TIMESTAMP
) USING iceberg
PARTITIONED BY (bucket_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

ALTER TABLE nessie.hypercore.l3_token_price_usd_1m WRITE ORDERED BY token_address ASC NULLS LAST, bucket_minute ASC NULLS LAST;

-- ----------------------------------------------------------------------------
-- Native 1h / 1d candles for the ETL's token_price_usd_1h / token_price_usd_1d
-- (v1.7.0): the SAME shape as the 1m table (bucket_minute = the candle open,
-- bucket_date = its UTC day), one row per CLOSED candle, stables pinned at 1.0 on
-- the same grid, no perp reference rows. Natural keys: (token_address,
-- bucket_minute) for 1h, (token_address, bucket_date) for 1d. Written FIRST in
-- every hourly run under its own time budget (blueblocx_hyperliquid.price_step).
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS nessie.hypercore.l3_token_price_usd_1h (
    token_address       STRING,
    token_group         INT,
    bucket_minute       TIMESTAMP, -- candle open (hour)
    bucket_date         DATE,
    open_price_usd      DOUBLE,
    high_price_usd      DOUBLE,
    low_price_usd       DOUBLE,
    close_price_usd     DOUBLE,
    vwap_price_usd      DOUBLE,
    volume_token        DOUBLE,
    volume_usd          DOUBLE,
    num_trades          INT,
    created_at          TIMESTAMP,
    updated_at          TIMESTAMP
) USING iceberg
PARTITIONED BY (bucket_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

ALTER TABLE nessie.hypercore.l3_token_price_usd_1h WRITE ORDERED BY token_address ASC NULLS LAST, bucket_minute ASC NULLS LAST;

CREATE TABLE IF NOT EXISTS nessie.hypercore.l3_token_price_usd_1d (
    token_address       STRING,
    token_group         INT,
    bucket_minute       TIMESTAMP, -- candle open (00:00 UTC of bucket_date)
    bucket_date         DATE,
    open_price_usd      DOUBLE,
    high_price_usd      DOUBLE,
    low_price_usd       DOUBLE,
    close_price_usd     DOUBLE,
    vwap_price_usd      DOUBLE,
    volume_token        DOUBLE,
    volume_usd          DOUBLE,
    num_trades          INT,
    created_at          TIMESTAMP,
    updated_at          TIMESTAMP
) USING iceberg
PARTITIONED BY (bucket_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

ALTER TABLE nessie.hypercore.l3_token_price_usd_1d WRITE ORDERED BY token_address ASC NULLS LAST, bucket_date ASC NULLS LAST;

CREATE TABLE IF NOT EXISTS nessie.hypercore.l4_wallet_metadata (
    wallet_address          STRING,
    first_block_timestamp   TIMESTAMP,
    latest_block_timestamp  TIMESTAMP,
    count_tx                INT,
    first_tx                TIMESTAMP, -- instant of the wallet's first ledger event (= first_block_timestamp); TIMESTAMPTZ since v1.5.0, was the tx hash (STRING)
    latest_tx               TIMESTAMP, -- instant of the wallet's latest ledger event (= latest_block_timestamp); TIMESTAMPTZ since v1.5.0, was the tx hash (STRING)
    active_days             INT,
    last_active             INT,
    active_on_avg           DOUBLE,
    updated_at              TIMESTAMP
) USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

ALTER TABLE nessie.hypercore.l4_wallet_metadata WRITE ORDERED BY wallet_address ASC NULLS LAST;

CREATE TABLE IF NOT EXISTS nessie.hypercore.token_whitelist (
    token_address       STRING,
    token_group         INT,
    is_active           BOOLEAN,
    whitelist_reason    STRING,
    created_at          TIMESTAMP,
    updated_at          TIMESTAMP
) USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

ALTER TABLE nessie.hypercore.token_whitelist WRITE ORDERED BY token_group ASC NULLS LAST, token_address ASC NULLS LAST;

CREATE TABLE IF NOT EXISTS nessie.hypercore.wallet_watchlist (
    wallet_address      STRING,
    wallet_group        INT,
    is_active           BOOLEAN,
    whitelist_reason    STRING,
    created_at          TIMESTAMP,
    updated_at          TIMESTAMP
) USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

ALTER TABLE nessie.hypercore.wallet_watchlist WRITE ORDERED BY wallet_group ASC NULLS LAST, wallet_address ASC NULLS LAST;

-- ============================================================================
-- REALIZED PnL ROLLUP NOTE (no new table — extends l4_wallet_pnl_realized_1d).
--
-- The stock FIFO engine computes SPOT realized PnL only. Perp realized PnL lives
-- in l1_wallet_ledger as USDC rows with event_type in
--   (perp_realized_pnl, funding, fee)   [+ staking_reward for income]
-- which FIFO nets to ~$0 (USDC pinned $1). The HyperCore realized-PnL rollup is:
--
--   realized_pnl_Nd(hypercore) =
--       FIFO_spot_realized_pnl_Nd
--     + Σ (amount_signed * price_usd(token_address)) WHERE event_type IN
--         (perp_realized_pnl, funding, fee, staking_reward, interest)
--         AND block_timestamp >= now - N days
--
-- NOT a raw SUM(amount_signed): spot-buy fee legs are booked in the BASE token
-- and must be valued (join l3_token_price_usd_1m; stables = 1). Rows without a
-- price must be counted/surfaced, not summed. This matches Hyperliquid's own net
-- figure (closedPnl + funding − fees), which the portfolio endpoint (perp*
-- periods) confirms as ground truth.
--
-- NET_FLOW NOTE: net_flow must sum ONLY event_type IN (deposit, withdraw,
-- transfer) — NEVER perp_realized_pnl/fee/funding (those are PnL, not capital
-- flow). Internal perp<->spot moves (accountClassTransfer) are emitted as two
-- opposite legs so they net to zero.
-- ============================================================================

-- ============================================================================
-- v1.9.0 ROLLUPS (phase A): the wallet-level L4 layer of the ETL contract, built
-- from l1_wallet_ledger + l3_token_price_usd_1d/_1h + l4x_perp_position_snapshot
-- by blueblocx_hyperliquid.rollups after every lift (docs/HYPERCORE_FULL_CONTRACT_PLAN.md).
-- Column names and types follow the atlas definitions the ETL already reads for
-- ethereum (docs/ATLAS_WALLET_TABLE_FORMULAS.md), so the ETL spec is a pass-through.
-- Every row carries updated_at = the WRITE stamp of the rollup run that (re)wrote it
-- (the ETL's Tier-A cursor); tables recomputed per wallet are partitioned by
-- wallet_address so a wallet's rows are replaced in one partition.
-- SCOPE: wallet-derived aggregates cover the TRACKED wallet set only (section 2 of
-- the plan); candle-derived numbers are market-wide.
-- ============================================================================

CREATE TABLE IF NOT EXISTS nessie.hypercore.l1_token_categories (
    token_address       STRING,
    token_group         INT,
    category            STRING,
    source              STRING,
    created_at          TIMESTAMP,
    updated_at          TIMESTAMP
) USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

CREATE TABLE IF NOT EXISTS nessie.hypercore.l4_wallet_balance_per_tx (
    wallet_address      STRING,
    token_address       STRING,
    block_timestamp     TIMESTAMP,
    block_date          DATE,
    tx_hash             STRING,
    event_index         INT,
    direction           STRING,
    amount_raw          DOUBLE,
    amount              DOUBLE,
    amount_signed       DOUBLE,
    balance_amount      DOUBLE,
    created_at          TIMESTAMP,
    updated_at          TIMESTAMP
) USING iceberg
PARTITIONED BY (wallet_address)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

CREATE TABLE IF NOT EXISTS nessie.hypercore.l4_wallet_balance_1d_dense (
    wallet_address          STRING,
    token_address           STRING,
    snapshot_date           DATE,
    opening_balance_amount  DOUBLE,
    closing_balance_amount  DOUBLE,
    net_flow_amount         DOUBLE,
    in_amount               DOUBLE,
    out_amount              DOUBLE,
    closing_block_timestamp TIMESTAMP,
    created_at              TIMESTAMP,
    updated_at              TIMESTAMP
) USING iceberg
PARTITIONED BY (wallet_address)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

CREATE TABLE IF NOT EXISTS nessie.hypercore.l4_wallet_networth_1d_dense (
    wallet_address              STRING,
    snapshot_date               DATE,
    opening_networth_usd        DOUBLE,
    closing_networth_usd        DOUBLE,
    net_flow_usd                DOUBLE,
    in_usd                      DOUBLE,
    out_usd                     DOUBLE,
    priced_token_count          INT,
    missing_price_token_count   INT,
    latest_price_bucket_minute  TIMESTAMP,
    closing_block_timestamp     TIMESTAMP,
    created_at                  TIMESTAMP,
    updated_at                  TIMESTAMP
) USING iceberg
PARTITIONED BY (wallet_address)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

CREATE TABLE IF NOT EXISTS nessie.hypercore.l4_wallet_pnl_state_fifo (
    wallet_address          STRING,
    token_address           STRING,
    open_qty                DOUBLE,
    open_cost_usd           DOUBLE,
    last_block_timestamp    TIMESTAMP,
    last_tx_hash            STRING,
    last_event_index        INT,
    updated_at              TIMESTAMP
) USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

CREATE TABLE IF NOT EXISTS nessie.hypercore.l4_wallet_pnl_realized_1d (
    wallet_address      STRING,
    snapshot_date       DATE,
    realized_pnl_usd    DOUBLE,
    proceeds_usd        DOUBLE,
    cost_usd            DOUBLE,
    num_sells           INT,
    updated_at          TIMESTAMP
) USING iceberg
PARTITIONED BY (wallet_address)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

CREATE TABLE IF NOT EXISTS nessie.hypercore.wallet_token_stats (
    wallet_address          STRING,
    token_address           STRING,
    token_symbol            STRING,
    token_name              STRING,
    position_qty            DOUBLE,
    position_cost_usd       DOUBLE,
    avg_cost_usd            DOUBLE,
    price_usd               DOUBLE,
    position_value_usd      DOUBLE,
    realized_pnl_usd        DOUBLE,
    unrealized_pnl_usd      DOUBLE,
    total_pnl_usd           DOUBLE,
    total_return_pct        DOUBLE,
    last_block_timestamp    TIMESTAMP,
    last_block_date         DATE,
    last_tx_hash            STRING,
    last_event_index        INT,
    updated_at              TIMESTAMP
) USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

CREATE TABLE IF NOT EXISTS nessie.hypercore.wallet_trade_summary (
    wallet_address          STRING,
    token_address           STRING,
    token_symbol            STRING,
    token_buy_qty           DOUBLE,
    token_buy_value_usd     DOUBLE,
    avg_buy_price_usd       DOUBLE,
    token_sell_qty          DOUBLE,
    token_sell_value_usd    DOUBLE,
    avg_sell_price_usd      DOUBLE,
    net_profit_usd          DOUBLE,
    net_gain_pct            DOUBLE,
    last_activity_date      DATE,
    updated_at              TIMESTAMP
) USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

CREATE TABLE IF NOT EXISTS nessie.hypercore.wallet_whale_moves_24h (
    wallet_address              STRING,
    token_address               STRING,
    direction                   STRING,
    vol_24h                     DOUBLE,
    wallet_volume_usd_24h       DOUBLE,
    max_block_timestamp         TIMESTAMP,
    total_supply                DOUBLE,
    total_market_cap            DOUBLE,
    total_volume_token          DOUBLE,
    brenchmark_supply           DOUBLE,
    brenchmark_volume_token     DOUBLE,
    create_at                   TIMESTAMP,
    updated_at                  TIMESTAMP
) USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

CREATE TABLE IF NOT EXISTS nessie.hypercore.token_netflow_24h (
    token_address           STRING,
    inflow_usd_24h          DOUBLE,
    outflow_usd_24h         DOUBLE,
    netflow_usd_24h         DOUBLE,
    tx_in_count_24h         BIGINT,
    tx_out_count_24h        BIGINT,
    latest_vwap_price_usd   DOUBLE,
    updated_at              TIMESTAMP
) USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

CREATE TABLE IF NOT EXISTS nessie.hypercore.wallet_overview_pnl_percentile_1d (
    wallet_address      STRING,
    computed_date       DATE,
    pnl_1d_usd          DOUBLE,
    pnl_7d_usd          DOUBLE,
    pnl_30d_usd         DOUBLE,
    pnl_percentile_1d   DOUBLE,
    pnl_percentile_7d   DOUBLE,
    pnl_percentile_30d  DOUBLE,
    updated_at          TIMESTAMP
) USING iceberg
PARTITIONED BY (computed_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

CREATE TABLE IF NOT EXISTS nessie.hypercore.l4_token_wallet_sharpe_1d (
    token_address           STRING,
    wallet_address          STRING,
    snapshot_date           DATE,
    balance_amount          DOUBLE,
    ownership_pct           DOUBLE,
    position_value_usd      DOUBLE,
    value_change_pct_7d     DOUBLE,
    value_change_pct_30d    DOUBLE,
    first_in_timestamp      TIMESTAMP,
    sharpe_ratio            DOUBLE,
    updated_at              TIMESTAMP
) USING iceberg
PARTITIONED BY (snapshot_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

CREATE TABLE IF NOT EXISTS nessie.hypercore.wallet_performance_30d (
    wallet_address                  STRING,
    as_of_date                      DATE,
    start_date                      DATE,
    end_date                        DATE,
    nw_start                        DOUBLE,
    nw_end                          DOUBLE,
    wallet_return_30d               DOUBLE,
    avg_annualized_return_30d       DOUBLE,
    volatility_daily_30d            DOUBLE,
    volatility_annualized_30d       DOUBLE,
    sharpe_ratio_annualized_30d     DOUBLE,
    correlation_with_eth_30d        DOUBLE,
    eth_return_30d                  DOUBLE,
    outperform_market_30d           BOOLEAN,
    alpha_vs_eth_30d                DOUBLE,
    num_return_points_used          INT,
    updated_at                      TIMESTAMP
) USING iceberg
PARTITIONED BY (as_of_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

-- v1.9.0: the exchange's own spot balances per wallet (spotClearinghouseState), the
-- anchor the rollups pin the ledger running balances to (blueblocx_hyperliquid.schema
-- L4X_SPOT_BALANCE_SNAPSHOT_COLUMNS). Replaced per (as_of_date, wallet) every run.
CREATE TABLE IF NOT EXISTS nessie.hypercore.l4x_spot_balance_snapshot (
    as_of_ts            TIMESTAMP,
    as_of_date          DATE,
    wallet_address      STRING,
    wallet_group        INT,
    token_address       STRING,
    coin                STRING,
    total               DOUBLE,
    hold                DOUBLE,
    entry_ntl           DOUBLE,
    created_at          TIMESTAMP
) USING iceberg
PARTITIONED BY (as_of_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);
