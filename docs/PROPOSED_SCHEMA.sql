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

-- ============================================================================
-- v1.10.0 ROLLUPS (phases B and C): the token / market tables of the ETL contract,
-- built from the native candles (1m / 1h / 1d), l1_token_metadata (supply from
-- Info tokenDetails), token_whitelist and the tracked wallets' ledger, with the atlas
-- Trino view / Rust definitions (docs/ATLAS_WALLET_TABLE_FORMULAS.md, "token / market").
-- Written by blueblocx_hyperliquid.rollups after every lift. Column names and types
-- are the ETL contract's (chain_id is added by the ETL); `updated_at` = the write
-- stamp of the run that (re)wrote the row. Tables with a date column are replaced
-- per date, the rest whole. Wallet-derived counts (token_active_address, the active
-- address columns of token_summary, exchange_flow) cover the TRACKED wallet set.
-- ============================================================================

-- vw_market_volume_24h: the last 24 complete hours of market-wide trade volume from
-- the 1h candles (every priceable token; the EVM view sums the chain's transfers)
CREATE TABLE IF NOT EXISTS nessie.hypercore.market_volume_24h (
    hour_ts                 TIMESTAMP,
    volume_token_1h         DOUBLE,
    volume_usd_1h           DOUBLE,
    updated_at              TIMESTAMP
) USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

-- vw_market_cap_1d: closed days, close x total supply, with the atlas admission gates
CREATE TABLE IF NOT EXISTS nessie.hypercore.market_cap_latest (
    token_address           STRING,
    as_of_date              DATE,
    total_supply            DOUBLE,
    total_market_cap        DOUBLE,
    is_stablecoin           BOOLEAN,
    updated_at              TIMESTAMP
) USING iceberg
PARTITIONED BY (as_of_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

-- vw_market_fear_index v3: five factors, point-in-time 730-day midrank calibration
CREATE TABLE IF NOT EXISTS nessie.hypercore.market_fear_index (
    as_of_date              DATE,
    momentum_score          DOUBLE,
    breadth_score           DOUBLE,
    volatility_score        DOUBLE,
    volume_score            DOUBLE,
    market_comp_score       DOUBLE,
    fear_index              DOUBLE,
    market_profile          STRING,
    updated_at              TIMESTAMP
) USING iceberg
PARTITIONED BY (as_of_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

-- vw_token_stats: as-of-now price and 24h change from the 1m candles, all-time
-- high / low / average from the 1d candles, strength vs ETH (UETH)
CREATE TABLE IF NOT EXISTS nessie.hypercore.token_stats (
    token_address           STRING,
    last_updated_at         TIMESTAMP,
    current_price           DOUBLE,
    all_time_high           DOUBLE,
    all_time_low            DOUBLE,
    all_time_avg            DOUBLE,
    price_pct_change_24h    DOUBLE,
    eth_relative_strength   DOUBLE,
    performance_status      STRING,
    updated_at              TIMESTAMP
) USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

-- vw_token_price_gainers: 24h gainers anchored to the 1m data tip
CREATE TABLE IF NOT EXISTS nessie.hypercore.token_price_gainers (
    token_address           STRING,
    timeframe               STRING,
    current_ts              TIMESTAMP,
    previous_ts             TIMESTAMP,
    current_price_usd       DOUBLE,
    previous_price_usd      DOUBLE,
    price_change_pct        DOUBLE,
    rank_price_change       BIGINT,
    updated_at              TIMESTAMP
) USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

-- vw_token_new_rank: tokens deployed within 30 days, 0.5 return + 0.3 newness + 0.2 ln(trades)
CREATE TABLE IF NOT EXISTS nessie.hypercore.token_new_rank (
    token_address           STRING,
    current_ts              TIMESTAMP,
    previous_24h_ts         TIMESTAMP,
    current_price_usd       DOUBLE,
    price_24h_ago_usd       DOUBLE,
    price_change_24h_pct    DOUBLE,
    first_seen_ts           TIMESTAMP,
    token_age_days          BIGINT,
    tx_count_24h            BIGINT,
    ranking_score           DOUBLE,
    rank_new_token          BIGINT,
    updated_at              TIMESTAMP
) USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

-- vw_token_beta_volatility_sharp: 30-day log returns of the whitelisted tokens vs ETH
CREATE TABLE IF NOT EXISTS nessie.hypercore.token_beta_volatility_sharp (
    token_address           STRING,
    beta                    DOUBLE,
    desc_beta               STRING,
    annual_volatility       DOUBLE,
    sharp_ratio             DOUBLE,
    updated_at              TIMESTAMP
) USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

-- vw_token_activity_trending: last complete day vs the day before (trades, volume, price)
CREATE TABLE IF NOT EXISTS nessie.hypercore.token_activity_trending (
    token_address           STRING,
    activity_date           DATE,
    tx_count_today          BIGINT,
    tx_count_yesterday      BIGINT,
    tx_change_pct           DOUBLE,
    price_usd               DOUBLE,
    price_change_pct        DOUBLE,
    volume_token            DOUBLE,
    volume_change_pct       DOUBLE,
    rank_tx_change          BIGINT,
    rank_volume_change      BIGINT,
    updated_at              TIMESTAMP
) USING iceberg
PARTITIONED BY (activity_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

-- vw_token_active_address: distinct TRACKED wallets with a ledger row per token, last 24h
CREATE TABLE IF NOT EXISTS nessie.hypercore.token_active_address (
    token_address           STRING,
    active_address          BIGINT,
    updated_at              TIMESTAMP
) USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

-- vw_token_summary: price, live market cap, yesterday's volume, active (tracked)
-- wallets and the four ranks
CREATE TABLE IF NOT EXISTS nessie.hypercore.token_summary (
    token_address                   STRING,
    current_price_usd               DOUBLE,
    price_change_24h_pct            DOUBLE,
    market_cap_usd                  DOUBLE,
    volume_usd_24h                  DOUBLE,
    active_address_count_24h        BIGINT,
    active_address_change_24h_pct   DOUBLE,
    rank_trending                   BIGINT,
    rank_mover                      BIGINT,
    rank_new_coin                   BIGINT,
    rank_gainer                     BIGINT,
    updated_at                      TIMESTAMP
) USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

-- market_signals (Rust): per token per 1m bucket EMA9 / EMA21, 20-bucket volume spike
-- ratio (>= 2x = spike), 20-bucket resistance and breakout; one global row per
-- minute (global_flag = 'global', token_address = ''). Last 3 UTC days, replaced per day.
CREATE TABLE IF NOT EXISTS nessie.hypercore.token_market_signals (
    global_flag                 STRING,
    token_address               STRING,
    bucket_minute               TIMESTAMP,
    bucket_date                 DATE,
    close_price_usd             DOUBLE,
    ema9                        DOUBLE,
    ema21                       DOUBLE,
    ema_signal                  STRING,
    volume_token                DOUBLE,
    volume_token_spike_ratio    DOUBLE,
    volume_token_spike_signal   STRING,
    volume_usd                  DOUBLE,
    volume_usd_spike_ratio      DOUBLE,
    volume_usd_spike_signal     STRING,
    resistance_price            DOUBLE,
    price_breakout              STRING,
    created_at                  TIMESTAMP,
    updated_at                  TIMESTAMP
) USING iceberg
PARTITIONED BY (bucket_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

-- token_quality (Rust): per token per day, trades in the window and the largest
-- minute-over-minute vwap jump against the v1_trades_jump thresholds (>= 10 trades,
-- jump <= 0.80). Last 3 UTC days, replaced per day.
CREATE TABLE IF NOT EXISTS nessie.hypercore.token_quality (
    as_of_date              DATE,
    quality_version         STRING,
    token_address           STRING,
    lookback_hours          INT,
    trades_in_window        BIGINT,
    max_jump_from_prev      DOUBLE,
    trade_threshold         INT,
    jump_threshold          DOUBLE,
    score                   INT,
    is_pass                 BOOLEAN,
    fail_reason             STRING,
    latest_bucket_minute    TIMESTAMP,
    created_at              TIMESTAMP,
    updated_at              TIMESTAMP
) USING iceberg
PARTITIONED BY (as_of_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

-- l3_dex_pair_volume_1d: one HyperCore spot market = one pair (TOKEN/USDC) = one pool;
-- the day's volume and trade count from the market's 1d candle (decision 7A: the
-- direction split is not observable market-wide, the per-direction columns are 0)
CREATE TABLE IF NOT EXISTS nessie.hypercore.dex_pair_volume_1d (
    bucket_date                     DATE,
    pair_address                    STRING,
    pair_symbol                     STRING,
    token0_address                  STRING,
    token1_address                  STRING,
    token0_symbol                   STRING,
    token1_symbol                   STRING,
    amount_token0_to_token1         DOUBLE,
    amount_token1_to_token0         DOUBLE,
    volume_usd_token0_to_token1     DOUBLE,
    volume_usd_token1_to_token0     DOUBLE,
    volume_usd                      DOUBLE,
    swap_count_token0_to_token1     INT,
    swap_count_token1_to_token0     INT,
    swap_count_total                INT,
    pool_count                      INT,
    created_at                      TIMESTAMP,
    updated_at                      TIMESTAMP
) USING iceberg
PARTITIONED BY (bucket_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

-- vw_exchange_flow: daily USDC that the TRACKED wallets bridged into (deposit) and
-- out of (withdraw) HyperCore, closed days of the last 31
CREATE TABLE IF NOT EXISTS nessie.hypercore.exchange_flow (
    bucket_date             DATE,
    incoming_volume_usd     DOUBLE,
    outgoing_volume_usd     DOUBLE,
    net_flow_usd            DOUBLE,
    updated_at              TIMESTAMP
) USING iceberg
PARTITIONED BY (bucket_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);
