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
--   l3_token_price_usd_1m     (native 1m candles, keyed by the ledger token_address)
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
