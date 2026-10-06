-- ============================================================================
-- BlueBlocx Iceberg table schema
--
-- Canonical rule:
--   - Arbitrum is the reference schema.
--   - Ethereum mirrors Arbitrum for all shared chain-local tables.
--   - This file lists only active tables used by the current Dagster/Spark/Rust
--     pipeline and Trino serving views.
--
-- Usage:
--   spark-submit services/spark-jobs/jobs/setup/create_tables.py
--   spark-sql --master spark://localhost:7077 -f ICEBERG_SCHEMA.sql
--
-- Notes:
--   - CREATE TABLE IF NOT EXISTS will not migrate existing Iceberg tables.
--   - Existing production table shape changes still need explicit migration or
--     drop/recreate.
-- ============================================================================

CREATE NAMESPACE IF NOT EXISTS nessie.control;
CREATE NAMESPACE IF NOT EXISTS nessie.ethereum;
CREATE NAMESPACE IF NOT EXISTS nessie.arbitrum;
CREATE NAMESPACE IF NOT EXISTS nessie.all_chain;

-- ============================================================================
-- CONTROL METADATA
-- ============================================================================

CREATE TABLE IF NOT EXISTS nessie.control.table_watermarks (
    namespace       STRING NOT NULL,
    table_name      STRING NOT NULL,
    layer           STRING,
    producer        STRING,
    watermark_col   STRING NOT NULL,
    watermark_date  DATE,
    watermark_ts    TIMESTAMP,
    row_count       BIGINT,
    snapshot_id     STRING,
    job_name        STRING,
    run_id          STRING,
    updated_at      TIMESTAMP NOT NULL,
    status          STRING NOT NULL
) USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

ALTER TABLE nessie.control.table_watermarks WRITE ORDERED BY namespace ASC NULLS LAST, table_name ASC NULLS LAST;

-- Per-block ingestion completeness. One row per (chain, block_number) for every
-- block we've processed, including zero-log blocks. Source of truth for block
-- completeness — independent of any external dataset (BQ, archive node, etc.).
CREATE TABLE IF NOT EXISTS nessie.control.raw_blocks_ingested (
    chain                STRING NOT NULL,
    block_number         BIGINT NOT NULL,
    block_timestamp      TIMESTAMP NOT NULL,
    logs_count           BIGINT NOT NULL,
    status               STRING NOT NULL,  -- completed | empty | failed | in_progress
    ingest_source        STRING NOT NULL,  -- quicknode | bigquery | archive | ...
    provider_request_id  STRING,
    attempt              INT NOT NULL,
    ingested_at          TIMESTAMP NOT NULL,
    job_run_id           STRING
) USING iceberg
PARTITIONED BY (days(block_timestamp))
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

ALTER TABLE nessie.control.raw_blocks_ingested WRITE ORDERED BY chain ASC NULLS LAST, block_number ASC NULLS LAST;

-- ============================================================================
-- ETHEREUM CHAIN
-- ============================================================================

CREATE TABLE IF NOT EXISTS nessie.ethereum.raw_logs (
    block_hash          STRING,
    block_number        BIGINT,
    block_timestamp     TIMESTAMP,
    transaction_hash    STRING,
    transaction_index   BIGINT,
    log_index           BIGINT,
    address             STRING,
    data                STRING,
    topics              ARRAY<STRING>,
    removed             BOOLEAN
) USING iceberg
PARTITIONED BY (days(block_timestamp))
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

CREATE TABLE IF NOT EXISTS nessie.ethereum.l0_dex_pools (
    factory_address     STRING,
    token0              STRING,
    token1              STRING,
    pool_address        STRING,
    fee_tier            INT,
    block_number        BIGINT,
    block_timestamp     TIMESTAMP,
    tx_hash             STRING,
    created_at          TIMESTAMP,
    dex                 STRING,
    dex_version         STRING,
    dex_type            STRING
) USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

CREATE TABLE IF NOT EXISTS nessie.ethereum.l1_wallet_ledger (
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

CREATE TABLE IF NOT EXISTS nessie.ethereum.l1_token_metadata (
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

CREATE TABLE IF NOT EXISTS nessie.ethereum.l1_token_categories (
    token_address       STRING,
    token_group         INT,
    category            STRING,
    source              STRING,
    created_at          TIMESTAMP
) USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

CREATE TABLE IF NOT EXISTS nessie.ethereum.l1_token_implementations (
    token_address       STRING,
    token_group         INT,
    implementation      STRING,
    created_at          TIMESTAMP
) USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

CREATE TABLE IF NOT EXISTS nessie.ethereum.l1_token_links (
    token_address       STRING,
    token_group         INT,
    link_type           STRING,
    url                 STRING,
    created_at          TIMESTAMP
) USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

CREATE TABLE IF NOT EXISTS nessie.ethereum.l2_token_supply (
    token_address         STRING,
    net_mint_burn_amount DOUBLE,
    total_supply         DOUBLE,
    cnt_txn              INT,
    block_timestamp      TIMESTAMP,
    block_date           DATE
) USING iceberg
PARTITIONED BY (block_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

CREATE TABLE IF NOT EXISTS nessie.ethereum.l2_dex_price_weth_in_stable_swaps (
    dex                     STRING,
    dex_version             STRING,
    factory_address         STRING,
    token_address           STRING,
    block_timestamp         TIMESTAMP,
    block_date              DATE,
    tx_hash                 STRING,
    log_index               INT,
    pool_address            STRING,
    token0                  STRING,
    token1                  STRING,
    quoted_in_token         STRING,
    price_in_counter_token  DOUBLE,
    tick                    INT,
    price_token1_per_token0 DOUBLE,
    amount_token_raw        DOUBLE,
    amount_token            DOUBLE,
    amount_quoted_raw       DOUBLE,
    amount_quoted           DOUBLE,
    created_at              TIMESTAMP
) USING iceberg
PARTITIONED BY (block_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

CREATE TABLE IF NOT EXISTS nessie.ethereum.l2_dex_price_token_in_weth_swaps (
    dex                     STRING,
    dex_version             STRING,
    factory_address         STRING,
    token_address           STRING,
    block_timestamp         TIMESTAMP,
    block_date              DATE,
    tx_hash                 STRING,
    log_index               INT,
    pool_address            STRING,
    token0                  STRING,
    token1                  STRING,
    quoted_in_token         STRING,
    price_in_counter_token  DOUBLE,
    tick                    INT,
    price_token1_per_token0 DOUBLE,
    amount_token_raw        DOUBLE,
    amount_token            DOUBLE,
    amount_quoted_raw       DOUBLE,
    amount_quoted           DOUBLE,
    created_at              TIMESTAMP
) USING iceberg
PARTITIONED BY (block_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

CREATE TABLE IF NOT EXISTS nessie.ethereum.l3_weth_price_usd_1m (
    bucket_minute       TIMESTAMP,
    bucket_date         DATE,
    open_price_usd      DOUBLE,
    high_price_usd      DOUBLE,
    low_price_usd       DOUBLE,
    close_price_usd     DOUBLE,
    vwap_price_usd      DOUBLE,
    volume_weth         DOUBLE,
    volume_usd          DOUBLE,
    trade_count         INT,
    created_at          TIMESTAMP,
    updated_at          TIMESTAMP
) USING iceberg
PARTITIONED BY (bucket_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

CREATE TABLE IF NOT EXISTS nessie.ethereum.l3_token_price_usd (
    token_address           STRING,
    token_group             INT,
    block_timestamp         TIMESTAMP,
    block_date              DATE,
    bucket_minute           TIMESTAMP,
    tx_hash                 STRING,
    log_index               INT,
    pool_address            STRING,
    dex                     STRING,
    dex_version             STRING,
    price_in_counter_token  DOUBLE,
    weth_price_usd          DOUBLE,
    token_price_usd         DOUBLE,
    amount_token            DOUBLE,
    swap_value_usd          DOUBLE,
    created_at              TIMESTAMP
) USING iceberg
PARTITIONED BY (block_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

CREATE TABLE IF NOT EXISTS nessie.ethereum.l3_token_price_usd_1m (
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

CREATE TABLE IF NOT EXISTS nessie.ethereum.l3_dex_pair_volume_1d (
    bucket_date                    DATE,
    pair_address                   STRING,
    pair_symbol                    STRING,
    token0_address                 STRING,
    token1_address                 STRING,
    token0_symbol                  STRING,
    token1_symbol                  STRING,
    amount_token0_to_token1        DOUBLE,
    amount_token1_to_token0        DOUBLE,
    volume_usd_token0_to_token1    DOUBLE,
    volume_usd_token1_to_token0    DOUBLE,
    volume_usd                     DOUBLE,
    swap_count_token0_to_token1    INT,
    swap_count_token1_to_token0    INT,
    swap_count_total               INT,
    created_at                     TIMESTAMP,
    updated_at                     TIMESTAMP
) USING iceberg
PARTITIONED BY (bucket_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

CREATE TABLE IF NOT EXISTS nessie.ethereum.l3_wallet_statement_watchlist (
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
PARTITIONED BY (block_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

CREATE TABLE IF NOT EXISTS nessie.ethereum.l3_wallet_statement_watchlist_filtered_tokens (
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
PARTITIONED BY (block_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

CREATE TABLE IF NOT EXISTS nessie.ethereum.l3_wallet_statement_usd_watchlist_1m (
    wallet_address      STRING,
    wallet_group        INT,
    token_address       STRING,
    counterparty_address STRING,
    block_timestamp     TIMESTAMP,
    block_date          DATE,
    tx_hash             STRING,
    event_index         INT,
    direction           STRING,
    amount_raw          DOUBLE,
    amount              DOUBLE,
    amount_signed       DOUBLE,
    price_bucket_start  TIMESTAMP,
    price_usd           DOUBLE,
    notional_usd        DOUBLE,
    notional_usd_signed DOUBLE,
    created_at          TIMESTAMP,
    updated_at          TIMESTAMP
) USING iceberg
PARTITIONED BY (block_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

CREATE TABLE IF NOT EXISTS nessie.ethereum.token_whitelist (
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

CREATE TABLE IF NOT EXISTS nessie.ethereum.token_quality (
    as_of_date           DATE,
    quality_version      STRING,
    token_address        STRING,
    lookback_hours       INT,
    trades_in_window     BIGINT,
    max_jump_from_prev   DOUBLE,
    trade_threshold      INT,
    jump_threshold       DOUBLE,
    score                INT,
    is_pass              BOOLEAN,
    fail_reason          STRING,
    latest_bucket_minute TIMESTAMP,
    created_at           TIMESTAMP,
    updated_at           TIMESTAMP
) USING iceberg
PARTITIONED BY (as_of_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

CREATE TABLE IF NOT EXISTS nessie.ethereum.wallet_watchlist (
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

CREATE TABLE IF NOT EXISTS nessie.ethereum.l4_wallet_balance_per_tx (
    wallet_address      STRING,
    wallet_group        INT,
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
PARTITIONED BY (block_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

CREATE TABLE IF NOT EXISTS nessie.ethereum.l4_wallet_balance_state (
    wallet_address          STRING,
    token_address           STRING,
    last_block_timestamp    TIMESTAMP,
    last_block_date         DATE,
    tx_hash                 STRING,
    event_index             INT,
    balance_amount          DOUBLE,
    updated_at              TIMESTAMP
) USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

CREATE TABLE IF NOT EXISTS nessie.ethereum.l4_wallet_balance_source_stats (
    wallet_address          STRING,
    token_address           STRING,
    source_row_count        BIGINT,
    last_block_timestamp    TIMESTAMP,
    last_tx_hash            STRING,
    last_event_index        INT,
    updated_at              TIMESTAMP
) USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

CREATE TABLE IF NOT EXISTS nessie.ethereum.l4_wallet_balance_1d_dense (
    wallet_address          STRING,
    wallet_group            INT,
    token_address           STRING,
    token_group             INT,
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
PARTITIONED BY (snapshot_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

CREATE TABLE IF NOT EXISTS nessie.ethereum.l4_wallet_pnl_realized_per_sell_tx (
    wallet_address      STRING,
    wallet_group        INT,
    token_address       STRING,
    block_timestamp     TIMESTAMP,
    block_date          DATE,
    tx_hash             STRING,
    event_index         INT,
    sell_qty            DOUBLE,
    sell_price_usd      DOUBLE,
    proceeds_usd        DOUBLE,
    matched_qty         DOUBLE,
    cost_usd            DOUBLE,
    realized_pnl_usd    DOUBLE,
    created_at          TIMESTAMP,
    updated_at          TIMESTAMP
) USING iceberg
PARTITIONED BY (block_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

CREATE TABLE IF NOT EXISTS nessie.ethereum.l4_wallet_pnl_fifo_open_lots (
    wallet_address          STRING,
    wallet_group            INT,
    token_address           STRING,
    lot_seq                 INT,
    buy_block_timestamp     TIMESTAMP,
    buy_tx_hash             STRING,
    buy_event_index         INT,
    buy_price_usd           DOUBLE,
    remaining_qty           DOUBLE,
    remaining_cost_usd      DOUBLE,
    created_at              TIMESTAMP,
    updated_at              TIMESTAMP
) USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

CREATE TABLE IF NOT EXISTS nessie.ethereum.l4_wallet_pnl_fifo_checkpoint (
    wallet_address          STRING,
    wallet_group            INT,
    token_address           STRING,
    last_block_timestamp    TIMESTAMP,
    last_tx_hash            STRING,
    last_event_index        INT,
    created_at              TIMESTAMP,
    updated_at              TIMESTAMP
) USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

CREATE TABLE IF NOT EXISTS nessie.ethereum.l4_wallet_pnl_state_fifo (
    wallet_address          STRING,
    wallet_group            INT,
    token_address           STRING,
    last_block_timestamp    TIMESTAMP,
    last_tx_hash            STRING,
    last_event_index        INT,
    open_qty                DOUBLE,
    open_cost_usd           DOUBLE,
    updated_at              TIMESTAMP
) USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

CREATE TABLE IF NOT EXISTS nessie.ethereum.l4_wallet_pnl_realized_1d (
    snapshot_date       DATE,
    wallet_address      STRING,
    wallet_group        INT,
    realized_pnl_usd    DOUBLE,
    proceeds_usd        DOUBLE,
    cost_usd            DOUBLE,
    num_sells           INT,
    created_at          TIMESTAMP,
    updated_at          TIMESTAMP
) USING iceberg
PARTITIONED BY (snapshot_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

CREATE TABLE IF NOT EXISTS nessie.ethereum.l4_wallet_pnl_fifo_source_stats (
    wallet_address          STRING,
    wallet_group            INT,
    token_address           STRING,
    source_row_count        BIGINT,
    last_block_timestamp    TIMESTAMP,
    last_tx_hash            STRING,
    last_event_index        INT,
    updated_at              TIMESTAMP
) USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

CREATE TABLE IF NOT EXISTS nessie.ethereum.l4_wallet_networth_1d (
    wallet_address      STRING,
    snapshot_date       DATE,
    net_worth_usd       DOUBLE,
    created_at          TIMESTAMP,
    updated_at          TIMESTAMP
) USING iceberg
PARTITIONED BY (snapshot_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

CREATE TABLE IF NOT EXISTS nessie.ethereum.l4_wallet_networth_1d_dense (
    wallet_address              STRING,
    wallet_group                INT,
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
PARTITIONED BY (snapshot_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

CREATE TABLE IF NOT EXISTS nessie.ethereum.l4_wallet_metadata (
    wallet_address          STRING,
    first_block_timestamp   TIMESTAMP,
    latest_block_timestamp  TIMESTAMP,
    count_tx                INT,
    first_tx                STRING,
    latest_tx               STRING,
    active_days             INT,
    last_active             INT,
    active_on_avg           DOUBLE,
    updated_at              TIMESTAMP
) USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

ALTER TABLE nessie.ethereum.raw_logs WRITE ORDERED BY address ASC NULLS LAST, block_timestamp ASC NULLS LAST;
ALTER TABLE nessie.ethereum.l0_dex_pools WRITE ORDERED BY pool_address ASC NULLS LAST;
ALTER TABLE nessie.ethereum.l1_wallet_ledger WRITE ORDERED BY wallet_group ASC NULLS LAST, wallet_address ASC NULLS LAST, token_address ASC NULLS LAST, block_timestamp ASC NULLS LAST;
ALTER TABLE nessie.ethereum.l1_token_metadata WRITE ORDERED BY token_group ASC NULLS LAST, token_address ASC NULLS LAST;
ALTER TABLE nessie.ethereum.l1_token_categories WRITE ORDERED BY token_group ASC NULLS LAST, token_address ASC NULLS LAST, category ASC NULLS LAST;
ALTER TABLE nessie.ethereum.l1_token_implementations WRITE ORDERED BY token_group ASC NULLS LAST, token_address ASC NULLS LAST;
ALTER TABLE nessie.ethereum.l1_token_links WRITE ORDERED BY token_group ASC NULLS LAST, token_address ASC NULLS LAST;
ALTER TABLE nessie.ethereum.l2_token_supply WRITE ORDERED BY token_address ASC NULLS LAST, block_date ASC NULLS LAST;
ALTER TABLE nessie.ethereum.l2_dex_price_weth_in_stable_swaps WRITE ORDERED BY block_timestamp ASC NULLS LAST;
ALTER TABLE nessie.ethereum.l2_dex_price_token_in_weth_swaps WRITE ORDERED BY token_address ASC NULLS LAST, block_timestamp ASC NULLS LAST;
ALTER TABLE nessie.ethereum.l3_weth_price_usd_1m WRITE ORDERED BY bucket_minute ASC NULLS LAST;
ALTER TABLE nessie.ethereum.l3_token_price_usd WRITE ORDERED BY token_address ASC NULLS LAST, bucket_minute ASC NULLS LAST;
ALTER TABLE nessie.ethereum.l3_token_price_usd_1m WRITE ORDERED BY token_address ASC NULLS LAST, bucket_minute ASC NULLS LAST;
ALTER TABLE nessie.ethereum.l3_dex_pair_volume_1d WRITE ORDERED BY token0_address ASC NULLS LAST, token1_address ASC NULLS LAST;
ALTER TABLE nessie.ethereum.l3_wallet_statement_watchlist WRITE ORDERED BY wallet_group ASC NULLS LAST, wallet_address ASC NULLS LAST, token_address ASC NULLS LAST, block_timestamp ASC NULLS LAST;
ALTER TABLE nessie.ethereum.l3_wallet_statement_watchlist_filtered_tokens WRITE ORDERED BY wallet_group ASC NULLS LAST, wallet_address ASC NULLS LAST, token_address ASC NULLS LAST, block_timestamp ASC NULLS LAST;
ALTER TABLE nessie.ethereum.l3_wallet_statement_usd_watchlist_1m WRITE ORDERED BY wallet_group ASC NULLS LAST, wallet_address ASC NULLS LAST, token_address ASC NULLS LAST, block_timestamp ASC NULLS LAST;
ALTER TABLE nessie.ethereum.token_whitelist WRITE ORDERED BY token_group ASC NULLS LAST, token_address ASC NULLS LAST;
ALTER TABLE nessie.ethereum.token_quality WRITE ORDERED BY token_address ASC NULLS LAST;
ALTER TABLE nessie.ethereum.wallet_watchlist WRITE ORDERED BY wallet_group ASC NULLS LAST, wallet_address ASC NULLS LAST;
ALTER TABLE nessie.ethereum.l4_wallet_balance_per_tx WRITE ORDERED BY wallet_group ASC NULLS LAST, wallet_address ASC NULLS LAST, token_address ASC NULLS LAST, block_timestamp ASC NULLS LAST;
ALTER TABLE nessie.ethereum.l4_wallet_balance_state WRITE ORDERED BY wallet_address ASC NULLS LAST, token_address ASC NULLS LAST;
ALTER TABLE nessie.ethereum.l4_wallet_balance_source_stats WRITE ORDERED BY wallet_address ASC NULLS LAST, token_address ASC NULLS LAST;
ALTER TABLE nessie.ethereum.l4_wallet_balance_1d_dense WRITE ORDERED BY wallet_group ASC NULLS LAST, wallet_address ASC NULLS LAST, snapshot_date ASC NULLS LAST, token_address ASC NULLS LAST;
ALTER TABLE nessie.ethereum.l4_wallet_pnl_realized_per_sell_tx WRITE ORDERED BY wallet_group ASC NULLS LAST, wallet_address ASC NULLS LAST, token_address ASC NULLS LAST, block_timestamp ASC NULLS LAST;
ALTER TABLE nessie.ethereum.l4_wallet_pnl_fifo_open_lots WRITE ORDERED BY wallet_group ASC NULLS LAST, wallet_address ASC NULLS LAST, token_address ASC NULLS LAST, lot_seq ASC NULLS LAST;
ALTER TABLE nessie.ethereum.l4_wallet_pnl_fifo_checkpoint WRITE ORDERED BY wallet_group ASC NULLS LAST, wallet_address ASC NULLS LAST, token_address ASC NULLS LAST;
ALTER TABLE nessie.ethereum.l4_wallet_pnl_state_fifo WRITE ORDERED BY wallet_group ASC NULLS LAST, wallet_address ASC NULLS LAST, token_address ASC NULLS LAST;
ALTER TABLE nessie.ethereum.l4_wallet_pnl_realized_1d WRITE ORDERED BY wallet_group ASC NULLS LAST, wallet_address ASC NULLS LAST, snapshot_date ASC NULLS LAST;
ALTER TABLE nessie.ethereum.l4_wallet_pnl_fifo_source_stats WRITE ORDERED BY wallet_group ASC NULLS LAST, wallet_address ASC NULLS LAST, token_address ASC NULLS LAST;
ALTER TABLE nessie.ethereum.l4_wallet_networth_1d WRITE ORDERED BY wallet_address ASC NULLS LAST, snapshot_date ASC NULLS LAST;
ALTER TABLE nessie.ethereum.l4_wallet_networth_1d_dense WRITE ORDERED BY wallet_group ASC NULLS LAST, wallet_address ASC NULLS LAST, snapshot_date ASC NULLS LAST;
ALTER TABLE nessie.ethereum.l4_wallet_metadata WRITE ORDERED BY wallet_address ASC NULLS LAST;

-- ============================================================================
-- ARBITRUM CHAIN
-- ============================================================================

CREATE TABLE IF NOT EXISTS nessie.arbitrum.raw_logs (
    block_hash          STRING,
    block_number        BIGINT,
    block_timestamp     TIMESTAMP,
    transaction_hash    STRING,
    transaction_index   BIGINT,
    log_index           BIGINT,
    address             STRING,
    data                STRING,
    topics              ARRAY<STRING>,
    removed             BOOLEAN
) USING iceberg
PARTITIONED BY (days(block_timestamp))
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

CREATE TABLE IF NOT EXISTS nessie.arbitrum.l0_dex_pools (
    factory_address     STRING,
    token0              STRING,
    token1              STRING,
    pool_address        STRING,
    fee_tier            INT,
    block_number        BIGINT,
    block_timestamp     TIMESTAMP,
    tx_hash             STRING,
    created_at          TIMESTAMP,
    dex                 STRING,
    dex_version         STRING,
    dex_type            STRING
) USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

CREATE TABLE IF NOT EXISTS nessie.arbitrum.l1_wallet_ledger (
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

CREATE TABLE IF NOT EXISTS nessie.arbitrum.l1_token_metadata (
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

CREATE TABLE IF NOT EXISTS nessie.arbitrum.l1_token_categories (
    token_address       STRING,
    token_group         INT,
    category            STRING,
    source              STRING,
    created_at          TIMESTAMP
) USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

CREATE TABLE IF NOT EXISTS nessie.arbitrum.l1_token_implementations (
    token_address       STRING,
    token_group         INT,
    implementation      STRING,
    created_at          TIMESTAMP
) USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

CREATE TABLE IF NOT EXISTS nessie.arbitrum.l1_token_links (
    token_address       STRING,
    token_group         INT,
    link_type           STRING,
    url                 STRING,
    created_at          TIMESTAMP
) USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

CREATE TABLE IF NOT EXISTS nessie.arbitrum.l2_token_supply (
    token_address         STRING,
    net_mint_burn_amount DOUBLE,
    total_supply         DOUBLE,
    cnt_txn              INT,
    block_timestamp      TIMESTAMP,
    block_date           DATE
) USING iceberg
PARTITIONED BY (block_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

CREATE TABLE IF NOT EXISTS nessie.arbitrum.l2_dex_price_weth_in_stable_swaps (
    dex                     STRING,
    dex_version             STRING,
    factory_address         STRING,
    token_address           STRING,
    block_timestamp         TIMESTAMP,
    block_date              DATE,
    tx_hash                 STRING,
    log_index               INT,
    pool_address            STRING,
    token0                  STRING,
    token1                  STRING,
    quoted_in_token         STRING,
    price_in_counter_token  DOUBLE,
    tick                    INT,
    price_token1_per_token0 DOUBLE,
    amount_token_raw        DOUBLE,
    amount_token            DOUBLE,
    amount_quoted_raw       DOUBLE,
    amount_quoted           DOUBLE,
    created_at              TIMESTAMP
) USING iceberg
PARTITIONED BY (block_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

CREATE TABLE IF NOT EXISTS nessie.arbitrum.l2_dex_price_token_in_weth_swaps (
    dex                     STRING,
    dex_version             STRING,
    factory_address         STRING,
    token_address           STRING,
    block_timestamp         TIMESTAMP,
    block_date              DATE,
    tx_hash                 STRING,
    log_index               INT,
    pool_address            STRING,
    token0                  STRING,
    token1                  STRING,
    quoted_in_token         STRING,
    price_in_counter_token  DOUBLE,
    tick                    INT,
    price_token1_per_token0 DOUBLE,
    amount_token_raw        DOUBLE,
    amount_token            DOUBLE,
    amount_quoted_raw       DOUBLE,
    amount_quoted           DOUBLE,
    created_at              TIMESTAMP
) USING iceberg
PARTITIONED BY (block_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

CREATE TABLE IF NOT EXISTS nessie.arbitrum.l3_weth_price_usd_1m (
    bucket_minute       TIMESTAMP,
    bucket_date         DATE,
    open_price_usd      DOUBLE,
    high_price_usd      DOUBLE,
    low_price_usd       DOUBLE,
    close_price_usd     DOUBLE,
    vwap_price_usd      DOUBLE,
    volume_weth         DOUBLE,
    volume_usd          DOUBLE,
    trade_count         INT,
    created_at          TIMESTAMP,
    updated_at          TIMESTAMP
) USING iceberg
PARTITIONED BY (bucket_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

CREATE TABLE IF NOT EXISTS nessie.arbitrum.l3_token_price_usd (
    token_address           STRING,
    token_group             INT,
    block_timestamp         TIMESTAMP,
    block_date              DATE,
    bucket_minute           TIMESTAMP,
    tx_hash                 STRING,
    log_index               INT,
    pool_address            STRING,
    dex                     STRING,
    dex_version             STRING,
    price_in_counter_token  DOUBLE,
    weth_price_usd          DOUBLE,
    token_price_usd         DOUBLE,
    amount_token            DOUBLE,
    swap_value_usd          DOUBLE,
    created_at              TIMESTAMP
) USING iceberg
PARTITIONED BY (block_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

CREATE TABLE IF NOT EXISTS nessie.arbitrum.l3_token_price_usd_1m (
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

CREATE TABLE IF NOT EXISTS nessie.arbitrum.l3_dex_pair_volume_1d (
    bucket_date                    DATE,
    pair_address                   STRING,
    pair_symbol                    STRING,
    token0_address                 STRING,
    token1_address                 STRING,
    token0_symbol                  STRING,
    token1_symbol                  STRING,
    amount_token0_to_token1        DOUBLE,
    amount_token1_to_token0        DOUBLE,
    volume_usd_token0_to_token1    DOUBLE,
    volume_usd_token1_to_token0    DOUBLE,
    volume_usd                     DOUBLE,
    swap_count_token0_to_token1    INT,
    swap_count_token1_to_token0    INT,
    swap_count_total               INT,
    created_at                     TIMESTAMP,
    updated_at                     TIMESTAMP
) USING iceberg
PARTITIONED BY (bucket_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

CREATE TABLE IF NOT EXISTS nessie.arbitrum.l3_wallet_statement_watchlist (
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
PARTITIONED BY (block_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

CREATE TABLE IF NOT EXISTS nessie.arbitrum.l3_wallet_statement_watchlist_filtered_tokens (
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
PARTITIONED BY (block_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

CREATE TABLE IF NOT EXISTS nessie.arbitrum.l3_wallet_statement_usd_watchlist_1m (
    wallet_address      STRING,
    wallet_group        INT,
    token_address       STRING,
    counterparty_address STRING,
    block_timestamp     TIMESTAMP,
    block_date          DATE,
    tx_hash             STRING,
    event_index         INT,
    direction           STRING,
    amount_raw          DOUBLE,
    amount              DOUBLE,
    amount_signed       DOUBLE,
    price_bucket_start  TIMESTAMP,
    price_usd           DOUBLE,
    notional_usd        DOUBLE,
    notional_usd_signed DOUBLE,
    created_at          TIMESTAMP,
    updated_at          TIMESTAMP
) USING iceberg
PARTITIONED BY (block_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

CREATE TABLE IF NOT EXISTS nessie.arbitrum.token_whitelist (
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

CREATE TABLE IF NOT EXISTS nessie.arbitrum.token_quality (
    as_of_date           DATE,
    quality_version      STRING,
    token_address        STRING,
    lookback_hours       INT,
    trades_in_window     BIGINT,
    max_jump_from_prev   DOUBLE,
    trade_threshold      INT,
    jump_threshold       DOUBLE,
    score                INT,
    is_pass              BOOLEAN,
    fail_reason          STRING,
    latest_bucket_minute TIMESTAMP,
    created_at           TIMESTAMP,
    updated_at           TIMESTAMP
) USING iceberg
PARTITIONED BY (as_of_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

CREATE TABLE IF NOT EXISTS nessie.arbitrum.wallet_watchlist (
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

CREATE TABLE IF NOT EXISTS nessie.arbitrum.l4_wallet_balance_per_tx (
    wallet_address      STRING,
    wallet_group        INT,
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
PARTITIONED BY (block_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

CREATE TABLE IF NOT EXISTS nessie.arbitrum.l4_wallet_balance_state (
    wallet_address          STRING,
    token_address           STRING,
    last_block_timestamp    TIMESTAMP,
    last_block_date         DATE,
    tx_hash                 STRING,
    event_index             INT,
    balance_amount          DOUBLE,
    updated_at              TIMESTAMP
) USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

CREATE TABLE IF NOT EXISTS nessie.arbitrum.l4_wallet_balance_source_stats (
    wallet_address          STRING,
    token_address           STRING,
    source_row_count        BIGINT,
    last_block_timestamp    TIMESTAMP,
    last_tx_hash            STRING,
    last_event_index        INT,
    updated_at              TIMESTAMP
) USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

CREATE TABLE IF NOT EXISTS nessie.arbitrum.l4_wallet_balance_1d_dense (
    wallet_address          STRING,
    wallet_group            INT,
    token_address           STRING,
    token_group             INT,
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
PARTITIONED BY (snapshot_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

CREATE TABLE IF NOT EXISTS nessie.arbitrum.l4_wallet_pnl_realized_per_sell_tx (
    wallet_address      STRING,
    wallet_group        INT,
    token_address       STRING,
    block_timestamp     TIMESTAMP,
    block_date          DATE,
    tx_hash             STRING,
    event_index         INT,
    sell_qty            DOUBLE,
    sell_price_usd      DOUBLE,
    proceeds_usd        DOUBLE,
    matched_qty         DOUBLE,
    cost_usd            DOUBLE,
    realized_pnl_usd    DOUBLE,
    created_at          TIMESTAMP,
    updated_at          TIMESTAMP
) USING iceberg
PARTITIONED BY (block_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

CREATE TABLE IF NOT EXISTS nessie.arbitrum.l4_wallet_pnl_fifo_open_lots (
    wallet_address          STRING,
    wallet_group            INT,
    token_address           STRING,
    lot_seq                 INT,
    buy_block_timestamp     TIMESTAMP,
    buy_tx_hash             STRING,
    buy_event_index         INT,
    buy_price_usd           DOUBLE,
    remaining_qty           DOUBLE,
    remaining_cost_usd      DOUBLE,
    created_at              TIMESTAMP,
    updated_at              TIMESTAMP
) USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

CREATE TABLE IF NOT EXISTS nessie.arbitrum.l4_wallet_pnl_fifo_checkpoint (
    wallet_address          STRING,
    wallet_group            INT,
    token_address           STRING,
    last_block_timestamp    TIMESTAMP,
    last_tx_hash            STRING,
    last_event_index        INT,
    created_at              TIMESTAMP,
    updated_at              TIMESTAMP
) USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

CREATE TABLE IF NOT EXISTS nessie.arbitrum.l4_wallet_pnl_state_fifo (
    wallet_address          STRING,
    wallet_group            INT,
    token_address           STRING,
    last_block_timestamp    TIMESTAMP,
    last_tx_hash            STRING,
    last_event_index        INT,
    open_qty                DOUBLE,
    open_cost_usd           DOUBLE,
    updated_at              TIMESTAMP
) USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

CREATE TABLE IF NOT EXISTS nessie.arbitrum.l4_wallet_pnl_realized_1d (
    snapshot_date       DATE,
    wallet_address      STRING,
    wallet_group        INT,
    realized_pnl_usd    DOUBLE,
    proceeds_usd        DOUBLE,
    cost_usd            DOUBLE,
    num_sells           INT,
    created_at          TIMESTAMP,
    updated_at          TIMESTAMP
) USING iceberg
PARTITIONED BY (snapshot_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

CREATE TABLE IF NOT EXISTS nessie.arbitrum.l4_wallet_pnl_fifo_source_stats (
    wallet_address          STRING,
    wallet_group            INT,
    token_address           STRING,
    source_row_count        BIGINT,
    last_block_timestamp    TIMESTAMP,
    last_tx_hash            STRING,
    last_event_index        INT,
    updated_at              TIMESTAMP
) USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

CREATE TABLE IF NOT EXISTS nessie.arbitrum.l4_wallet_networth_1d (
    wallet_address      STRING,
    snapshot_date       DATE,
    net_worth_usd       DOUBLE,
    created_at          TIMESTAMP,
    updated_at          TIMESTAMP
) USING iceberg
PARTITIONED BY (snapshot_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

CREATE TABLE IF NOT EXISTS nessie.arbitrum.l4_wallet_networth_1d_dense (
    wallet_address              STRING,
    wallet_group                INT,
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
PARTITIONED BY (snapshot_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

CREATE TABLE IF NOT EXISTS nessie.arbitrum.l4_wallet_metadata (
    wallet_address          STRING,
    first_block_timestamp   TIMESTAMP,
    latest_block_timestamp  TIMESTAMP,
    count_tx                INT,
    first_tx                STRING,
    latest_tx               STRING,
    active_days             INT,
    last_active             INT,
    active_on_avg           DOUBLE,
    updated_at              TIMESTAMP
) USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

ALTER TABLE nessie.arbitrum.raw_logs WRITE ORDERED BY address ASC NULLS LAST, block_timestamp ASC NULLS LAST;
ALTER TABLE nessie.arbitrum.l0_dex_pools WRITE ORDERED BY pool_address ASC NULLS LAST;
ALTER TABLE nessie.arbitrum.l1_wallet_ledger WRITE ORDERED BY wallet_group ASC NULLS LAST, wallet_address ASC NULLS LAST, token_address ASC NULLS LAST, block_timestamp ASC NULLS LAST;
ALTER TABLE nessie.arbitrum.l1_token_metadata WRITE ORDERED BY token_group ASC NULLS LAST, token_address ASC NULLS LAST;
ALTER TABLE nessie.arbitrum.l1_token_categories WRITE ORDERED BY token_group ASC NULLS LAST, token_address ASC NULLS LAST, category ASC NULLS LAST;
ALTER TABLE nessie.arbitrum.l1_token_implementations WRITE ORDERED BY token_group ASC NULLS LAST, token_address ASC NULLS LAST;
ALTER TABLE nessie.arbitrum.l1_token_links WRITE ORDERED BY token_group ASC NULLS LAST, token_address ASC NULLS LAST;
ALTER TABLE nessie.arbitrum.l2_token_supply WRITE ORDERED BY token_address ASC NULLS LAST, block_date ASC NULLS LAST;
ALTER TABLE nessie.arbitrum.l2_dex_price_weth_in_stable_swaps WRITE ORDERED BY block_timestamp ASC NULLS LAST;
ALTER TABLE nessie.arbitrum.l2_dex_price_token_in_weth_swaps WRITE ORDERED BY token_address ASC NULLS LAST, block_timestamp ASC NULLS LAST;
ALTER TABLE nessie.arbitrum.l3_weth_price_usd_1m WRITE ORDERED BY bucket_minute ASC NULLS LAST;
ALTER TABLE nessie.arbitrum.l3_token_price_usd WRITE ORDERED BY token_address ASC NULLS LAST, bucket_minute ASC NULLS LAST;
ALTER TABLE nessie.arbitrum.l3_token_price_usd_1m WRITE ORDERED BY token_address ASC NULLS LAST, bucket_minute ASC NULLS LAST;
ALTER TABLE nessie.arbitrum.l3_dex_pair_volume_1d WRITE ORDERED BY token0_address ASC NULLS LAST, token1_address ASC NULLS LAST;
ALTER TABLE nessie.arbitrum.l3_wallet_statement_watchlist WRITE ORDERED BY wallet_group ASC NULLS LAST, wallet_address ASC NULLS LAST, token_address ASC NULLS LAST, block_timestamp ASC NULLS LAST;
ALTER TABLE nessie.arbitrum.l3_wallet_statement_watchlist_filtered_tokens WRITE ORDERED BY wallet_group ASC NULLS LAST, wallet_address ASC NULLS LAST, token_address ASC NULLS LAST, block_timestamp ASC NULLS LAST;
ALTER TABLE nessie.arbitrum.l3_wallet_statement_usd_watchlist_1m WRITE ORDERED BY wallet_group ASC NULLS LAST, wallet_address ASC NULLS LAST, token_address ASC NULLS LAST, block_timestamp ASC NULLS LAST;
ALTER TABLE nessie.arbitrum.token_whitelist WRITE ORDERED BY token_group ASC NULLS LAST, token_address ASC NULLS LAST;
ALTER TABLE nessie.arbitrum.token_quality WRITE ORDERED BY token_address ASC NULLS LAST;
ALTER TABLE nessie.arbitrum.wallet_watchlist WRITE ORDERED BY wallet_group ASC NULLS LAST, wallet_address ASC NULLS LAST;
ALTER TABLE nessie.arbitrum.l4_wallet_balance_per_tx WRITE ORDERED BY wallet_group ASC NULLS LAST, wallet_address ASC NULLS LAST, token_address ASC NULLS LAST, block_timestamp ASC NULLS LAST;
ALTER TABLE nessie.arbitrum.l4_wallet_balance_state WRITE ORDERED BY wallet_address ASC NULLS LAST, token_address ASC NULLS LAST;
ALTER TABLE nessie.arbitrum.l4_wallet_balance_source_stats WRITE ORDERED BY wallet_address ASC NULLS LAST, token_address ASC NULLS LAST;
ALTER TABLE nessie.arbitrum.l4_wallet_balance_1d_dense WRITE ORDERED BY wallet_group ASC NULLS LAST, wallet_address ASC NULLS LAST, snapshot_date ASC NULLS LAST, token_address ASC NULLS LAST;
ALTER TABLE nessie.arbitrum.l4_wallet_pnl_realized_per_sell_tx WRITE ORDERED BY wallet_group ASC NULLS LAST, wallet_address ASC NULLS LAST, token_address ASC NULLS LAST, block_timestamp ASC NULLS LAST;
ALTER TABLE nessie.arbitrum.l4_wallet_pnl_fifo_open_lots WRITE ORDERED BY wallet_group ASC NULLS LAST, wallet_address ASC NULLS LAST, token_address ASC NULLS LAST, lot_seq ASC NULLS LAST;
ALTER TABLE nessie.arbitrum.l4_wallet_pnl_fifo_checkpoint WRITE ORDERED BY wallet_group ASC NULLS LAST, wallet_address ASC NULLS LAST, token_address ASC NULLS LAST;
ALTER TABLE nessie.arbitrum.l4_wallet_pnl_state_fifo WRITE ORDERED BY wallet_group ASC NULLS LAST, wallet_address ASC NULLS LAST, token_address ASC NULLS LAST;
ALTER TABLE nessie.arbitrum.l4_wallet_pnl_realized_1d WRITE ORDERED BY wallet_group ASC NULLS LAST, wallet_address ASC NULLS LAST, snapshot_date ASC NULLS LAST;
ALTER TABLE nessie.arbitrum.l4_wallet_pnl_fifo_source_stats WRITE ORDERED BY wallet_group ASC NULLS LAST, wallet_address ASC NULLS LAST, token_address ASC NULLS LAST;
ALTER TABLE nessie.arbitrum.l4_wallet_networth_1d WRITE ORDERED BY wallet_address ASC NULLS LAST, snapshot_date ASC NULLS LAST;
ALTER TABLE nessie.arbitrum.l4_wallet_networth_1d_dense WRITE ORDERED BY wallet_group ASC NULLS LAST, wallet_address ASC NULLS LAST, snapshot_date ASC NULLS LAST;
ALTER TABLE nessie.arbitrum.l4_wallet_metadata WRITE ORDERED BY wallet_address ASC NULLS LAST;

-- ============================================================================
-- ALL_CHAIN
-- ============================================================================

CREATE TABLE IF NOT EXISTS nessie.all_chain.wallet_watchlist (
    wallet_address      STRING,
    source_chain        STRING,
    whitelist_reason    STRING,
    wallet_group        INT,
    is_active           BOOLEAN,
    created_at          TIMESTAMP,
    updated_at          TIMESTAMP
) USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

CREATE TABLE IF NOT EXISTS nessie.all_chain.binance_market_info (
    symbol          STRING,
    base_asset      STRING,
    quote_asset     STRING,
    listing_date    TIMESTAMP
) USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

CREATE TABLE IF NOT EXISTS nessie.all_chain.wallet_analytics (
    as_of_date              DATE,
    wallet_address          STRING,
    chains                  ARRAY<STRING>,
    net_worth               DOUBLE,
    wallet_age_days         INT,
    realized_pnl_24h        DOUBLE,
    realized_pnl_7d         DOUBLE,
    realized_pnl_30d        DOUBLE,
    unrealized_pnl          DOUBLE,
    net_flow_24h            DOUBLE,
    net_flow_7d             DOUBLE,
    net_flow_30d            DOUBLE,
    net_flow_usd_pct_24h    DOUBLE,
    net_flow_usd_pct_7d     DOUBLE,
    net_flow_usd_pct_30d    DOUBLE,
    stable_coin_pct         DOUBLE,
    alpha_score_adj         DOUBLE,
    sharpe_ratio_24h        DOUBLE,
    sharpe_ratio_7d         DOUBLE,
    sharpe_ratio_30d        DOUBLE,
    top_tokens_display      STRING,
    daily_net_worth_history ARRAY<DOUBLE>,
    created_at              TIMESTAMP,
    updated_at              TIMESTAMP
) USING iceberg
PARTITIONED BY (as_of_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

CREATE TABLE IF NOT EXISTS nessie.all_chain.wallet_insider_token (
    as_of_date              DATE,
    chain_id                STRING,
    wallet_address          STRING,
    token_address           STRING,
    listing_date            DATE,
    no_txn                  BIGINT,
    sell_tx_count           BIGINT,
    amount_token_sum        DOUBLE,
    avg_buy_price_usd       DOUBLE,
    avg_sell_price_usd      DOUBLE,
    first_buy_timestamp     TIMESTAMP,
    last_buy_timestamp      TIMESTAMP,
    first_sell_timestamp    TIMESTAMP,
    last_sell_timestamp     TIMESTAMP,
    realized_pnl            DOUBLE,
    unrealized_pnl          DOUBLE,
    total_pnl               DOUBLE,
    rank_by_realized_pnl    BIGINT,
    insider_reason          STRING
) USING iceberg
PARTITIONED BY (as_of_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

CREATE TABLE IF NOT EXISTS nessie.all_chain.wallet_insider_analytics (
    as_of_date              DATE,
    wallet_address          STRING,
    chains                  ARRAY<STRING>,
    net_worth               DOUBLE,
    wallet_age_days         INT,
    realized_pnl_24h        DOUBLE,
    realized_pnl_7d         DOUBLE,
    realized_pnl_30d        DOUBLE,
    unrealized_pnl          DOUBLE,
    net_flow_24h            DOUBLE,
    net_flow_7d             DOUBLE,
    net_flow_30d            DOUBLE,
    net_flow_usd_pct_24h    DOUBLE,
    net_flow_usd_pct_7d     DOUBLE,
    net_flow_usd_pct_30d    DOUBLE,
    stable_coin_pct         DOUBLE,
    alpha_score_adj         DOUBLE,
    sharpe_ratio_24h        DOUBLE,
    sharpe_ratio_7d         DOUBLE,
    sharpe_ratio_30d        DOUBLE,
    top_tokens_display      STRING,
    daily_net_worth_history ARRAY<DOUBLE>,
    created_at              TIMESTAMP,
    updated_at              TIMESTAMP
) USING iceberg
PARTITIONED BY (as_of_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
);

ALTER TABLE nessie.all_chain.wallet_watchlist WRITE ORDERED BY wallet_group ASC NULLS LAST, wallet_address ASC NULLS LAST;
ALTER TABLE nessie.all_chain.binance_market_info WRITE ORDERED BY base_asset ASC NULLS LAST, symbol ASC NULLS LAST;
ALTER TABLE nessie.all_chain.wallet_analytics WRITE ORDERED BY wallet_address ASC NULLS LAST, as_of_date ASC NULLS LAST;
ALTER TABLE nessie.all_chain.wallet_insider_token WRITE ORDERED BY chain_id ASC NULLS LAST, token_address ASC NULLS LAST, wallet_address ASC NULLS LAST;
ALTER TABLE nessie.all_chain.wallet_insider_analytics WRITE ORDERED BY wallet_address ASC NULLS LAST, as_of_date ASC NULLS LAST;
