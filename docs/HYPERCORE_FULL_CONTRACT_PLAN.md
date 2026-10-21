# HyperCore: filling all 34 contract tables (plan, 2026-10-20)

> Status 2026-10-21: owner approved 1A / 3A / 4A / 5A / 6A / 7A. **Phase A is
> implemented in v1.9.0** (`blueblocx_hyperliquid/rollups/`, 13 tables + the
> `l4x_spot_balance_snapshot` anchor; see docs/DEPLOY.md release notes and
> docs/ATLAS_WALLET_TABLE_FORMULAS.md). Phases B and C are still plans.
> Companion: `docs/ETL_HANDOFF_HYPERCORE.md` (what exists),
> `dagster_blueblocx/NEW_CHAIN_SOURCE_CONTRACT_TABLES.md` (the per-table contract).
>
> One finding during phase A, not in the original plan: a tracked wallet's ledger
> in the lake starts at its first pull (and the Info API caps fills at 10k), so a
> running sum of movements is a balance "since go-live", not the balance. The
> extractor now stages the exchange's own spot balances every run
> (`l4x_spot_balance_snapshot`) and the rollups pin the running sum to that
> balance at the snapshot instant; the pre-history holding this reveals becomes
> the FIFO engine's opening lot, priced at the first observed instant.

## 1. Why only 7 of 34 tables are filled today (root cause)

The ETL contract has 34 labels per chain. On ethereum / arbitrum / optimism /
avalanche the 27 labels that hypercore does not fill are **not read from the chain
itself**: they are produced by the atlas lake's L2/L3/L4 layers (Rust rollups and
Trino views such as `vw_token_stats`, `l4_wallet_balance_1d_dense`,
`l4_wallet_pnl_state_fifo`, `vw_market_fear_index`, `l3_dex_pair_volume_1d`)
computed on top of each chain's `l1_wallet_ledger`, token metadata and candles.

The HyperCore pipeline (`blueblocx-hyperliquid`, v1.8.1) implements only the
layers the handoff asked for: L1 ledger, token metadata, the two consumer gates,
`l4_wallet_metadata`, native 1h/1d candles and a perp position snapshot. The
2026-10-14 decision (handoff section 6.2) was explicit: *"Every other label has no
HyperCore source: leave it out of the hypercore bundle"*. The Dagster package
built this week honours that decision, so `hypercore_backfill_duckdb_to_neon_job`
loads exactly the 7 labels that have a source.

Nothing is lost or misrouted; the other 27 tables were never produced for this
chain. Filling them means **building the missing L2/L3/L4 layer for HyperCore**,
then adding 27 pass-through specs on the Dagster side.

## 2. What HyperCore has to build them from

| raw material (already in the lake) | rows | used for |
|---|---|---|
| `l1_wallet_ledger` (fills, PnL, fees, funding, deposits, withdrawals, transfers; per token, per wallet, immutable, `updated_at` stamp) | 4.3M, +~200k/h | every wallet-level table, token activity/netflow, exchange flow |
| `l1_token_metadata` (+ `is_stablecoin`, decimals) | 35 | FK parent, stable pricing, categories |
| `l3_token_price_usd_1h` / `_1d` (native closed candles, OHLCV + `num_trades`) | 28k / 1.1k | prices, volumes, volatility, signals, DEX-pair volume |
| `l4x_perp_position_snapshot` (positions, unrealized PnL, account value, hourly) | 5.5k | networth, unrealized PnL |
| Info API `spotMeta` / `tokenDetails` (total & circulating supply, markPx), `spotMetaAndAssetCtxs` (24h volume per market) | live | market cap, token_summary, dex_pair_volume |

Two semantic caveats the consumers must accept (handoff section 7):

1. **Wallet-derived aggregates cover the tracked wallet set only** (~150 whales
   and smart-money wallets), not the whole chain. `token_netflow_24h`,
   `token_active_address`, `wallet_whale_moves_24h`, `exchange_flow`,
   `token_activity_trending.tx_count_*` describe those wallets. Candle-derived
   numbers (prices, volumes, market cap) are market-wide.
2. **HyperCore has perps.** Perp activity is USDC cashflow in the ledger; the
   spot-oriented tables (FIFO, balances) treat it as USDC in/out, and
   `unrealized_pnl` comes from the perp snapshot, which no EVM chain has.

## 3. Table-by-table feasibility

Legend: **D** = derivable from the lake as it is; **D+API** = needs one Info API
call per run; **F** = formula lives in the atlas Rust/Trino code and must be
obtained (or approximated and labelled as such).

### Group A: wallet-level (12) — from the ledger + prices + snapshot

| # | label | PK | source for hypercore | note |
|---|---|---|---|---|
| 33 | `wallet_balance_per_tx` | wallet, token, tx_hash, log_index | running sum of `amount_signed` per (wallet, token) over the ledger, `balance_amount` after each row | **D**. 1:1 with ledger rows (4.3M); 8-day window at load time |
| 32 | `wallet_balance_1d_dense` | wallet, token, snapshot_date | daily closing/opening balance, in/out/net per UTC day, dense (carry forward) | **D** |
| 28 | `wallet_networth_1d_dense` | wallet, snapshot_date | Σ spot balance × 1d close + perp account value (snapshot) + staked + vault; in/out/net_flow from `deposit`/`withdraw`/`transfer`; `priced_token_count` / `missing_price_token_count` | **D** (reuses `analytics.compute_analytics` logic per day) |
| 30 | `wallet_pnl_state_fifo` | wallet, token | open lots (qty, cost) after FIFO over `spot_fill`/`transfer` legs | **D** (needs the FIFO engine — new module) |
| 29 | `wallet_pnl_realized_1d` | wallet, snapshot_date | FIFO realized on sells + Σ `perp_realized_pnl`+`funding`−`fee`+`interest` per day; `proceeds_usd`, `cost_usd`, `num_sells` | **D** |
| 24 | `wallet_token_stats` | wallet, token | position qty/cost/avg, price, value, realized/unrealized/total PnL, return %, last tx | **D** (from FIFO state + price) |
| 26 | `wallet_trade_summary` | wallet, token | buy/sell qty & USD, avg prices, net profit, gain %, last_activity_date | **D** |
| 27 | `wallet_whale_moves_24h` | wallet, token, direction | 24h volume per direction, `max_block_timestamp`, supply/market cap/benchmark columns (`brenchmark_*` typo kept) | **D+API** (supply from `tokenDetails`); benchmark formula **F** |
| 31 | `wallet_overview_pnl_percentile_1d` | wallet, computed_date | pnl 1d/7d/30d (realized + Δunrealized) and percentile rank across tracked wallets | **D**; percentile over ~150 wallets, not the whole chain |
| 23 | `token_wallet_top_sharp_ratio` | token, wallet, snapshot_date | per wallet/token daily Sharpe from daily PnL series | **F** (exact window/definition) else approximation |
| 34 | `wallet_performance_30d` | wallet, as_of_date | 30d return, annualized, volatility, Sharpe, correlation/alpha vs benchmark, `outperform_market_30d` | **D**, chain-scoped variant (contract option (a)); benchmark = ETH via UETH/ETH candle, or HYPE (decision 6.3) |
| 11 | `token_netflow_24h` | token | in/out USD and counts over the last 24h across tracked wallets, latest VWAP | **D** |

### Group B: token / market level (10) — from candles (+ one API call)

| # | label | source for hypercore | note |
|---|---|---|---|
| 8 | `token_stats` | 1d candles: current, ATH/ATL/avg, 24h %; `eth_relative_strength` vs ETH candle | **D** (`performance_status` vocabulary **F**) |
| 9 | `token_new_rank` | `first_seen` → age, 24h tx count (tracked), price Δ, `ranking_score`/`rank_new_token` | **F** (score formula) |
| 10 | `token_price_gainers` | price Δ per timeframe (1h/24h/7d) from candles, rank | **D** (timeframe labels **F**) |
| 12 | `token_beta_volatility_sharp` | beta vs market (HYPE or cap-weighted), annualised vol, Sharpe from 1d candles | **D**, `desc_beta` buckets **F** |
| 13 | `market_volume_24h` | Σ `volume_usd` / `volume_token` per hour over all tokens with candles | **D** |
| 14 | `market_fear_index` | momentum/breadth/volatility/volume scores → fear index, profile | **F** (composite weights) |
| 15 | `market_cap_latest` | daily: `total_supply` × 1d close, `is_stablecoin` | **D+API** (`tokenDetails.totalSupply`) |
| 19 | `token_active_address` | distinct tracked wallets with a ledger row per token (24h), `snapshot_date` stamped by the ETL | **D** |
| 20 | `token_activity_trending` | tx counts today/yesterday (tracked), price Δ, volume Δ, ranks | **D**, rank formula **F** |
| 21 | `token_summary` | price, 24h %, market cap, 24h volume, active addresses, four ranks | **D+API**; ranks depend on 9/10/20 |
| 22 | `token_market_signals` | volume spike ratios/signals, close, resistance, breakout, EMA9/EMA21/signal per 1h bucket (+ `__global__` rows) | **F** (spike thresholds, resistance window) else documented defaults |

### Group C: structural (3)

| # | label | source for hypercore | note |
|---|---|---|---|
| 2 | `token_categories` | small dimension: `stablecoin` / `native` (HYPE) / `spot` / `hip3-collateral` from metadata + `tokenDetails` | **D** (taxonomy decision 6.4) |
| 17 | `dex_pair_volume_1d` | HyperCore spot **markets are pairs**: `pair_address = hlcore:pair:<spot index>`, `pair_symbol = PURR/USDC`, token0/token1, daily volume & `num_trades` from the pair's candle (`swap_count_total`), `pool_count = 1` | **D** (candles per *market* exist: the price step fetches the USDC market of each token) |
| 16 | `exchange_flow` | daily bridge `deposit` / `withdraw` USD across tracked wallets, `net_flow_usd` | **D** (tracked scope) |
| 18 | `token_quality` | trades-in-window / max price jump vs thresholds → score, pass/fail, per `quality_version` | **D** with `quality_version = 'hc-v1'` and the thresholds copied from the atlas definition (**F** to match) |

Every table is buildable. The open risk WAS **formula parity** for the 9 labels
marked **F**. RESOLVED 2026-10-21: the definitions live on Olympus in
`~/blueblocx/atlas-project` (`trino/views/<chain>/*.sql` for the 38 views,
`services/rust/{market-signals,token-wallet-sharpe,control-jobs,fifo-pnl,
wallet-balance,wallet-balance-1d,wallet-networth,wallet-trades-usd}/src` for the
L4 tables). A copy is kept locally for porting. What they need, and what HyperCore
must add to match them exactly:

| definition | inputs on EVM | HyperCore gap |
|---|---|---|
| `vw_market_fear_index` (5 factors, 730-day percentile calibration, needs ≥ 60 days) | 1d prices, market cap per day, whitelist, stables | 1d candle history must go back ≥ 2 years (`HL_PRICE_HISTORY_DAYS` 32 → 820; the Info API serves ~5000 1d candles); `market_cap_1d` first |
| `vw_token_new_rank` (0.5 return + 0.3 newness + 0.2 ln(trades), 30-day window) | 1m candles | use `first_seen` + 1h `num_trades`; or 1m candles (below) |
| `vw_token_stats`, `vw_token_price_gainers`, `vw_token_activity_trending`, `vw_token_summary` ("as-of now / now−24h" on 1-minute buckets, tip-anchored) | 1m candles | **fetch 1m candles for the last 3 days** per token each run (Info API `candleSnapshot` 1m serves ~5000 candles ≈ 3.5 days): 35 tokens × 1 call, exact parity |
| `vw_token_beta_volatility_sharp` (30 days, log returns, beta vs WETH) | 1d prices | benchmark token decision (6.3) |
| `market_signals` Rust (EMA9/EMA21, 20-bucket spike ratio ≥ 2×, 20-bucket resistance, breakout; `__global__` rows) | 1m candles | same 1m feed |
| `token_quality` Rust (`trades >= 10`, `max jump <= 0.80`, lookback 24 h, version `v1_trades_jump`) | 1m candles | same 1m feed |
| `token-wallet-sharpe` Rust (30-day window over `balance_per_tx` + prices) | balance_per_tx, 1d prices | after Group A |
| `vw_wallet_overview_pnl_percentile_1d` (30-day windows, PERCENT_RANK per day) | `l4_wallet_pnl_realized_1d` | after Group A |
| `dex_pair_volume` Rust (per pool-pair-day from L2 swap tables, direction split) | DEX swaps | HyperCore spot fills by market (tracked wallets) for the direction split, or candle volume (market-wide, no split) — decision 6.7 |

So every F becomes D with two additions to the price step: a **1-minute candle
feed for the last 3 days** and **2 years of 1d history**.

## 4. Architecture (hyperliquid side, per the owner: build here, not in Dagster)

```
hourly run (existing)                     NEW: rollup step, same run, after the lift
 price step -> wallets -> lift            DuckDB over the just-committed Iceberg tables
                                          (same read path the ETL uses) -> 27 result
                                          tables -> iceberg_load (replace-by-key for
                                          state/dimension tables, insert-if-absent +
                                          restate-window for daily facts) -> control
                                          watermarks -> run summary `rollups` block
```

- **Engine**: DuckDB SQL inside `blueblocx_hyperliquid/rollups/` (one module per
  group, one SQL per table, parameterised by `as_of`), reading the lake through
  the catalog like `query.py` does. 4.3M ledger rows aggregate in seconds; the
  FIFO engine is Python over per-wallet partitions (`wallet_pnl_state_fifo` is
  incremental: state per wallet/token persisted in Iceberg, replayed from the
  last processed `(tx_hash, event_index)`).
- **Schemas**: 27 new tables in `docs/PROPOSED_SCHEMA.sql` with the contract's
  column names and types (`updated_at` = write stamp of the run that (re)wrote the
  row, so the ETL's Tier-A cursors work unchanged; Tier-B tables carry their
  business date). Created by a `migrate.py` entry (v1.9.0), guarded by
  `schema_drift` like every other table.
- **Cadence**: hourly for current-state and 24h tables (Tier A/C labels); the
  `*_1d` tables are (re)computed for `today` and `yesterday` every hour (so a
  late lift corrects them), older days frozen. Bounded by a time budget
  (`HL_ROLLUP_TIME_BUDGET_MIN`, default 8) so the 50-minute unit is never at risk;
  a skipped table is reported in `_run_summary.json.rollups` and on the web
  monitor, never silently.
- **Scope flag**: every tracked-wallet-scoped table gets a row in a new
  `control.table_notes` (or a column comment) saying `scope=tracked_wallets`.
- **Memory**: the OOM of 2026-10-20 (8G cap on the Python 3.14 venv) is raised to
  32G; DuckDB is configured with `memory_limit=8GB`, `threads=4`.

## 5. Dagster side (later, when naxlab is free; small)

- `shared/hypercore/lake_sql.py`: add the 27 specs = ethereum `columns`/`pk`/
  `where_*` byte-for-byte, `bq_from = {{hl:<new table>}}`, pass-through
  `select_exprs` (renames the contract already uses: `last_event_index AS
  last_log_index`, `balance_amount AS balance_qty`, `event_index AS log_index`,
  `create_at AS created_at`, `current_date AS snapshot_date` → DuckDB
  `current_date`). `LAKE_TABLES` += 27. Load order = ethereum's 34.
- `ingest_tiers.py` is label-keyed and already correct for all 34 labels; no
  engine change. `truncate_where_by_table` keeps every table chain-scoped.
- The static check already asserts contract byte-equality; the validate op
  already checks FK closure for `wallet_transactions` — extend to the 13 tables
  with a token FK.
- Rehearse on `neon_staging` (backfill + ingest), then re-run
  `hypercore_backfill_duckdb_to_neon_job` on prod (chain-scoped DELETE, other
  chains untouched).

### What the platform code actually reads (files referencing each table, 2026-10-21,
`blueblocx-platform-backend` + `core-api` + `blueblocx-frontend-platform`)

| table | files | | table | files |
|---|---|---|---|---|
| token_categories | 19 | | market_volume_24h, market_fear_index | 7 |
| wallet_networth_1d_dense | 16 | | token_price_gainers, token_netflow_24h | 6 |
| token_summary | 11 | | wallet_whale_moves_24h, exchange_flow | 5 |
| wallet_pnl_realized_1d, market_cap_latest, dex_pair_volume_1d | 9 | | wallet_performance_30d, wallet_balance_per_tx, token_stats, token_new_rank, token_beta_volatility_sharp | 4 |
| wallet_token_stats, token_market_signals | 8 | | wallet_overview_pnl_percentile_1d, token_activity_trending | 3 |
| wallet_trade_summary, wallet_balance_1d_dense, token_wallet_top_sharp_ratio, token_quality | 7 | | wallet_pnl_state_fifo 1, token_active_address 0 | |

`token_categories` (19) and `wallet_networth_1d_dense` (16) are the most-read
missing tables; `token_active_address` is read by nothing.

## 6. Decisions needed before coding

1. **Priority / phases.** Proposed: Phase 1 = Group A (what the wallet pages and
   `view_api_smart_whale` consume: PnL percentile, token stats, balances,
   networth) **plus `token_categories` pulled forward** (most-read table, a small
   dimension); Phase 2 = Group B; Phase 3 = Group C + the three Sharpe/quality
   tables.
2. **Formula parity.** RESOLVED: definitions found on Olympus (section 3). The
   port will keep the atlas SQL/Rust as the reference and add a parity test per
   table (same inputs → same outputs) in `tests/`.
7. **`dex_pair_volume_1d` source**: tracked-wallet spot fills (exact direction
   split, partial volume) or market candles (full volume, no direction split,
   `swap_count_total = num_trades`)?
3. **Benchmark for `wallet_performance_30d` / `eth_relative_strength` /
   `correlation_with_eth_30d`**: ETH (via the UETH or ETH-perp candle, matches the
   column names) or HYPE (the chain's native asset)?
4. **`token_categories` taxonomy** for HyperCore tokens.
5. **Scope disclosure**: accept that wallet-derived aggregates are tracked-wallet
   scoped (documented), or widen the tracked set first.
6. **`wallet_balance_per_tx` volume**: 4.3M rows now, +~5M/day at the current fill
   rate; the contract loads only an 8-day window, fine for Neon, but the Iceberg
   table doubles the lake size. OK?

## 7. Effort

| phase | hyperliquid side | Dagster side | rehearsal |
|---|---|---|---|
| A (12 wallet tables, FIFO engine) | 4 days | 0.5 day | 0.5 day |
| B (10 token/market tables) | 3 days (+ waiting on formulas) | 0.5 day | 0.5 day |
| C (4 tables) | 1 day | 0.5 day | 0.5 day |

About two and a half working weeks elapsed, each phase shippable on its own as a
release (v1.9.0 / v1.10.0 / v1.11.0) through the normal bootstrap flow.
