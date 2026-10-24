# HyperCore lake: hand-off to the dagster_blueblocx ETL

> **Who this is for.** The ETL team adding `hypercore` (Hyperliquid HyperCore) as a
> source of `dagster_blueblocx`. It answers the questions your source contract
> (`NEW_CHAIN_SOURCE_CONTRACT*.md`) asks a new chain, and lists what has to change
> on the ETL side because this chain is read **with DuckDB from a pyiceberg SQL
> catalog**, not through Trino (owner decision, 2026-10-14).
>
> **Status.** Every producer-side change this document relies on is implemented in
> `blueblocx-hyperliquid` v1.4.0 … v1.10.0 (section 8 says what each release adds;
> v1.9.0 adds the 13 wallet-level L4 tables, v1.10.0 the 14 token / market tables:
> **all 34 labels of the contract have a HyperCore source from v1.10.0 on**).
> Which release the host actually runs is shown by the web monitor (`/api/status`,
> field `version`) and by `hypercore-status --json`; sections marked *(vX.Y.Z)*
> describe behaviour that exists from that release on. Use the onboarding checklist
> at the end of section 8 before the first load.

---

## 1. Decisions in one paragraph

HyperCore runs on its own host (`server4090-2`) as a standalone Iceberg lake: a
pyiceberg **SQL catalog in PostgreSQL** (`hl_catalog`) and **plain Parquet/Avro files
on a NAS** (`/mnt/blueblocx/hypercore/warehouse`). No Nessie, no MinIO, no Trino.
The ETL gets a **read-only catalog role** (`hl_ro`) and a **read-only NFS mount at
the identical path**, reads each table's current `metadata_location` from the
catalog and `iceberg_scan()`s it with DuckDB. Every object the ETL needs that is
not a base table (the `vw_*` views of the contract) is **SQL inside the ETL's
hypercore spec**, not a lake-side view. Labels for which HyperCore has no data are
**excluded from the hypercore spec** (section 6), never seeded with stub rows.

## 2. Access

| item | value |
|---|---|
| chain identifier | `hypercore` (lowercase; also the Iceberg namespace, the `chains` array element and the `chain_id` literal to use) |
| catalog | PostgreSQL `hl_catalog` on `server4090-2`, role `hl_ro` (SELECT only, `default_transaction_read_only`), connection string in `/etc/hypercore/reader.env` on that host — handed over out of band, never pasted |
| pointer table | `iceberg_tables` with `catalog_name = 'hl'`, `table_namespace = 'hypercore'` (and `'control'` for the watermark table) |
| warehouse | NFS export mounted **read-only at `/mnt/blueblocx`** on the ETL host (`ro,vers=4.1,hard,noatime,_netdev,lookupcache=positive`); metadata and manifests hold absolute `file:///mnt/blueblocx/hypercore/warehouse/…` locations |
| acceptance test | `python -m blueblocx_hyperliquid.duckdb_smoke` from a clone of this repo with `HL_ENV_FILE=reader.env`, in a venv with only `duckdb`, `sqlalchemy` and `psycopg2-binary` (docs/DEPLOY.md "Serving the lake to DuckDB readers"); it prints the `iceberg_scan` form that works on your host: raw `file:///…` location, stripped path, or table root with the pointer's version pinned (`allow_moved_paths = true`) |

The read is two-step because `iceberg_scan()` needs a constant path:

```sql
-- once per ETL run; never cache pointers across runs (metadata files older than
-- write.metadata.previous-versions-max commits are deleted)
SELECT table_name, metadata_location FROM iceberg_tables
WHERE catalog_name = 'hl' AND table_namespace = 'hypercore';

SET TimeZone = 'UTC';            -- timestamptz values then render/export as +00:00
-- form A: the location as the catalog holds it
SELECT ... FROM iceberg_scan('<metadata_location>') WHERE ...;
-- form B (scheme-independent): <metadata_location> = <root>/metadata/<version>.metadata.json;
-- reads exactly that committed file, resolves data/manifest paths relative to <root>
SELECT ... FROM iceberg_scan('<root>', version = '<version>', allow_moved_paths = true) WHERE ...;
```

Verified 2026-10-15 with DuckDB 1.5.6 (iceberg extension 890b78a9c) on a development
lake: the pointer query, form B and `SET TimeZone` behave as described; form A works
wherever DuckDB resolves the stored `file:///…` locations (it does for the standard
three-slash form production writes; the smoke on your host is the final word). Prefer
form B if both work: it never depends on path resolution inside manifests.

Concurrency: our lift advances a table's pointer at most **once per hourly run**
(delete + append of a replaced table are one commit since v1.4.0), the nightly
compaction (v1.8.0, ~00:05 UTC) once more per table, files are immutable and
uniquely named, and nightly snapshot expiry (7 days) deletes no data files — a scan
that started before a commit keeps reading its snapshot. Fetch all
pointers in one statement at the start of your run; use `control.table_watermarks`
(section 5) as the batch-complete marker if you need one.

## 3. What the lake holds

| Iceberg table (`hypercore.*`) | grain / key | contract label | notes |
|---|---|---|---|
| `l1_token_metadata` | `token_address` | `token_metadata` | parent of every `token_address`; `name == symbol` (HL tokens have no display name), both capped 100/200 code points *(v1.4.0)* |
| `token_whitelist` | `token_address` | `token_whitelist` | `is_active` always true today (never de-listed, see section 7) |
| `wallet_watchlist` | `wallet_address` | `wallet_watchlist` | one row per tracked wallet; same de-listing caveat |
| `l4_wallet_metadata` | `wallet_address` | `wallet_metadata` | `first_tx` / `latest_tx` are **TIMESTAMPTZ instants** of the wallet's first/latest ledger event *(v1.5.0; tx-hash strings before)*; from v1.6.0 the row is a **running aggregate over the wallet's accumulated history** since go-live (funding excluded: `count_tx` = non-funding ledger rows, `active_days` = distinct UTC days with activity, `last_active` = whole UTC days since the latest event, `active_on_avg` NULL); a wallet with no non-funding activity has no row |
| `l1_wallet_ledger` | `(tx_hash, event_index, direction)` | source of `wallet_transactions` (your SQL, section 6) | immutable facts, insert-if-absent; `block_number`/`tx_index` NULL (no EVM block) |
| `l3_token_price_usd_1h` | `(token_address, bucket_minute)` | `token_price_usd_1h` | one row per **closed** native 1h candle (`bucket_minute` = the candle open) for every token the ledger referenced that has a USDC spot market, plus stables pinned at 1.0 on the same grid; 32 days back on a token's first visit; no perp rows *(v1.7.0)* |
| `l3_token_price_usd_1d` | `(token_address, bucket_date)` | `token_price_usd_1d` | same per closed UTC day (`bucket_minute` = 00:00Z of `bucket_date`) *(v1.7.0)* |
| `l3_token_price_usd_1m` | `(token_address, bucket_minute)` | — (input of the token views) | one row per closed native **1-minute** candle of every priceable token, stables pinned at 1.0, from v1.10.0 (the first run after the deploy reaches 3 days back; before that, development-only). The token views below read it the way the EVM views read the atlas 1m table |
| `market_volume_24h` | `hour_ts` | `market_volume_24h` | the last 24 complete hours anchored to the 1h data tip: Σ volume_token / volume_usd over every token's 1h candle (market-wide trade volume; the EVM view sums the chain's transfers) *(v1.10.0)* |
| `market_cap_latest` | `(token_address, as_of_date)` | `market_cap_latest` | closed days: 1d close × total supply (Info tokenDetails) with the atlas admission gates; last 35 days rewritten per run *(v1.10.0)* |
| `market_fear_index` | `as_of_date` | `market_fear_index` | the v3 five-factor index, point-in-time 730-day midrank calibration, whitelisted non-stable tokens cap-weighted; whole series rewritten per run, prints once >= 60 calibrated days exist *(v1.10.0)* |
| `token_stats` | `token_address` | `token_stats` | as-of-now price and 24h change from the 1m candles; **all-time high / low / average from the 1d candles** (820 days), not from the 1m table; strength vs ETH = UETH *(v1.10.0)* |
| `token_price_gainers` / `token_new_rank` | `(token, timeframe, current_ts)` / `(token, current_ts)` | same labels | 1m tip-anchored 24h gainers; new-token rank for tokens **deployed** within 30 days (`token_created_at` from tokenDetails; tokens without a deploy time are not ranked) *(v1.10.0)* |
| `token_beta_volatility_sharp` | `token_address` | same | 30 daily log returns of the whitelisted tokens vs UETH *(v1.10.0)* |
| `token_activity_trending` | `(token_address, activity_date)` | same | last complete day vs the day before, from the 1d candles (= the day's 1m sums) *(v1.10.0)* |
| `token_active_address` | `token_address` | same (`current_date AS snapshot_date` on your side) | distinct **tracked** wallets with a ledger row per token in the last 24h *(v1.10.0)* |
| `token_summary` | `token_address` | same | price, live market cap, yesterday's candle volume, active **tracked** wallets, the four ranks *(v1.10.0)* |
| `token_market_signals` | `(global_flag, token_address, bucket_minute)` | same (`entity_key` derived on your side) | the Rust market-signals port on the 1m candles, last 3 UTC days rewritten per run (partitioned by `bucket_date`); `price_breakout` is the Rust string `breakout` / `normal` *(v1.10.0)* |
| `token_quality` | `(as_of_date, quality_version, token_address)` | same | the Rust token_quality port (`v1_trades_jump`: >= 10 trades, jump <= 0.80), last 3 days rewritten per run *(v1.10.0)* |
| `dex_pair_volume_1d` | `(pair_address, bucket_date)` | same | one spot market = one pair `TOKEN/USDC` = one pool, volume and trade count from the market's 1d candle; **the per-direction columns are 0** (not observable from candles), `pair_address` = `<token_address>/<usdc address>` *(v1.10.0)* |
| `exchange_flow` | `bucket_date` | same | USDC the **tracked** wallets bridged in (`deposit`) / out (`withdraw`) per closed day, last 31 days *(v1.10.0)* |
| `l4x_perp_position_snapshot` | `(as_of_date, wallet_address, coin)` | — (extension) | daily perp positions / unrealized PnL; no `updated_at` (use `created_at`); replaced per `(as_of_date, wallet)` every hour |
| `l4x_spot_balance_snapshot` | `(as_of_date, wallet_address, token_address)` | — (extension) | the exchange's own spot balances per tracked wallet, replaced per `(as_of_date, wallet)` every hour; the anchor the balance rollups are pinned to *(v1.9.0)* |
| `l1_token_categories` | `(token_address, token_group, category)` | `token_categories` | on-chain rules (`source = 'hypercore_rules'`): every token is `spot`, plus `stablecoin`, `native` (HYPE), `wrapped` (U-prefixed bridged assets such as UETH, UBTC), `hip3-collateral` where they apply *(v1.9.0)* |
| `l4_wallet_balance_per_tx` | `(wallet_address, token_address, tx_hash, event_index)` | `wallet_balance_per_tx` | one row per ledger row, `balance_amount` after it; `event_index AS log_index`, `balance_amount AS balance_qty` on your side; partitioned by wallet, **replaced per wallet** when the wallet's ledger changes (so `updated_at` moves for the whole wallet) *(v1.9.0)* |
| `l4_wallet_balance_1d_dense` | `(wallet_address, token_address, snapshot_date)` | `wallet_balance_1d_dense` | dense UTC days, carry-forward; same replace-per-wallet rule *(v1.9.0)* |
| `l4_wallet_networth_1d_dense` | `(wallet_address, snapshot_date)` | `wallet_networth_1d_dense` | Σ spot balance × 1d close + perp account value; `in_usd`/`out_usd`/`net_flow_usd` from deposits, withdrawals and transfers *(v1.9.0)* |
| `l4_wallet_pnl_state_fifo` / `l4_wallet_pnl_realized_1d` | `(wallet, token)` / `(wallet, snapshot_date)` | `wallet_pnl_state_fifo` / `wallet_pnl_realized_1d` | FIFO over spot fills and transfers (atlas `fifo-pnl` port), perp realized PnL, funding, fees and interest as USDC legs; opening lots from the spot-balance anchor priced at the first observed instant; `last_event_index AS last_log_index` *(v1.9.0)* |
| `wallet_token_stats` / `wallet_trade_summary` | `(wallet, token)` | same labels | from the FIFO state + latest price / buy-sell aggregates (`vw_wallet_token_stats`, `wallet-trades-usd` ports) *(v1.9.0)* |
| `wallet_whale_moves_24h` / `token_netflow_24h` | `(wallet, token, direction)` / `(token)` | same labels | trailing 24h at run time, whole table replaced every run; `create_at AS created_at`; **tracked wallets only** *(v1.9.0)* |
| `wallet_overview_pnl_percentile_1d` / `wallet_performance_30d` / `l4_token_wallet_sharpe_1d` | per wallet (+ token) per day | `wallet_overview_pnl_percentile_1d` / `wallet_performance_30d` / `token_wallet_top_sharp_ratio` | ranks across the tracked wallet set; `wallet_performance_30d` is **chain-scoped** (contract option (a)) with ETH via the UETH candle as benchmark; `updated_at AS source_updated_at` for the sharpe label *(v1.9.0)* |
| `control.table_watermarks` | `(namespace, table_name)` | — | one row per table per lift, section 5 |

Every `TIMESTAMP` column of the data tables is Iceberg **timestamptz (UTC,
microseconds)**; `DATE` columns are UTC business days; integers are Iceberg
`int`/`long`; doubles are finite (no NaN/Infinity is produced). All strings are
varchar (no `char(n)`), NUL-free from v1.4.0.

## 4. Identifiers (byte-exact, not hex)

- `token_address`: `hlcore:usdc`, `hlcore:token:<spotMeta token index>`,
  `hlcore:spot:<SYMBOL>` (fallback, symbol-cased). **Never `LOWER()` it.**
- `tx_hash`: wallet-scoped and content-addressed: `hlcore:<wallet>:tid:<tid>` (fills),
  `hlcore:<wallet>:dust:<ms>:<coin>:<sz>`, `hlcore:<wallet>:funding:<COIN>:<ms>`,
  `hlcore:<wallet>:<l1 hash>:<type>`, `hlcore:<wallet>:<type>:<ms>`. Not an on-chain
  hash: an explorer `{tx_hash}` template cannot resolve it (section 9).
- `wallet_address`, `counterparty_address`: lowercase `0x…` (counterparty may be a
  coin or ledger type string for perp/internal rows).
- `direction`: lowercase `in` / `out`. Your destination view tests `'IN'`: project
  `upper(direction)`.
- `event_type`: `spot_fill`, `perp_realized_pnl`, `fee`, `funding`, `deposit`,
  `withdraw`, `transfer`, `staking_reward`, `interest`. `standard` is always `ERC20`.
- Natural key of a ledger row: `(tx_hash, event_index, direction)`, unique chain-wide
  (asserted on every run). Proposed `row_key` for `wallet_transactions`:
  `tx_hash || ':' || event_index || ':' || direction` — confirm the convention you use
  on ethereum and freeze it (it is your upsert conflict target).

## 5. Stamps, cursors and the control table

- **`updated_at` on `l1_wallet_ledger`** is the stamp of the lift that first landed
  the row and never changes (facts are immutable) — correct Tier-A semantics. Rows
  are stamped with the run's **start** instant and committed at the end of the same
  run (≤ 50 min later under the hourly unit), inside your 2-hour re-pull margin.
  All pending rows of a table land in one commit, so an older-stamped row never
  lands after a newer-stamped one.
- **Dimension tables** (`l1_token_metadata`, `token_whitelist`, `wallet_watchlist`,
  `l4_wallet_metadata`): from v1.6.0 a row is re-stamped **only when its content
  changed** (`created_at` = first ingestion, `first_seen` never moves later); a row
  re-emitted with identical values keeps its stamp, so a Tier-A pull sees exactly
  the changed rows. `l4_wallet_metadata.last_active` is a day counter, so every
  wallet's row changes once per UTC day (a real content change). Before v1.6.0
  every row touched by a run was re-stamped with the run's start instant (tables
  are small: ~150 wallets, a few hundred tokens — the contract's tolerated case).
- **`control.table_watermarks`** (namespace `hypercore`, naive UTC `timestamp(6)`
  columns like python-ingestion): one row per table **that received rows in the
  lift**, written in one commit after all data commits of that run, with the same
  `run_id` for every table of the lift, `snapshot_id` = the table's current snapshot
  after the lift, `row_count` = table total from Iceberg metadata,
  `watermark_col` = the write-stamp column (`updated_at`; `created_at` for l4x),
  `watermark_ts` = max write stamp of the batch *(v1.4.0; was business time)*,
  `watermark_date` = max business date of the batch, `status` ∈
  {`completed`, `failed`, `restated`}. `failed` = that table's lift raised (its
  previous watermark is kept); `restated` = an operator re-lifted staged rows
  (`--full` / `--since`): treat as a restatement notice. The vocabulary is a
  constant (`iceberg_load.WM_STATUS_OK`); say so if you prefer `ok`.
- Timing: our timer fires at **:10 UTC** (+ ≤ 2 min jitter), the lift lands between
  ~:15 and ~:58 depending on wallet count, the unit is killed by :02. Your
  `30 * * * *` tick therefore usually sees the previous hour's lift; a tick that
  overlaps our lift sees each table either fully before or fully after its commit.

## 6. What changes on the ETL side

1. **A new source type** reading via DuckDB + the SQL catalog instead of Trino:
   render the Tier-A/B/C predicates in DuckDB dialect (`updated_at >= TIMESTAMPTZ
   '<cursor - 2h>' AND updated_at < TIMESTAMPTZ '<now>'`), fetch pointers once per
   run, derive `<root>` / `<version>` from each pointer for read form B (section 2),
   `SET TimeZone = 'UTC'`.
2. **A hypercore spec with only the labels that have a source**, in this order:
   `token_metadata`, `token_whitelist`, `wallet_watchlist`, `wallet_metadata`,
   `wallet_transactions` (your SQL over `l1_wallet_ledger` joined to
   `l1_token_metadata` for `token_symbol`; `log_index := event_index`;
   `row_key` as in section 4; `transfer_price_usd` / `transfer_value_usd` from an
   ASOF join on `l3_token_price_usd_1h.bucket_minute` once the onboarding gate below
   is green, stables at 1.0 via `is_stablecoin`), `token_price_usd_1h` /
   `token_price_usd_1d` (plain SELECTs on `l3_token_price_usd_1h` / `_1d`: same
   column names as your spec; `num_trades` is INT here, cast to BIGINT if you
   reconcile sums). **From v1.9.0** the 13 wallet-level labels (`token_categories`,
   `wallet_balance_per_tx`, `wallet_balance_1d_dense`, `wallet_networth_1d_dense`,
   `wallet_pnl_state_fifo`, `wallet_pnl_realized_1d`, `wallet_token_stats`,
   `wallet_trade_summary`, `wallet_whale_moves_24h`, `token_netflow_24h`,
   `wallet_overview_pnl_percentile_1d`, `token_wallet_top_sharp_ratio`,
   `wallet_performance_30d`) are plain pass-through SELECTs on the lake tables of
   section 3 with the renames listed there (`event_index AS log_index`,
   `balance_amount AS balance_qty`, `last_event_index AS last_log_index`,
   `create_at AS created_at`, `updated_at AS source_updated_at`); their columns, keys
   and tiers are the ethereum spec's byte for byte. **From v1.10.0** the remaining 14
   labels (`token_stats`, `token_price_gainers`, `token_new_rank`,
   `token_beta_volatility_sharp`, `token_activity_trending`, `token_active_address`,
   `token_summary`, `token_market_signals`, `token_quality`, `market_volume_24h`,
   `market_fear_index`, `market_cap_latest`, `dex_pair_volume_1d`, `exchange_flow`)
   are pass-through SELECTs on the lake tables of section 3 as well, with the two
   expressions the ethereum spec already carries (`current_date AS snapshot_date` for
   `token_active_address`; `entity_key` and the boolean `price_breakout` for
   `token_market_signals` - the lake stores the Rust strings `breakout` / `normal`, so
   test `price_breakout IN ('true', 'breakout')`). The hypercore bundle is therefore
   the full 34-label contract; until a host runs v1.10.0 the 14 lake tables do not
   exist and those labels must stay out (a 0-row stub would fail your full refresh
   after the shared truncate).
3. **`chains` row** for `hypercore` in every destination before the first load:
   `is_evm = false`, `chain_numeric_id = NULL` (HyperCore has none; 999 is HyperEVM,
   a separate future chain), `native_symbol` / `native_decimals` to be decided with
   the owner (HYPE vs USDC), `explorer_tx_template = NULL` (synthetic `tx_hash`),
   `explorer_address_template` = a HyperCore wallet page, `sort_order` next free.
4. Keep the `token_metadata`-first load order and the semi-join of FK-dependent
   feeds on `l1_token_metadata`; Iceberg has no cross-table snapshot, so a pull that
   straddles our lift can see ledger rows newer than the metadata it already read.

## 7. Semantics the ETL must not misread

- The lake covers the **tracked wallet set** (pins + top-100 whales + top-50 smart
  money from the leaderboard, re-planned every hour), not the whole chain. Any
  market-level aggregate derived from it describes those wallets only.
- `wallet_watchlist.is_active` / `token_whitelist.is_active` mean "ever ingested":
  a wallet that leaves the tracked set keeps its row (never deleted, never flipped
  to false). De-listing semantics are an open owner decision.
- `l1_token_metadata.first_seen` = earliest ledger event of the token among tracked
  wallets, stable from v1.6.0 (before that it moved to the last run that saw the
  token; a one-time repair after the v1.6.0 deploy moves those values back and
  re-stamps the affected token rows once — expect one burst of re-stamped
  `token_metadata` rows); `token_created_at` stays NULL; `total_supply*` stay NULL.
- Perp activity is booked as **USDC (or HIP-3 collateral) cashflow rows**
  (`perp_realized_pnl`, `fee`, `funding`); open positions live only in
  `l4x_perp_position_snapshot`. `net_flow` must sum only `deposit`/`withdraw`/
  `transfer`; `realized_pnl` sums `perp_realized_pnl + funding − fee + staking_reward
  + interest` valued in USD (docs/HYPERCORE_LEDGER_SPEC.md §3).

## 8. Release status

| release | what the ETL gets |
|---|---|
| v1.4.0 | single-commit replaces (no half-replaced table), parents-before-facts lift order, NOT-NULL/duplicate guards before any commit, `name`/`symbol` caps, watermark statuses and write-stamp semantics, read-only catalog role + smoke test, this document |
| v1.5.0 | `l4_wallet_metadata.first_tx` / `latest_tx` are **TIMESTAMPTZ instants** (the first/latest ledger event = `first_block_timestamp` / `latest_block_timestamp`; before, tx-hash strings that would fail your TIMESTAMPTZ COPY). Done by an in-place schema evolution + one backfill commit (`migrate --apply`, run by our deploy); the columns keep their names and position, the Iceberg field ids change (DuckDB resolves by current schema; irrelevant to you). Our lift refuses to write a table whose types differ from the DDL, so this cannot regress silently |
| v1.6.0 | `l4_wallet_metadata` is a running aggregate over the wallet's **accumulated history** (count_tx = non-funding ledger rows, active_days = distinct UTC days, last_active = UTC days since the latest event, first/latest instants; funding excluded; `active_on_avg` NULL pending a definition), carried next to our per-wallet watermark and advanced only over complete windows; dimension rows re-stamped **only on content change** (`created_at` = first ingestion); `first_seen` stable, with a one-time repair that re-stamps drifted token rows once |
| v1.7.0 | native candle tables `l3_token_price_usd_1h` / `l3_token_price_usd_1d` (one row per closed candle, stables pinned at 1.0, no perp reference rows), written FIRST in every hourly run under a 3-minute budget (tokens with recent ledger activity every run, the rest rotated most-stale first) and lifted before the wallet phase, so they are present even when a run defers wallets. **Onboarding gate:** `prices.history_pending == 0` in `_run_summary.json` (every priceable token carries its 32-day history; after the deploy this takes a few hours of rotation). `prices_deferred=N` in a summary means the budget was reached that hour - the next run continues |
| v1.8.0 | nightly **compaction** of closed, fragmented partitions (≥ 8 files; a partition is closed 2 UTC days after its date, 1 for `l4x`) into one file each - one commit per table per night just after 00:05 UTC, so your scans of yesterday-and-older partitions open one file instead of 24; `write.metadata.previous-versions-max` raised to 100, so a `metadata_location` you fetched stays readable for well over a day even through commit bursts (fetch it once per run anyway) |
| v1.9.0 | **the wallet-level L4 layer** (section 3, 13 tables + `l4x_spot_balance_snapshot`), recomputed after every lift for the wallets whose ledger changed, with the atlas formulas. Semantics to carry into your consumers: (1) wallet-derived aggregates (`token_netflow_24h`, `wallet_whale_moves_24h`, the ranks and percentiles) cover the **tracked wallet set** (~150 wallets), not the whole chain; (2) running balances are pinned to the exchange's spot balance at the snapshot instant, so a wallet's first `wallet_balance_per_tx` row may already carry a non-zero `balance_qty` and the FIFO state an opening lot priced at the first observed instant; (3) perp cashflows appear as USDC legs; (4) per-wallet tables are **replaced per wallet**, so `updated_at` moves for all of a wallet's rows at once (Tier-A cursors pull the whole wallet, Tier-B windows the trailing dates); (5) `created_at` on `wallet_balance_per_tx` is re-stamped on rewrite. `control.table_watermarks` gains one row per rollup table (`status = 'completed'` after each run that reached it; a table skipped by the time budget keeps its previous row and the run summary says `rollups_skipped=`) |
| v1.10.0 | **the token / market layer** (section 3, 14 tables) and the inputs it needs: `l3_token_price_usd_1m` populated in production (1m candles, 3 days back then the gap), 820 days of 1d candles per token (fetched once), `l1_token_metadata.total_supply` / `token_created_at` / `supply_*` from Info `tokenDetails` (refreshed daily per token; NULL until a token's first refresh). Semantics to carry into your consumers: (1) `token_active_address`, the active-address columns of `token_summary` and `exchange_flow` count the **tracked wallet set**; (2) `token_stats` all-time high / low / average are over the 1d candles (820 days), the as-of prices over the 1m candles; (3) `token_new_rank` ranks tokens by **deploy time**, tokens without one (genesis tokens, USDC) never appear; (3b) Unit-bridged assets (`UETH`, `UBTC`, `USOL`, ...) carry `supply_confidence = 'low'` (their `totalSupply` is the pre-minted pool, not the bridged amount) and are absent from `market_cap_latest`, `market_fear_index` and `token_summary.market_cap_usd`; `HYPE` market cap follows the atlas (total supply x price, not circulating) *(v1.10.2)*; (4) `dex_pair_volume_1d` has no direction split (per-direction columns 0, totals in `volume_usd` / `swap_count_total`), one pool per pair; (5) `market_volume_24h` is market-wide candle volume; (6) `market_fear_index` appears only once 60 calibrated days exist (about two days after the deploy) and the whole series is rewritten each run (point-in-time scores do not change, `updated_at` does); (7) `token_market_signals` and `token_quality` rewrite the last 3 UTC days every hour (Tier-A cursors pull ~170k signal rows per hour, as on ethereum). The first 4-5 runs after the deploy are a candle catch-up (`prices_deferred=N`); the token views fill in as their inputs land |
| v1.10.8 | `l1_token_metadata.total_supply` is the **circulating** supply (Info `tokenDetails.circulatingSupply`; minted total only as fallback), so `market_cap_latest.total_supply` / `total_market_cap` and `token_summary.market_cap_usd` follow the circulating convention from the first run after the deploy (35 days of `market_cap_latest` rewritten - widen the Neon load window once if you want the older days restated). `USDH`, `USDE`, `USDXL` are stablecoins now: `is_stablecoin = true`, category `stablecoin`, excluded by your `exclude_stable` filters |
| v1.10.9 | `market_cap_latest` drops tokens whose cap exceeds 5,000 days of their trailing-7-day spot volume (DRV, ONEAR, ZAMA, LINK0 disappear; HYPE, KNTQ, TREAD, XMR1 stay). `wallet_metadata.count_tx` / `active_days` / `first_tx` / `latest_tx` count token movements only (spot_fill, transfer, deposit, withdraw, mint, burn) - set `HL_WALLET_TX_EVENT_TYPES=spot_fill,transfer,deposit,withdraw` on your side so `wallet_transactions` carries the same rows, then run the hypercore backfill once |

Check the deployed version before onboarding (`/api/status` on the web monitor, or
`hypercore-status --json`): while the host still runs v1.4.0, project
`first_block_timestamp AS first_tx` and `latest_block_timestamp AS latest_tx` in your
`wallet_metadata` SQL (or `CAST(NULL AS TIMESTAMPTZ)`); never load the string columns
as they are. From v1.5.0 the columns are the instants themselves.

**Onboarding checklist** (all green before the first `hypercore` load):

1. The host runs v1.7.0 or later (`version` in `/api/status`), and its last run has
   `ok: true`.
2. `python -m blueblocx_hyperliquid.duckdb_smoke` from **your** host (reader role +
   read-only mount) reads every table and prints the `iceberg_scan` form to use.
3. `prices.history_pending == 0` in `/api/status` (or `_run_summary.json`): every
   priceable token has its 32-day candle history.
4. `control.table_watermarks` shows `status = 'completed'` for `l1_wallet_ledger`,
   `l1_token_metadata`, `wallet_watchlist`, `token_whitelist`, `l4_wallet_metadata`,
   `l3_token_price_usd_1h`, `l3_token_price_usd_1d` with `watermark_ts` from the
   last hour.
5. The `chains` row for `hypercore` exists in every destination (section 6, item 3).

## 9. Open decisions (owner)

- `native_symbol` / `native_decimals` for the `chains` row; explorer templates.
- De-listing (`is_active = false` + fresh stamp) for wallets/tokens that leave the set.
- `active_on_avg` definition (NULL until then). Decided: funding is excluded from `count_tx` / `active_days` / first and latest instants.
- ~~Whether a hypercore L4 layer (balances, networth, PnL, sharpe) is built at all, and by whom.~~ Decided 2026-10-21: built on the lake side, shipped in v1.9.0 (section 3); the token / market layer followed in v1.10.0 (2026-10-22). Nothing of the 34-label contract is left out.
