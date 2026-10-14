# HyperCore lake: hand-off to the dagster_blueblocx ETL

> **Who this is for.** The ETL team adding `hypercore` (Hyperliquid HyperCore) as a
> source of `dagster_blueblocx`. It answers the questions your source contract
> (`NEW_CHAIN_SOURCE_CONTRACT*.md`) asks a new chain, and lists what has to change
> on the ETL side because this chain is read **with DuckDB from a pyiceberg SQL
> catalog**, not through Trino (owner decision, 2026-10-14).
>
> **Status.** This document is updated with every release of `blueblocx-hyperliquid`;
> the "Release status" table says which guarantees are live. Sections marked
> *(from vX.Y.Z)* describe behaviour that lands in that release.

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
(delete + append of a replaced table are one commit since v1.4.0), files are
immutable and uniquely named, and nightly snapshot expiry (7 days) deletes no data
files — a scan that started before a commit keeps reading its snapshot. Fetch all
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
| `l3_token_price_usd_1m` | `(token_address, bucket_minute)` | — | development-only output, **not populated in production**; native 1h/1d tables arrive in v1.7.0 |
| `l4x_perp_position_snapshot` | `(as_of_date, wallet_address, coin)` | — (extension) | daily perp positions / unrealized PnL; no `updated_at` (use `created_at`); replaced per `(as_of_date, wallet)` every hour |
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
   `row_key` as in section 4; `transfer_price_usd` / `transfer_value_usd` NULL until
   the price tables exist — stables may be valued at 1.0 via `is_stablecoin`),
   and from v1.7.0 `token_price_usd_1h` / `token_price_usd_1d` (plain SELECTs on
   native-candle tables). Every other label (balances, networth, PnL, sharpe,
   token stats/signals/quality, DEX pair volume, market cap / fear index /
   exchange flow, `wallet_performance_30d`) has **no HyperCore source**: leave it
   out of the hypercore bundle — the fixed 34-label clone would fail at export and
   a 0-row stub would fail your full refresh after the shared truncate.
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
| v1.7.0 | native candle tables `l3_token_price_usd_1h` / `l3_token_price_usd_1d` (one row per closed candle, stables pinned at 1.0, no perp reference rows) for `token_price_usd_1h/1d` and USD valuation in `wallet_transactions`; onboarding gate: `prices.history_pending == 0` in our run summary |
| v1.8.0 | nightly compaction of small files (reader planning time) |

Check the deployed version before onboarding (`/api/status` on the web monitor, or
`hypercore-status --json`): while the host still runs v1.4.0, project
`first_block_timestamp AS first_tx` and `latest_block_timestamp AS latest_tx` in your
`wallet_metadata` SQL (or `CAST(NULL AS TIMESTAMPTZ)`); never load the string columns
as they are. From v1.5.0 the columns are the instants themselves.

## 9. Open decisions (owner)

- `native_symbol` / `native_decimals` for the `chains` row; explorer templates.
- De-listing (`is_active = false` + fresh stamp) for wallets/tokens that leave the set.
- `active_on_avg` definition (NULL until then). Decided: funding is excluded from `count_tx` / `active_days` / first and latest instants.
- Whether a hypercore L4 layer (balances, networth, PnL, sharpe) is built at all, and by whom; the contract's L4 labels stay excluded until then.
