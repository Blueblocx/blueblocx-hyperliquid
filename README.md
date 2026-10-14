# Hyperliquid → BlueBlocx lakehouse pipeline (HyperCore)

Pulls Hyperliquid **HyperCore** data (native L1 perps + spot, non-EVM) via the
public Info API into CSVs whose columns match the Iceberg schema, so they lift
into `nessie.hypercore.*` with the DDL in `docs/PROPOSED_SCHEMA.sql`. The mapping
rules are frozen in **`docs/HYPERCORE_LEDGER_SPEC.md`** and enforced by
`tests/test_mappers.py`.

## Two namespaces (not one)
HyperCore and HyperEVM are **separate** namespaces (`nessie.hypercore.*`,
`nessie.hyperevm.*`), mirroring ethereum/arbitrum. HyperCore PnL is exact (per-fill
`closedPnl` from the engine, average-entry model, no FIFO); EVM values must be
reconstructed from transfers + price oracles. Keeping them apart keeps the exact
numbers exact. This repo covers HyperCore only.

## Invariants (see the spec)
- **`standard` is always `"ERC20"`** so downstream consumer gates don't drop rows;
  the HL symbol lives in `l1_token_metadata.symbol`.
- **Natural keys are wallet-scoped and content-addressed**: `tx_hash =
  hlcore:{wallet}:tid:{tid}` (fills), `...:funding:{coin}:{time}`,
  `...:{hash}:{type}` (ledger); `event_index` is a fixed per-leg ordinal. Re-runs,
  window shifts and multi-wallet ingest never collide on the Rust
  `(tx_hash, event_index, direction)` dedup key. The extractor asserts this
  chain-wide on every run.
- **net_flow** sums only `deposit`/`withdraw`/`transfer`; perp PnL/fee/funding are
  never flow. `accountClassTransfer` emits two legs and nets to zero.
- **realized PnL** = FIFO spot + Σ USD-valued(`perp_realized_pnl` + `funding` − `fee`
  + `interest`). Rows are valued by token price (spot-buy fees are in the base
  token); a window labelled Nd is emitted only when ≥ N days were ingested and
  fills were not truncated, otherwise `null` with `*_complete=false`.
- **Internal moves net to zero**: `accountClassTransfer`, `cStakingTransfer`,
  `borrowLend`, and self-`send` between dexes emit two opposite legs.

## Layout
| module | role |
|---|---|
| `blueblocx_hyperliquid/extract.py` | per-wallet (or multi-wallet) extract → L1 / L4x / gates / analytics contribution; thread-pooled Info API calls under a weight budget; **upserts** every CSV by natural key; `--incremental` (per-wallet watermark, 1h overlap), `--s3-backfill` (fills beyond the 10k API cap), `--iceberg` (lift), HIP-3 dexes snapshotted per `perpDexs` |
| `blueblocx_hyperliquid/s3_backfill.py` | fills from `s3://hl-mainnet-node-data/node_fills_by_block/hourly/` (lz4 JSON lines, Requester Pays); same fill shape as the API plus `builderFee`; verified tid-for-tid equal to the API on an overlapping hour |
| `blueblocx_hyperliquid/iceberg_load.py` | lift CSVs into Iceberg via pyiceberg — schemas parsed from `docs/PROPOSED_SCHEMA.sql`; REST/Nessie when `NESSIE_URI` is set, SQL catalog with `HL_CATALOG_URI` (production), else a local SQLite catalog under `out/iceberg/`; facts are insert-if-absent, snapshots/dimensions replace-by-key **in one commit per table** (a reader never sees a half-replaced table), parents lifted before facts; NOT-NULL-by-construction columns and duplicate keys are refused before any commit; writes `control.table_watermarks` (`completed` / `failed` / `restated`, `watermark_ts` = max write stamp) |
| `blueblocx_hyperliquid/daily.py` | one-command daily run for a wallet list (incremental + HIP-3 + optional S3/Iceberg) under a single-flight lock; writes the run summary and the live progress file; `deploy/dagster_hypercore_assets.py` is the dagster asset template that wraps it |
| `blueblocx_hyperliquid/web.py`, `blueblocx_hyperliquid/status.py` | the **web monitor** (`hypercore-web.service`, port 8787: running / phase / wallet i/N / STUCK, last run, history, per-wallet table; `/api/status`, `/healthz`) and the same verdict as a CLI (`hypercore-status`, exit 0/1) |
| `blueblocx_hyperliquid/duckdb_smoke.py` | reads the lake the way the **ETL's DuckDB** does (pointer from the catalog's `iceberg_tables`, then `iceberg_scan`), tries the raw location, the stripped path and the table root with the pointer's version pinned (`allow_moved_paths`), and reports versions, row counts, timestamp rendering and the read form that worked; `preflight --duckdb` wraps it. Needs only `duckdb` + `sqlalchemy` (not pyiceberg): run from a throwaway venv or the ETL host, never from `/opt`; hand-off for the ETL team in `docs/ETL_HANDOFF_HYPERCORE.md` |
| `blueblocx_hyperliquid/tracked.py` | **which wallets**: the Info API is per address, so a run needs a list — pinned wallets (`HL_WALLETS_FILE`, optional) + with `HL_TRACKED_SOURCE=leaderboard` the set the TS platform-backend sweeps live (top 100 whales by account value + up to 50 smart-money wallets from the Hyperliquid leaderboard), refreshed every run from the stats host, cached, rotated so a tail deferred by the soft deadline runs first next hour |
| `blueblocx_hyperliquid/config.py`, `blueblocx_hyperliquid/ops.py`, `blueblocx_hyperliquid/preflight.py` | env/.env/logging (`ENV_VARS` is the authoritative list), atomic writes + lock + summary + progress, pre-run health check |
| `blueblocx_hyperliquid/evm_extract.py` | **HyperEVM** per-wallet ERC-20 extract → `out/hyperevm/*` (separate namespace; same `standard="ERC20"` / metadata / upsert invariants; production should use the atlas EVM path) |
| `blueblocx_hyperliquid/price_job.py` | **shared per-token** L3 price asset (`l3_token_price_usd_1m`); interval chosen from range because `candleSnapshot` only serves the ~5000 most recent candles |
| `blueblocx_hyperliquid/mappers.py` | fills / funding / ledger → `l1_wallet_ledger` rows (the spec, executable) |
| `blueblocx_hyperliquid/analytics.py` | realized / unrealized / net_flow / net_worth / stable_pct + `portfolio` oracle check |
| `blueblocx_hyperliquid/spot_meta.py` | spotMeta resolver: coin → pair / base / quote / decimals / `token_address` |
| `blueblocx_hyperliquid/info_client.py` | Info API client: retries, **proactive weight budget**, thread-safe sessions, pagination |
| `tests/test_mappers.py` | cross-wallet key uniqueness, fixed ordinals, idempotency, dust tid=0, net-zero internal moves, non-USDC quote pairs |

## Production
**Server deployment is documented in [`docs/DEPLOY.md`](docs/DEPLOY.md)** (host
requirements, install, env, preflight, first run, systemd/cron/Docker scheduling,
monitoring, upgrade, what still lives in atlas). The pieces:

| piece | where |
|---|---|
| pinned dependencies | `requirements.txt` + `constraints.txt` (full transitive pin set) |
| configuration | environment variables only, documented in `blueblocx_hyperliquid/config.py` (`ENV_VARS`); `deploy/.env.example` mirrors it; `.env` is loaded without overriding the process env |
| health check | `python -m blueblocx_hyperliquid.preflight [--offline] [--iceberg --s3]` — deps, tzdata, DDL, paths, env sanity, HL API + clock skew, S3 requester-pays, Nessie REST; never prints secrets |
| scheduler entry point | `python -m blueblocx_hyperliquid.daily` — lock, incremental run, run summary (`_run_summary.json`, `_run_history.jsonl`); exit 0/1/2/3 |
| wallet list | pins in `HL_WALLETS_FILE` + the automatic tracked set with `HL_TRACKED_SOURCE=leaderboard` (`tracked.py`); `HL_RUN_SOFT_DEADLINE_MIN` defers the tail of a list that outgrows the slot to the next run instead of the service timeout killing the whole run |
| monitoring | the web monitor `deploy/hypercore-web.service` (`python -m blueblocx_hyperliquid.web`, `http://<host>:8787/`): running / phase / wallet i/N / STUCK after `HL_STUCK_MINUTES` quiet, last verdict and its age, recent runs, per-wallet table, timers; `/api/status`, `/healthz`. Same verdict as a CLI: `hypercore-status` (exit 0/1). Reads the live `_run_progress.json`; standard library only, read-only, no authentication (LAN) |
| scheduling | `deploy/hypercore-daily.{service,timer}` (systemd, daily) or `deploy/hypercore-hourly.timer` + `deploy/hourly-override.conf` (hourly), `deploy/run_daily.sh` + `deploy/crontab.example` (cron), `Dockerfile` (batch image), `deploy/dagster_hypercore_assets.py` (template) |
| catalog | three modes in `blueblocx_hyperliquid/iceberg_load.py`: **standalone** `HL_CATALOG_URI` (PostgreSQL, or SQLite for a single host) + `WAREHOUSE=file:///…` on a NAS mount guarded by `HL_WAREHOUSE_MOUNT` — no Nessie, no object store; **shared atlas lake** `NESSIE_URI` + S3 store; **dev** SQLite under `HL_OUT_DIR/iceberg` |
| maintenance | `python -m blueblocx_hyperliquid.maintenance --expire-days 7` (nightly timer in `deploy/`): expires old snapshots so `metadata.json` stays small; a lift is at most one commit per table that received rows plus one for the watermarks (~8), regardless of wallet count |
| single-flight lock | `HL_OUT_DIR/hypercore/.daily.lock`, shared by `daily`, `extract`, `iceberg_load` and `maintenance`; a lock whose PID is a live pipeline process (`/proc/<pid>/cmdline`) is never stolen (a hand-run S3 backfill may hold it for hours: scheduled ticks exit 3 meanwhile); a dead or recycled PID's lock is discarded at once |
| readers | the ETL reads the lake with DuckDB through a read-only catalog role (`READER_CIDR=<ip>/32 bash deploy/bootstrap.sh`, `/etc/hypercore/reader.env`) and a read-only NFS mount at the identical path; `blueblocx_hyperliquid.duckdb_smoke` is the acceptance test, `docs/ETL_HANDOFF_HYPERCORE.md` the hand-off |
| state hygiene | atomic writes for every state file; staged CSVs pruned to `HL_RETENTION_DAYS` after a successful Iceberg lift |
| CI | `.github/workflows/ci.yml` — tests on Python 3.12/3.13, offline preflight, Docker image build + in-container smoke |

## Run
```bash
python -m venv .venv && ./.venv/Scripts/python -m pip install -r requirements.txt -c constraints.txt
# (tzdata is in there: pyarrow needs the IANA zone database for timestamptz columns on Windows)
./.venv/Scripts/python -m unittest discover -s tests -t . -v
./.venv/Scripts/python -m blueblocx_hyperliquid.preflight --offline            # deps / DDL / env sanity

# full history for a 10k-capped wallet, resumable, lifted into Iceberg (local catalog by default)
./.venv/Scripts/python -m blueblocx_hyperliquid.extract --wallet 0xA --days 30 --s3-backfill --incremental --iceberg
# daily (scheduler entry point) + HyperEVM namespace
./.venv/Scripts/python -m blueblocx_hyperliquid.daily --wallets-file wallets.txt --iceberg --s3-backfill
./.venv/Scripts/python -m blueblocx_hyperliquid.evm_extract --wallet 0xA --blocks 3000

# one or many wallets; outputs are upserted into out/hypercore/
./.venv/Scripts/python -m blueblocx_hyperliquid.extract --wallet 0xA,0xB --days 7
./.venv/Scripts/python -m blueblocx_hyperliquid.extract --wallet 0xA --days 7 --with-prices   # also L3
#   --workers N (concurrent Info API calls, default 4)  --max-price-coins N (perp
#   reference prices cap, default 30)  --out DIR
# exit code is non-zero if a natural-key violation is detected (nothing is written)

# shared L3 price asset on its own
./.venv/Scripts/python -m blueblocx_hyperliquid.price_job --spot HYPE,UBTC --perp BTC --days 30
```
Env (`.env` is auto-loaded, never committed; full list with defaults in
`blueblocx_hyperliquid/config.py`): `HL_INFO_URL` (default mainnet), `HL_WEIGHT_BUDGET` (default
900/min; HL caps at 1200 per IP — leave headroom for other pollers on the same
IP), `HL_OUT_DIR` (state directory), `HL_RETENTION_DAYS` (staged-CSV retention
after a lift, ≥ 31), `HL_WALLETS_FILE` + `HL_TRACKED_SOURCE=leaderboard` (which
wallets: pins + the automatic whale/smart-money set), `HL_RUN_SOFT_DEADLINE_MIN`
(hourly cadence: defer the tail instead of being killed), `HL_WEB_BIND`/`HL_WEB_PORT` (web monitor), `AWS_ACCESS_KEY_ID`/
`AWS_SECRET_ACCESS_KEY` (S3 backfill, Requester Pays — **rotate the key that was
pasted in chat before any production use**), `NESSIE_URI` + `NESSIE_BRANCH`/
`WAREHOUSE`/`S3_ENDPOINT`/`S3_ACCESS_KEY`/`S3_SECRET_KEY` (Iceberg REST; unset →
local catalog), `HYPEREVM_RPC_URL`.

### Outputs (`out/hypercore/`)
| file | key | contents |
|---|---|---|
| `l1_wallet_ledger.csv` | (tx_hash, event_index, direction) | spot legs, perp cashflow (`closedPnl`/`fee`/`funding`), deposits/withdrawals/transfers |
| `l1_token_metadata.csv` | token_address | symbol / decimals / `is_stablecoin` |
| `l4x_perp_position_snapshot.csv` | (as_of_date, wallet, coin) | open positions + unrealized / margin / liquidation — accumulates a **daily history** across runs |
| `l4_wallet_metadata.csv` | wallet | activity summary |
| `wallet_watchlist.csv`, `token_whitelist.csv` | wallet / token | consumer gates seeded `is_active=true` |
| `l3_token_price_usd_1m.csv` | (token_address, bucket_minute) | only with `--with-prices` or `price_job` |
| `wallet_analytics_contrib.json` | wallet | contribution to `all_chain.wallet_analytics` (the Rust `wallet-analytics` crate owns the table) |
| `_reconciliation.json` | — | per-wallet cashflow identity assert, `portfolio` oracle check, **chain-wide natural-key collision check**, unmapped ledger types, unpriced symbols |
| `_run_summary.json`, `_run_history.jsonl` | — | written by `blueblocx_hyperliquid.daily`: verdict per run (hard problems vs warnings, per-wallet status, iceberg/retention counts); the history file gets one line per run |
| `_watermarks.json` | wallet | incremental state: `through_ms`, `coverage_start_ms`, sticky `fills_truncated`, cached lending position |
| `_lift_state.json` | — | last successful Iceberg lift; the next lift reads only rows updated since (bounded incremental lift) |
| `_tracked_wallets.json` | — | the leaderboard tracked set of the last refresh (`whales`, `smart`, `fetched_at`) + the rotation `cursor`; reused for `HL_TRACKED_MAX_AGE_HOURS` when the stats host is down |
| `_run_progress.json` | — | live state of the current run: `state`, `phase` (tracked-set / pull / s3-backfill / analyse / write / lift / finalise), `wallets_done`/`wallets_total`/`wallets_deferred`, `current`, `updated_at`; read by `status` |

### Validation
1. **Mapping fidelity (raw vs mapped)**: Σ raw `closedPnl` == Σ `perp_realized_pnl`
   rows, Σ raw USDC fees == −Σ `fee` rows, Σ raw funding == Σ `funding` rows, and
   row counts match (±$1) → `fidelity_ok`. This compares the API input to the
   emitted rows, so dropped/duplicated/sign-flipped rows fail it.
2. **External**: perp equity (main + HIP-3 dexes) == `portfolio.perpAllTime` latest
   value (the HL UI number) → `oracle.account_value_matches`; and computed net
   worth (perp − overlap with spot USDC + spot + staked HYPE + vault equity +
   borrow/lend supply) == `portfolio.allTime` within 1% → `oracle.networth_matches`.
   Validated on 7 live wallets (light, MM, HIP-3 trader, 48M whale, two lenders).
3. **Structural**: `natural_key_ok` — no `(tx_hash, event_index, direction)`
   collision across wallets, no duplicates within the run.
4. **PnL (external, from the 2nd daily run)**: HL `perpDay.pnlHistory` change over
   24h == our realized perp cashflow (closedPnl + funding − fees) + change in
   unrealized since the previous day's l4x snapshot → `oracle.pnl.pnl_matches`
   (reported as pending until a ~24h-old snapshot exists — never as passed).

Source completeness traps the checks above cannot see on their own, now handled:
`userFillsByTime` omits **TWAP slice fills** (6% of all fills chain-wide; pulled
from `userTwapSliceFillsByTime` and merged by tid), and the fills cap is not
exactly 10k (12.8k–21.6k observed), so truncation requires a cap-sized response
**and** a >1h gap after the window start.

### Demo wallets
- `0x04d4ffc796a9b9244ccd00459fe2785880dcf399` — spot + staked HYPE + light perps.
- `0x010461c14e146ac35fe42271bdc1134ee31c703a` — perp-heavy market maker (~13k
  fills/day, 175 open positions). Exceeds the 10k-fill API window →
  `fills_truncated_at_10k_cap=true`.

## Where this sits (see spec §7)
- Live / near-real-time Hyperliquid is served by the TypeScript module in
  `platform-backend/src/hyperliquid`. This pipeline provides **durable history
  and per-wallet PnL** for the lakehouse.
- Atlas production splits **Python raw ingest → Rust compute**. The cheapest path
  to production is: Python (this code or `python-ingestion`) → Iceberg
  `nessie.hypercore.*` → existing Rust L4 crates with `{"chain":"hypercore"}`,
  after the small atlas changes listed in the spec. A Rust port is justified for
  the S3 node_fills backfill (~3M fills/day), using `hyperliquid-poc-1`'s S3
  reader as the starting point and this spec + the unit tests as the contract.

## Known follow-ups
- **HIP-3**: fills/funding on builder dexes (`xyz:SP500` …) are mapped as perps
  and open positions are snapshotted per dex; whether `portfolio.allTime`
  includes other-dex equity is unverified (demo wallets had none open) — the
  `networth_matches` check will surface it.
- **Bridge identity** (Core USDC ↔ HyperEVM USDC) is a pending product decision;
  the two namespaces are kept separate until then.
- **S3 backfill is mandatory for market-maker wallets**: the demo MM wallet does
  ~5,000–8,500 fills **per hour** (measured from node_fills hour files), so the
  Info API's 10k cap covers only ~80 minutes of its history; any longer window
  comes from S3. Hour files are 25–70 MB lz4 (Requester Pays) and publish ~1h
  after the hour; the extractor fetches only the gap the API could not serve,
  reports missing/failed hours, and does not advance the watermark until the
  window is complete. Measured end to end on the MM wallet: a 24h window = 23
  hour files, 165,504 S3 fills + 14,504 API fills = 180,008 (fidelity residual
  0.0), ~35 min wall-clock (~90 s per hour file, dominated by scanning
  ~130–330 MB of JSON lines per file — parallelising the parse is the next
  throughput lever).
- **`group hash`**: `xxh64(addr, seed=0).abs() % 4000` matches Rust production.
