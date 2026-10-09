# Hyperliquid → BlueBlocx lakehouse POC (HyperCore)

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
| `hl_poc/extract.py` | per-wallet (or multi-wallet) extract → L1 / L4x / gates / analytics contribution; thread-pooled Info API calls under a weight budget; **upserts** every CSV by natural key; `--incremental` (per-wallet watermark, 1h overlap), `--s3-backfill` (fills beyond the 10k API cap), `--iceberg` (lift), HIP-3 dexes snapshotted per `perpDexs` |
| `hl_poc/s3_backfill.py` | fills from `s3://hl-mainnet-node-data/node_fills_by_block/hourly/` (lz4 JSON lines, Requester Pays); same fill shape as the API plus `builderFee`; verified tid-for-tid equal to the API on an overlapping hour |
| `hl_poc/iceberg_load.py` | lift CSVs into Iceberg via pyiceberg — schemas parsed from `docs/PROPOSED_SCHEMA.sql`; REST/Nessie when `NESSIE_URI` is set, else a local SQLite catalog under `out/iceberg/`; facts are insert-if-absent, snapshots/dimensions replace-by-key; writes `control.table_watermarks` |
| `hl_poc/daily.py` | one-command daily run for a wallet list (incremental + HIP-3 + optional S3/Iceberg) under a single-flight lock; writes the run summary and POSTs the alert webhook; `deploy/dagster_hypercore_assets.py` is the dagster asset template that wraps it |
| `hl_poc/config.py`, `hl_poc/ops.py`, `hl_poc/preflight.py` | env/.env/logging (`ENV_VARS` is the authoritative list), atomic writes + lock + summary + alert, pre-run health check |
| `hl_poc/evm_extract.py` | **HyperEVM** per-wallet ERC-20 extract → `out/hyperevm/*` (separate namespace; same `standard="ERC20"` / metadata / upsert invariants; production should use the atlas EVM path) |
| `hl_poc/price_job.py` | **shared per-token** L3 price asset (`l3_token_price_usd_1m`); interval chosen from range because `candleSnapshot` only serves the ~5000 most recent candles |
| `hl_poc/mappers.py` | fills / funding / ledger → `l1_wallet_ledger` rows (the spec, executable) |
| `hl_poc/analytics.py` | realized / unrealized / net_flow / net_worth / stable_pct + `portfolio` oracle check |
| `hl_poc/spot_meta.py` | spotMeta resolver: coin → pair / base / quote / decimals / `token_address` |
| `hl_poc/info_client.py` | Info API client: retries, **proactive weight budget**, thread-safe sessions, pagination |
| `tests/test_mappers.py` | cross-wallet key uniqueness, fixed ordinals, idempotency, dust tid=0, net-zero internal moves, non-USDC quote pairs |

## Production
**Server deployment is documented in [`docs/DEPLOY.md`](docs/DEPLOY.md)** (host
requirements, install, env, preflight, first run, systemd/cron/Docker scheduling,
monitoring, upgrade, what still lives in atlas). The pieces:

| piece | where |
|---|---|
| pinned dependencies | `requirements.txt` + `constraints.txt` (full transitive pin set) |
| configuration | environment variables only, documented in `hl_poc/config.py` (`ENV_VARS`); `deploy/.env.example` mirrors it; `.env` is loaded without overriding the process env |
| health check | `python -m hl_poc.preflight [--offline] [--iceberg --s3]` — deps, tzdata, DDL, paths, env sanity, HL API + clock skew, S3 requester-pays, Nessie REST; never prints secrets |
| scheduler entry point | `python -m hl_poc.daily` — lock, incremental run, run summary (`_run_summary.json`, `_run_history.jsonl`), webhook alert; exit 0/1/2/3 |
| scheduling | `deploy/hypercore-daily.{service,timer}` (systemd), `deploy/run_daily.sh` + `deploy/crontab.example` (cron), `Dockerfile` (batch image), `deploy/dagster_hypercore_assets.py` (template) |
| state hygiene | atomic writes for every state file; staged CSVs pruned to `HL_RETENTION_DAYS` after a successful Iceberg lift |
| CI | `.github/workflows/ci.yml` — tests on Python 3.12/3.13, offline preflight, Docker image build + in-container smoke |

## Run
```bash
python -m venv .venv && ./.venv/Scripts/python -m pip install -r requirements.txt -c constraints.txt
# (tzdata is in there: pyarrow needs the IANA zone database for timestamptz columns on Windows)
./.venv/Scripts/python -m unittest discover -s tests -t . -v
./.venv/Scripts/python -m hl_poc.preflight --offline            # deps / DDL / env sanity

# full history for a 10k-capped wallet, resumable, lifted into Iceberg (local catalog by default)
./.venv/Scripts/python -m hl_poc.extract --wallet 0xA --days 30 --s3-backfill --incremental --iceberg
# daily (scheduler entry point) + HyperEVM namespace
./.venv/Scripts/python -m hl_poc.daily --wallets-file wallets.txt --iceberg --s3-backfill
./.venv/Scripts/python -m hl_poc.evm_extract --wallet 0xA --blocks 3000

# one or many wallets; outputs are upserted into out/hypercore/
./.venv/Scripts/python -m hl_poc.extract --wallet 0xA,0xB --days 7
./.venv/Scripts/python -m hl_poc.extract --wallet 0xA --days 7 --with-prices   # also L3
#   --workers N (concurrent Info API calls, default 4)  --max-price-coins N (perp
#   reference prices cap, default 30)  --out DIR
# exit code is non-zero if a natural-key violation is detected (nothing is written)

# shared L3 price asset on its own
./.venv/Scripts/python -m hl_poc.price_job --spot HYPE,UBTC --perp BTC --days 30
```
Env (`.env` is auto-loaded, never committed; full list with defaults in
`hl_poc/config.py`): `HL_INFO_URL` (default mainnet), `HL_WEIGHT_BUDGET` (default
900/min; HL caps at 1200 per IP — leave headroom for other pollers on the same
IP), `HL_OUT_DIR` (state directory), `HL_RETENTION_DAYS` (staged-CSV retention
after a lift, ≥ 31), `HL_ALERT_WEBHOOK_URL`, `AWS_ACCESS_KEY_ID`/
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
| `_run_summary.json`, `_run_history.jsonl` | — | written by `hl_poc.daily`: verdict per run (hard problems vs warnings, per-wallet status, iceberg/retention counts); the history file gets one line per run |
| `_watermarks.json` | wallet | incremental state: `through_ms`, `coverage_start_ms`, sticky `fills_truncated`, cached lending position |
| `_lift_state.json` | — | last successful Iceberg lift; the next lift reads only rows updated since (bounded incremental lift) |

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
