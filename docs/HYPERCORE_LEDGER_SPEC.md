# HyperCore → `nessie.hypercore.*` ledger spec (v1, 2026-10-05)

This is the **single source of truth** for how Hyperliquid HyperCore data maps into
the BlueBlocx lakehouse. Two earlier POCs encoded the ledger differently; this
document reconciles them. Any implementation (Python here, a future Rust
`parse_hypercore_ledger`, or the S3 node_fills backfill) must produce
byte-identical keys and rows from the same input. `tests/test_mappers.py` is the
executable form of this spec.

## 0. Decisions reconciled against `hyperliquid-poc-1` (2026-07-14)

| Topic | poc-1 | **this spec** | Why |
|---|---|---|---|
| `standard` column | `spot` / `perp` / `HIP-1` | **`"ERC20"` on every row** | Downstream Rust consumers (statement-watchlist, FIFO, balance, networth) hard-gate `standard=="ERC20"`; anything else is dropped. Symbol lives in `l1_token_metadata.symbol`. |
| token identity | EVM contract address from spotMeta, else `hl:{asset_id}` | **`hlcore:token:{spotMeta token index}`**, USDC = `hlcore:usdc` | Stable across renames; no collision with HyperEVM ERC-20 rows which live in a separate namespace. Bridging Core↔EVM netting is a later product decision (see §7). |
| rows per fill | fixed 2 (`event_index ∈ {0,1}`) | **0–3 legs with fixed ordinals** (§2) | Spot buy fees are charged in the base token and need their own leg; perp fills with `closedPnl==0` and `fee==0` produce 0 rows (no cashflow). |
| perp representation | ±size position on `hl:{asset}` + USDC cash | **USDC cashflow only** (closedPnl / fee / funding) | A perp position is not a token balance; HL's `closedPnl` is exact (average-entry), so no FIFO. Open positions go to `l4x_perp_position_snapshot`. |
| spot `closedPnl` | — | **not booked** | HL reports a small `closedPnl` on spot sells against its own cost basis. We emit only the base/quote legs; spot realized PnL is computed by the stock FIFO layer. The raw spot closedPnl is surfaced in `_reconciliation.json` (`spot_closed_pnl_raw_not_booked`) for audit. |
| source | S3 `hl-mainnet-node-data` (lz4, requester-pays) | Info API for the recent window; **reuse poc-1's S3 reader for backfill** | Info API serves only the ~10k most-recent fills. |

## 1. Natural key — wallet-scoped, content-addressed

Downstream Rust dedups on `(tx_hash, event_index[, direction])` **without**
`wallet_address` (parse-raw `existing_keys`, fifo-pnl `seen_keys`). Therefore
every HyperCore `tx_hash` is prefixed with the wallet and derived from content,
never from a counter.

| Record | `tx_hash` | notes |
|---|---|---|
| fill (tid ≠ 0) | `hlcore:{wallet}:tid:{tid}` | `tid` is HL's unique trade id; one order `hash` can carry many fills, so the order hash is **not** used. |
| fill (tid = 0, dust conversion) | `hlcore:{wallet}:dust:{time_ms}:{coin}:{sz}` | HL emits tid=0 + zero hash for midnight dust conversions. |
| funding | `hlcore:{wallet}:funding:{coin}:{time_ms}` | no hash on the wire; one row per (coin, hour). |
| ledger update with real hash | `hlcore:{wallet}:{hash}:{type}` | a `send` shows the **same** hash on sender and receiver → must be wallet-scoped. |
| ledger update without hash | `hlcore:{wallet}:{type}:{time_ms}` | |

`block_number` and `tx_index` are **NULL** (no EVM block). The Iceberg DDL allows
it; a Rust row struct must use `Option<i64>` / `Option<i32>`.

Invariant (asserted by `extract.py` on every run and by the unit tests):
`(tx_hash, event_index, direction)` is unique chain-wide across all wallets.

## 2. `event_index` — fixed per-leg ordinals (`schema.LEG_*`)

| Source | leg | `event_index` | `event_type` | token |
|---|---|---|---|---|
| perp fill | closedPnl ≠ 0 | 0 | `perp_realized_pnl` | USDC |
| perp fill | fee ≠ 0 | 1 | `fee` | `feeToken` (USDC) |
| perp fill | builderFee ≠ 0 | 2 | `fee` | USDC |
| spot fill | base leg | 0 | `spot_fill` | base token |
| spot fill | quote leg | 1 | `spot_fill` | **quote token** (USDC for most pairs; 17/316 pairs quote in another token) |
| spot fill | fee ≠ 0 | 2 | `fee` | `feeToken` (base token on buys, USDC on sells) |
| spot fill | builderFee ≠ 0 | 3 | `fee` | USDC (distinct ordinal from the regular spot fee) |
| funding | — | 0 | `funding` | USDC |
| ledger update | — | 0 | see §3 | USDC or token |
| `accountClassTransfer`, `cStakingTransfer`, `borrowLend`, self-`send`/`spotTransfer` (user == destination == wallet) | out / in | 0 / 1 | `transfer` | token — **two legs so the wallet nets to zero** (internal move between the wallet's own books) |
| `borrowLend` with `interestAmount` ≠ 0 | income | 2 | `interest` | token — lending interest is realized income |

Ordinals never depend on iteration order, the input window, or which fills the
source happened to include. Re-running, shifting the window, or switching from
Info API to S3 node_fills yields identical keys.

## 3. `event_type` taxonomy and classification

| `event_type` | from | classification |
|---|---|---|
| `perp_realized_pnl`, `fee`, `funding` | fills / userFunding | **PnL** (`REALIZED_PNL_EVENT_TYPES`); excluded from FIFO and from net_flow |
| `staking_reward` | (reserved) delegator rewards | PnL (income) |
| `spot_fill` | spot fills | token movement → stock FIFO/balance |
| `deposit`, `withdraw` | bridge | **capital flow** (`FLOW_EVENT_TYPES`) |
| `transfer` | send / spotTransfer / vault* / sub-account / accountClassTransfer / spotGenesis / borrowLend | token movement; internal moves net to zero |

Unknown `delta.type` values map to `transfer` **and are surfaced** in
`_reconciliation.json.unmapped_ledger_types` — never silent.

Derived metrics (one definition, used by `analytics.py`; a Rust rollup must match):

```
usd(row)        = amount_signed × price_usd(token_address)   -- stables = 1; spot tokens from
                                                             -- spotMetaAndAssetCtxs markPx; a row
                                                             -- with NO price is counted/surfaced
                                                             -- (unvalued_ledger_rows), never summed
realized_pnl_Nd = FIFO_spot_realized_Nd
                + Σ usd(row) WHERE event_type IN (perp_realized_pnl, funding, fee, staking_reward, interest)
                  -- NOT a raw Σ amount_signed: spot-buy fee legs are in the BASE token
                  -- and must be valued, and a window labelled Nd is only emitted when
                  -- ≥ N days were ingested and fills were not truncated (else NULL +
                  -- *_complete=false)
unrealized_pnl  = Σ l4x_perp_position_snapshot.unrealized_pnl_usd        (latest as_of)
net_flow_Nd     = Σ usd(row) WHERE event_type IN (deposit, withdraw, transfer)  -- never PnL
net_worth       = clearinghouseState.accountValue                          -- perp book (ground truth)
                − min(accountValue, spot USDC.spotHold)                     -- portfolio-margin overlap: unified
                                                                            --   accounts collateralise perps with
                                                                            --   spot USDC that is already in total
                + Σ spot balance × price  (stables at $1)                   -- spotClearinghouseState
                + staked HYPE × HYPE price                                  -- delegatorSummary
                + Σ vault equity                                            -- userVaultEquities
```

## 4. Token identity, decimals, stables

* Resolve every spot coin through `spotMeta` **at run start**: `universe[i].tokens =
  [baseIdx, quoteIdx]`, `tokens[j].weiDecimals`. `token_address = hlcore:token:{idx}`
  (USDC idx 0 → `hlcore:usdc`). `l1_token_metadata` is written in the same run.
  Do **not** derive `amount` from a pre-existing metadata table (first-seen tokens
  would get `amount=NULL` and be dropped by the statement gate).
* Stablecoins (`USDC, USDT0, USDT, USDHL, FEUSD`) → `is_stablecoin=true`, priced
  at $1.
* `amount_raw = amount × 10^decimals` (f64 on both Python and Rust; identical bits
  for decimals ≤ 22).

## 5. Pricing

* **Per-wallet path**: current prices come from **one** `spotMetaAndAssetCtxs`
  call (`midPx`, fallback `markPx`), aligned to `spotMeta.universe`. No candles.
* **L3 `l3_token_price_usd_1m`** is a **per-token shared asset** built by
  `price_job.py`, not per wallet. `candleSnapshot` returns only the ~5000 most
  recent candles and cannot be paged backward, so the interval is chosen from the
  range (`interval_for_range`: 1m ≤ 3.47d, 5m ≤ 17d, 15m ≤ 52d, 1h ≤ 208d,
  4h ≤ 833d, else 1d); rows are sparse for coarser intervals and
  the downstream backward ASOF join tolerates that. Spot candles must be requested
  with the `@index` coin form (`SYMBOL/USDC` returns HTTP 500).
* Stables are pinned to $1 on the same grid.

## 6. Consumer gates and ownership

* Seed `wallet_watchlist.is_active=true` and `token_whitelist.is_active=true` for
  every HyperCore wallet/token, or the sink stays empty even with correct L1.
* `wallet_analytics_contrib.json` is **input/validation**, not the sink. The Rust
  `wallet-analytics` crate owns `all_chain.wallet_analytics`.
* Validation oracle: `portfolio` endpoint — `perpAllTime.accountValueHistory[-1]`
  must equal `clearinghouseState.accountValue`, and `allTime` must equal the
  computed net_worth within 1% (verified: portfolio-margin wallet −0.001%, plain
  perp wallet 0.0%). Both are asserted in `_reconciliation.json`.

## 6b. Sources, HIP-3 dexes, fallbacks

* **Sources**: Info API for the recent window (`userFillsByTime` ≤ ~10k fills,
  `userFunding`, `userNonFundingLedgerUpdates`); S3
  `hl-mainnet-node-data/node_fills_by_block/hourly/{YYYYMMDD}/{h}.lz4` for fills
  beyond the cap. Node fills carry the same fields plus `builderFee`/`builder`/
  `cloid`; `block_number` is available there but left **NULL** on the row so
  rows are byte-identical across sources (the key never uses it).
* **HIP-3 builder dexes** (`perpDexs`: xyz, flx, vntl, …): fills and funding
  carry `dex:COIN` coins and map as perps (USDC cashflow). Each dex has its own
  margin account → `clearinghouseState(user, dex=…)` per dex; positions join the
  l4x snapshot and their `accountValue` is added to net worth
  (`account_value_by_dex`). The portfolio-margin overlap (§3) applies to the
  main dex only.
* **Ledger types**: `internalTransfer` and `subAccountTransfer` map to
  `transfer` with normal in/out direction (they move between distinct
  addresses); the net-zero two-leg treatment is reserved for moves inside ONE
  address' books (§2).
* **Fallback identities** when spotMeta cannot resolve a coin/token:
  `hlcore:spot:{SYMBOL}` with `decimals=0` (logged); perp reference prices use
  `hlcore:perp:{COIN}`. Neither is a balance token; both are surfaced via
  `unvalued_ledger_rows` / `unpriceable_spot_tokens` rather than silently valued.

## 7. Out of scope here (atlas-project / platform changes needed)

Language-independent work on the serving chain, tracked but **not** done in this repo:

1. `atlas-project/services/rust/wallet-analytics/src/core.rs` — add `hypercore`
   arms (stablecoin set, no WETH).
2. `atlas-project/services/rust/control-jobs/.../wallet_watchlist.rs` — gate counts
   only `event_type=='transfer'`; HyperCore wallets with only fills/pnl/fee rows
   never become active unless seeded (we seed) or the gate is widened.
3. `atlas-project/trino/views/all_chain/0_vw_wallet_analytics_live.sql` — UNION
   currently ethereum+arbitrum only.
4. `etl/dagster_blueblocx` export chain spec and `core-api` `chains` row
   (`is_evm=false`); `cockroachdb_sync.py` hard-codes `CHAIN='ethereum'`.
5. Bridge identity: Core USDC/HYPE vs HyperEVM ERC-20 should map to one asset so
   bridging nets to zero across namespaces (product decision pending).
6. Live/near-real-time Hyperliquid is owned by `platform-backend/src/hyperliquid`
   (TypeScript). This pipeline's job is durable history and per-wallet PnL.
