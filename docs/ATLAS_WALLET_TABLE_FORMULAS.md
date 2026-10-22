# ATLAS_WALLET_TABLE_FORMULAS.md — parity reference for the HyperCore rollups

Extracted 2026-10-21 from the atlas lake repo (Olympus, `~/blueblocx/atlas-project`;
a copy lives on the hub at `D:\BlueBlocx\agents\hyperliquid-etl\atlas`). Line
references are to that tree. These are the definitions the ETL already consumes
for ethereum/arbitrum/optimism/avalanche; `blueblocx_hyperliquid/rollups/*` ports
them for HyperCore and `tests/test_rollups.py` checks the port against them.

Chain-wide conventions:
- Every numeric column is float64; no rounding anywhere in the Rust jobs.
- Addresses lowercased. `wallet_group`/`token_group` = `xxh64(addr, seed=0) % 4000` (unsigned).
- Timestamps are `timestamp(6)` UTC; dates are UTC business days.
- "ASOF" = the latest entry with key `<=` the lookup key (inclusive), no forward
  fill from the future, no max staleness unless stated.

HyperCore deviations (documented in each module): the ledger IS the per-wallet
statement (no DEX swap layer), prices come from the native 1h/1d candles
(`l3_token_price_usd_1h` plays the role of `l3_token_price_usd_1m`), perp activity
is USDC cashflow (`perp_realized_pnl`, `fee`, `funding` rows), and the wallet set is
the tracked set.

---

## 1. `l4_wallet_balance_per_tx` + `l4_wallet_balance_state` (crate `services/rust/wallet-balance`)

Inputs: the wallet statement (`l3_wallet_statement_watchlist_filtered_tokens`:
`wallet_address, token_address, counterparty_address, block_number, block_timestamp,
block_date, tx_hash, tx_index, event_index, direction, amount_raw, amount,
amount_signed, created_at`, `tables.rs:186-202`).

Row filter / normalisation (`tables.rs:480-535`): drop rows where
`counterparty_address == wallet_address AND direction == 'in'` (self-transfer "in"
leg); keep the "out" leg with `amount_signed = 0.0`; NULL amounts → 0.0;
`block_date` NULL → `block_timestamp.date()`.

Grain: one row per ledger row, natural key `(tx_hash, event_index, direction)`.

| Column | Rule |
|---|---|
| `balance_amount` | **clamped running sum** `running = max(0, running + amount_signed)` per (wallet, token) (`core.rs:554`) |
| `balance_amount_raw` | unclamped running sum (`core.rs:549`) — HyperCore keeps only `balance_amount` (contract) |
| `created_at` | copied from the source row (`core.rs:580`) |
| `updated_at` | job wall-clock (`core.rs:502, 581`) |

Ordering (critical): per (wallet, token) sort by `block_number, tx_index, event_index,
direction` ASC (`"in" < "out"`) (`core.rs:522-528`). HyperCore has no block_number:
sort by `block_timestamp, event_index, direction`, then `tx_hash` for determinism.

Edge cases: `balance_amount` never negative (clamp discards the debt). No dust floor.

`l4_wallet_balance_state` (one row per pair, full overwrite): last row's
`block_timestamp`, `block_date`, `tx_hash`, `event_index`, `balance_amount`.

Window: FULL = truncate + replay from the first source row in 120-day chunks; a
member-set change (watchlist/whitelist hash) forces FULL. INCREMENTAL = rows with
`created_at > watermark`, replay guard on the natural key, cumsum from the state
seeds. No "today" clamp.

HyperCore port: wallet-level recompute of changed wallets (the replay is cheap
per wallet and immune to late rows), FULL once.

---

## 2. `l4_wallet_balance_1d_dense` (crate `services/rust/wallet-balance-1d`)

Inputs: `l4_wallet_balance_per_tx` (`wallet_address, token_address, block_date,
block_timestamp, tx_hash, tx_index, event_index, amount_signed, balance_amount`).
Seed for a window: the latest per_tx row per pair with `block_date < start`.

Grain: `(wallet_address, token_address, snapshot_date)`, partitioned by
`snapshot_date`, range-replace per window.

| Column | Rule (`core.rs:205-279`) |
|---|---|
| `snapshot_date` | every calendar day in the window |
| `opening_balance_amount` | previous day's closing; first day = seed balance or 0.0 |
| `closing_balance_amount` | `balance_amount` of the LAST row of the day (sort `block_number DESC, tx_index DESC, event_index DESC`), else carry-forward |
| `in_amount` | Σ `amount_signed` over rows with `amount_signed > 0` that day |
| `out_amount` | Σ `|amount_signed|` over rows with `amount_signed < 0` |
| `net_flow_amount` | `in_amount − out_amount` |
| `closing_block_timestamp` | `block_timestamp` of the closing row; carried forward; no history at all → `1970-01-01T00:00:00Z` |
| `created_at`, `updated_at` | job wall-clock |

Dense carry-forward: a pair appears every day once it is in the seed or has a row
in the window; it stops being carried when `prev_close == 0.0` and it had no rows
in the chunk (`core.rs:288-290`). Rows with `amount_signed == 0` set the day's
closing balance but add nothing to in/out. 7-day chunks. No today clamp: the last
snapshot_date is the source high (partial day), rewritten by the next run.

---

## 3. `l4_wallet_networth_1d_dense` (crate `services/rust/wallet-networth`)

Inputs: `l4_wallet_balance_1d_dense` (all balance/flow columns) and
`l3_token_price_usd_1m` (`token_address, bucket_date, bucket_minute, vwap_price_usd`)
from `start − 1 day`.

Price: daily price per token = `vwap_price_usd` of the LATEST `bucket_minute` of the
day (`tables.rs:188-194`); lookup per (token, snapshot_date) = latest daily price
with `bucket_date <= snapshot_date` within the loaded window (`core.rs:160-162`). No
stablecoin special case.

Grain `(wallet_address, snapshot_date)`, partition `snapshot_date`, 7-day chunks.

| Column | Rule (`core.rs:155-208`) |
|---|---|
| `opening_networth_usd` | Σ over priced tokens `clip(opening_balance_amount) × px` |
| `closing_networth_usd` | Σ `clip(closing_balance_amount) × px` |
| `net_flow_usd`, `in_usd`, `out_usd` | Σ `clip(net_flow/in/out) × px` |
| `priced_token_count` / `missing_price_token_count` | tokens with / without an ASOF price |
| `latest_price_bucket_minute` | max `bucket_minute` of the daily prices used; NULL if nothing priced |
| `closing_block_timestamp` | max `closing_block_timestamp` across ALL the wallet's tokens that day |
| `created_at`, `updated_at` | job wall-clock |

`clip(x) = 0.0 if |x| < 1e-9 else x` (`BALANCE_EPS`). No today clamp.

HyperCore deviation: perp account value is NOT a token balance; the networth here
is the spot/ledger-token networth like every other chain. (The perp equity is in
`l4x_perp_position_snapshot` / `wallet_analytics`.)

---

## 4. FIFO PnL (crate `services/rust/fifo-pnl`)

Source: the priced statement `l3_wallet_statement_usd_watchlist_1m`
(`wallet_address, token_address, block_number, block_timestamp, block_date, tx_hash,
tx_index, event_index, amount_signed, price_usd`). Filters: NULL `amount_signed`/
`price_usd` skipped; `amount_signed == 0` skipped; NaN/Inf price skipped; dedupe on
`(tx_hash, event_index)` first wins (`tables.rs:258-275`). `price_usd` = ASOF
`vwap_price_usd` of `l3_token_price_usd_1m` at the row's minute (`wallet-statement-usd`).

Buy = any row with `amount_signed > 0` (cost basis = `amount_signed × price_usd`);
sell = any row with `amount_signed < 0` (`sell_qty = |amount_signed|`). Transfers are
not distinguished from trades (`fifo/core.rs:47-80`).

Matching (`fifo/core.rs:107-142`): buys = carried open lots + new buys sorted by
`(block_number, tx_index, event_index)`; sells sorted the same; two-pointer FIFO,
`matched_qty = min(buy_remaining, sell_remaining)`, `sell_cost += matched × buy_price`;
`QTY_EPSILON = 1e-12`. Sells with no buys at all emit nothing; a sell that exceeds
the lots realizes only the matched portion (`proceeds = matched_qty × sell_price`).

Per-sell row: `sell_qty, sell_price_usd, matched_qty, proceeds_usd, cost_usd,
realized_pnl_usd = proceeds − cost`.

`l4_wallet_pnl_state_fifo` (pair grain): `last_block_timestamp, last_tx_hash,
last_event_index` of the pair's last source row; `open_qty = Σ remaining_qty`,
`open_cost_usd = Σ remaining_qty × buy_price`; `updated_at` = now. A pair appears
once it had any priced row (open_qty 0 if it only sold).

`l4_wallet_pnl_realized_1d` (`(snapshot_date, wallet_address)`): per sell day
`realized_pnl_usd = Σ`, `proceeds_usd = Σ`, `cost_usd = Σ`, `num_sells` = count of
realized sell rows (unbacked sells excluded).

"Today": FIFO never folds today — the auto window ends at `min(today − 1,
source_high − 1)`; explicit windows are clamped to yesterday (`fifo.rs:280-359`).

HyperCore port: spot legs (`event_type IN (spot_fill, transfer, deposit, withdraw)`)
are the statement; `price_usd` = ASOF 1h candle close (stables 1.0); USDC cashflow
rows of perps (`perp_realized_pnl`, `fee`, `funding`, `interest`, `staking_reward`)
are USDC in/out at price 1.0, i.e. FIFO on USDC is trivially 0 PnL — perp PnL is
reported through `wallet_analytics`, not through FIFO (same as the atlas: the FIFO
table is spot).

---

## 5. `vw_wallet_token_stats`, `vw_wallet_trade_summary`, `l4_wallet_trades_priced_usd`

### 5a `vw_wallet_token_stats` (`0_vw_wallet_token_stats.sql`)
Base `l4_wallet_pnl_state_fifo f` JOIN `l1_token_metadata m` LEFT JOIN latest price
(`close_price_usd` at max `bucket_minute`, no staleness cap) LEFT JOIN
`Σ realized_pnl_usd` per pair.

| Column | Formula |
|---|---|
| `position_qty` | `f.open_qty` |
| `position_cost_usd` | `f.open_cost_usd` |
| `avg_cost_usd` | `open_cost_usd / NULLIF(open_qty, 0)` |
| `price_usd` | latest close (NULL if never priced) |
| `position_value_usd` | `open_qty × price` (NULL if no price) |
| `realized_pnl_usd` | `COALESCE(Σ realized, 0)` |
| `unrealized_pnl_usd` | `open_qty > 0 AND price IS NOT NULL ? open_qty × price − open_cost_usd : 0.0` |
| `total_pnl_usd` | realized + unrealized |
| `total_return_pct` | `total_pnl_usd / NULLIF(open_cost_usd, 0)` (a ratio, not ×100) |
| `last_block_timestamp`, `last_block_date` (= date of it), `last_tx_hash`, `last_event_index` | from state |

### 5b `vw_wallet_trade_summary` (`0_vw_wallet_trade_summary.sql`)
Over ALL rows of the priced statement per pair, JOIN `l1_token_metadata`, LEFT JOIN
Σ realized per pair.

| Column | Formula |
|---|---|
| `token_buy_qty` | Σ `amount` where direction = 'in' |
| `token_buy_value_usd` | Σ `notional_usd` where 'in' (unpriced rows add NULL → ignored) |
| `avg_buy_price_usd` | `buy_value / NULLIF(buy_qty, 0)` |
| `token_sell_qty`, `token_sell_value_usd`, `avg_sell_price_usd` | same for 'out' |
| `net_profit_usd` | `COALESCE(Σ realized_pnl_usd, 0)` |
| `net_gain_pct` | `net_profit_usd / NULLIF(token_buy_value_usd, 0)` (ratio) |
| `last_activity_date` | max `block_date` (any direction) |

### 5c `l4_wallet_trades_priced_usd` (crate `wallet-trades-usd`)
Per per_tx row: `price_bucket_minute` = block_timestamp floored to the minute;
`price_vwap_usd` = ASOF vwap `<= price_bucket_minute` (30-day lookback);
`signed_notional_usd = amount_signed × price`, `abs_notional_usd = |amount| × price`.

---

## 6. `vw_wallet_whale_moves_24h` (`2_vw_wallet_whale_moves_24h.sql`)

`l1_wallet_ledger` rows with `block_timestamp >= now − 24h` JOIN `l1_token_metadata`:
- per (wallet, token, direction): `vol_24h = Σ |amount_raw / 10^decimals|`, `max_block_timestamp`
- per token: `total_volume_token = Σ |amount|` over BOTH legs (each transfer twice)
- `latest_price` = `close_price_usd` of `vw_token_price_usd_1d` at the max completed `bucket_date` within the last 7 days
- `latest_market_cap` = latest `vw_market_cap_1d` row (`total_supply` normalised, `total_market_cap = supply × close`; admission filters there)

| Column | Formula |
|---|---|
| `wallet_volume_usd_24h` | `vol_24h × COALESCE(close, 0)` |
| `total_supply`, `total_market_cap` | from market cap (NULL if not admitted) |
| `total_volume_token` | token 24h two-leg volume |
| `brenchmark_supply` | `total_supply × 0.01` |
| `brenchmark_volume_token` | `total_volume_token × 0.01` |
| `create_at` | query time |

Filter: `vol_24h >= total_volume_token × 0.01` (≥ 1 % of the token's 24h volume).

---

## 7. `vw_token_netflow_24h` (`0_vw_token_netflow_24h.sql`)

Window `[date_trunc('hour', now) − 24h, date_trunc('hour', now))`. Only `direction =
'in'` rows with `amount_raw IS NOT NULL`, JOIN metadata (`decimals` default 18),
INNER JOIN latest vwap (1m, last 3 days, `< date_trunc('hour', now)`; tokens without
one are dropped).

| Column | Formula |
|---|---|
| `inflow_usd_24h` | Σ `amount_raw / 10^decimals × vwap` over 'in' rows |
| `outflow_usd_24h` | the IDENTICAL expression (same 'in' rows) |
| `netflow_usd_24h` | constant `0.0` |
| `tx_in_count_24h`, `tx_out_count_24h` | `COUNT(*)` of the same rows |
| `latest_vwap_price_usd` | the latest vwap |

(The view deliberately reports total transfer volume at the latest VWAP; in/out are
symmetric by construction.) HyperCore port keeps the exact shape.

---

## 8. `vw_wallet_performance_30d` (`trino/views/all_chain/0_vw_wallet_performance_30d.sql`)

- `start_date = CURRENT_DATE − 30`, `end_date = CURRENT_DATE`
- `daily_nw`: Σ `closing_networth_usd` per (wallet, snapshot_date) across chains for `snapshot_date >= start_date`, `HAVING Σ > 10`
- `eth_daily`: WETH daily price = `MAX_BY(vwap, bucket_minute)` per day from `start − 1`; `r_eth = LN(p / LAG(p))`
- `r_wallet = LN(nw / NULLIF(LAG(nw) OVER (PARTITION BY wallet ORDER BY date), 0))`; LAG over the wallet's present days
- stats over rows with `r_wallet IS NOT NULL`, `HAVING COUNT >= 7`: `n_days`, `nw_start = MIN_BY(nw, date)`, `nw_end = MAX_BY(nw, date)`, `avg_return = AVG(r_wallet)`, `vol_daily = STDDEV(r_wallet)` (sample), `avg_eth_return = AVG(r_eth)`, `corr = COVAR_POP(r_wallet, r_eth) / NULLIF(STDDEV_POP(r_wallet) × STDDEV_POP(r_eth), 0)`

| Column | Formula |
|---|---|
| `as_of_date` | `end_date` (max date with a return) |
| `wallet_return_30d` | `(nw_end − nw_start) / NULLIF(nw_start, 0)` |
| `avg_annualized_return_30d` | `avg_return × 365` |
| `volatility_daily_30d` | `vol_daily` |
| `volatility_annualized_30d` | `vol_daily × √365` |
| `sharpe_ratio_annualized_30d` | `vol_daily > 0 ? avg_return / vol_daily × √365 : 0.0` |
| `correlation_with_eth_30d` | `corr` |
| `eth_return_30d` | `avg_eth_return × n_days` |
| `outperform_market_30d` | `avg_return × n_days > avg_eth_return × n_days` |
| `alpha_vs_eth_30d` | `(avg_return − avg_eth_return) × n_days` |
| `num_return_points_used` | `n_days` |

HyperCore (decision 3A): the ETH series is the UETH/USDC spot market candle
(`l1_token_metadata.symbol = 'UETH'`); chain-scoped networth (hypercore only).

---

## 9. `l4_token_wallet_sharpe_1d` (crate `services/rust/token-wallet-sharpe`)

Snapshot `snap = yesterday` by default. Inputs: per_tx for `[snap−30, snap]`, daily
prices for `[snap−37, snap]` (vwap at max bucket_minute of the day), `total_supply`,
first "in" ledger row per pair in the 30-day window, dense closing balance at
`snap − 7` and `snap − 30`.

Per pair (`core.rs:89-249`): daily closing balance = `balance_amount` of the last
row per day; `today_balance` = ASOF `<= snap` (skip if none or `<= 0`); `today_price`
= ASOF daily price (skip if none); `position_value_usd = bal × px`; value series over
`[snap−30, snap]` keeps only days with a price and `bal > 0` (need ≥ 2);
`log_returns` over consecutive kept days; `sharpe_ratio = mean / stddev_sample`
when `n >= 2 and stddev > 0`, else NULL (not annualised, no risk-free rate);
`value_change_pct_7d/30d = (today − past) / past × 100` only when `past >= 1.0 USD`;
`ownership_pct = today_balance / total_supply × 100` (0.0 when supply unknown);
`first_in_timestamp` = min 'in' timestamp in the window (NULL if older).

---

## 10. `l1_token_categories`

Columns: `token_address, token_group, category, source, created_at`. The only
`source` value in the atlas is `'moralis_rest'`. HyperCore (decision 4A):
`source = 'hypercore_rules'`, categories `stablecoin | native | wrapped | spot |
hip3-collateral`.

---

## Stamps and "today"

| Table | created_at | updated_at | partial day |
|---|---|---|---|
| balance_per_tx | source row | job clock | written as rows arrive |
| balance_1d_dense, networth_1d_dense | job clock | job clock | today is written partial, rewritten next run |
| FIFO tables | job clock | job clock | never folds today (ends at yesterday) |
| token_wallet_sharpe_1d | – | job clock | snapshot = yesterday |
| 24h views | `create_at = now()` | – | rolling windows at query time |
| performance_30d | – | – | includes today's networth row if present |

HyperCore rollups stamp `created_at = updated_at = run instant` on every rewritten
row (per-wallet recompute cannot preserve first-landing stamps; the atlas rebuilds
re-stamp too, and the ETL ignores `created_at` in its change comparison).

## 11. Token / market tables (v1.10.0, phases B and C)

Sources on Olympus (`~/blueblocx/atlas-project`): `trino/views/ethereum/0_vw_market_volume_24h.sql`,
`0_vw_token_active_address.sql`, `0_vw_token_new_rank.sql`, `0_vw_token_price_gainers.sql`,
`0_vw_token_stats.sql`, `1_vw_exchange_flow.sql`, `1_vw_market_cap_1d.sql`,
`1_vw_token_activity_trending.sql`, `1_vw_token_beta_volatility_sharp.sql`,
`2_vw_market_fear_index.sql`, `2_vw_token_summary.sql`; Rust `services/rust/market-signals/src/signals.rs`,
`services/rust/control-jobs/src/token_quality.rs`, `services/rust/control-jobs/src/dex_pair_volume.rs`.
Ports: `rollups/market.py`, `tokens.py`, `signals.py`, `quality.py`, `pairs.py`, `flows.py`.

| table | the atlas definition | HyperCore port and deviations |
|---|---|---|
| `market_volume_24h` | per complete hour of the last 24 (tip-anchored, the leading partial hour dropped): Σ amount and Σ amount × 1m price over the chain's `direction = 'out'` transfers | Σ `volume_token` / `volume_usd` of every token's **1h candle** (market-wide trade volume, the quantity the view approximates); the tip is the last closed 1h candle |
| `market_cap_latest` (`vw_market_cap_1d`) | closed days: `close × supply`, supply normalized (`total_supply_raw / 10^decimals`, else `total_supply` if < 1e15, ...), supply in [1, 1e15]; stables admitted when close in [0.5, 2]; others when close < 1e5, cap < 5e11, day volume >= $10k, >= 50 trades | identical SQL on the 1d candles; supply = Info `tokenDetails.totalSupply` (already in token units, `supply_source = 'hypercore:tokenDetails'`), refreshed daily per token by the price step. Tokens without a refresh yet (and USDC, which has no tokenDetails) are absent |
| `market_fear_index` (v3) | whitelisted tokens, 820 days; per token: ma30, ma90 (trailing means), winsorized log return ±50%, rv30 = sample stddev over 30 rows; per day cap-weighted momentum (`clamp(close/ma90 - 1, -0.95, 10)`), breadth (share of cap above ma30), rv, market volume, stable share of volume; volmom = avg30 / avg90 of market volume; each day scored by the **midrank percentile** of its own value within the trailing 730 calendar days (volatility and stable share inverted); needs >= 60 days; weights 30/20/20/15/15; profile bands 25/45/55/75 | identical arithmetic in python (`market.fear_index_rows`); stable volume on HyperCore is the pinned 1.0 rows (volume 0) so the market-composition factor is always tied → 50. The whole series is rewritten per run (point-in-time scores do not change) |
| `token_stats` | as-of-now price (latest 1m bucket <= now, vwap) and 24h change ((now − 24h-ago) / 24h-ago, **a fraction**, NULL when the same bucket resolves both sides or no 24h-old bucket within 30 days); all-time high / low / average over every 1m row; relative strength = change / WETH change; `Outperforming ETH` when > 1 | as-of prices from the 1m candles exactly; **all-time high / low / average from the 1d candles** (`max(high)`, `min(low)`, `sum(vwap)/count`) because the 1m table starts at v1.10.0 while the 1d history reaches 820 days; benchmark UETH |
| `token_price_gainers` | tip = max 1m bucket of the last 7 days; current = latest close <= tip (3-day scan), previous = latest close <= tip − 24h (30-day scan); `(cur − prev) / prev × 100`, > 0 only; ROW_NUMBER by pct desc, price desc; `timeframe = '24h'` | identical on the 1m candles |
| `token_new_rank` | tokens whose first 1m bucket is < 30 days old; as-of-now vwap and 24h-ago vwap (31-day scan); `tx_count_24h` = Σ `num_trades` over 31 days (despite the name); return capped [−100, 5000] (NULL → 0), newness = 1 − min(age, 30)/30, ln(1 + trades); min-max normalized; score 0.5/0.3/0.2; DENSE_RANK desc | the token's birth is its **deploy time** (`token_created_at` from tokenDetails); tokens without one (genesis tokens such as PURR, USDC) are not ranked. Trades = 1d candles of 31 days + today's 1m candles |
| `token_beta_volatility_sharp` | whitelisted tokens, the 30 days before today; daily close forward-filled within the window (0 before the first), log return 0 where either side is 0; beta = COVAR_POP / VAR_POP vs WETH; `desc_beta` Over / Lower / Same; annual volatility = sample STDDEV × √365; sharp = AVG / STDDEV | identical in python, benchmark UETH (must be whitelisted, as WETH must be in the view) |
| `token_activity_trending` | today = the last COMPLETE day of 1m data, yesterday = the day before; per token Σ trades, Σ volume_usd, Σ volume_token per day; prices = 1d closes; pct changes; RANK by tx change (yesterday >= 5 trades) and by volume change (yesterday >= $1k), NULLS LAST | identical, the day sums taken from the 1d candles (= the day's 1m sums) |
| `token_active_address` | distinct wallets with a ledger row per token in the last 24h, tokens in metadata; the ETL stamps `current_date AS snapshot_date` | identical over the **tracked** wallets' ledger |
| `token_summary` | `token_stats` ⟕ live market cap (price × supply with the cap gates) ⟕ yesterday's 1d volume ⟕ active wallets yesterday vs the day before (ledger) ⟕ trending ranks (activity_date = yesterday) ⟕ new rank ⟕ gainers; kept when priced within 3 days or present in a rank view or with yesterday's volume | identical; active wallets = tracked wallets |
| `token_market_signals` (Rust) | per token in bucket order: EMA9 / EMA21 seeded with the first close; spike ratio = volume / mean of the last 20 buckets (>= 2 = `spike`); resistance = max close of the last 20 buckets, `breakout` when close > resistance; one `global` row per minute (vwap of closes, totals, their spike ratios; ema 0, signals empty); 30 days of warm-up | identical on the 1m candles with 5 days of warm-up (EMA21 converges in ~100 minutes); the last 3 UTC days rewritten per run (`bucket_date` partitions); `price_breakout` stored as the Rust strings |
| `token_quality` (Rust, `v1_trades_jump`) | per token per day: Σ trades, max |vwap / previous vwap − 1| with the LAG crossing midnight; thresholds >= 10 trades, jump <= 0.80, lookback 24h; score 100 / 60 / 40 / 0; `fail_reason` = `low_trades:<n>`, `high_jump:<x>` joined by `,` | identical; the last 3 UTC days rewritten per run |
| `dex_pair_volume_1d` (Rust) | per (token pair, day) over all pools: amounts, USD volumes and swap counts split by direction (`is_token0_in`), `volume_usd` = both directions + unknown, `pool_count` | one HyperCore spot market = one pair `TOKEN/USDC` = one pool: `volume_usd` and `swap_count_total` from the market's 1d candle, **no direction split** (per-direction columns 0), `pair_address = <token_address>/<usdc address>`, `pair_symbol = SYMBOL/USDC`, `pool_count = 1`; the last 7 closed days rewritten per run |
| `exchange_flow` | daily stable volume into (`in`) and out of (`out`) the chain's WETH/stable pools, closed days of the last 31, `net = in − out` | the **tracked** wallets' bridge legs: `deposit` = incoming, `withdraw` = outgoing (USDC) |

Inputs added for this layer (price_step.py v1.10.0): `l3_token_price_usd_1m` (every priceable
token, `HL_PRICE_1M_DAYS` back on first visit, then the gap; stables pinned), 1d candles
`HL_PRICE_HISTORY_DAYS_1D` (820) back per token, Info `tokenDetails` per token every
`HL_SUPPLY_REFRESH_HOURS` (24) → `l1_token_metadata.total_supply`, `supply_as_of`,
`supply_source`, `supply_confidence`, `token_created_at`.
Unit-bridged assets (category `wrapped`: UETH, UBTC, USOL, ...) report their pre-minted pool as
`totalSupply`, so their supply is stored with `supply_confidence = 'low'` and skipped by market cap,
fear index and token summary (v1.10.2).
