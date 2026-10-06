"""
Canonical schema + shared helpers, kept compatible with the existing BlueBlocx
Iceberg lakehouse (see ../atlas-project/ICEBERG_SCHEMA.sql).

We only target the two tables the L4 PnL/portfolio layer actually depends on:

  l1_wallet_ledger      — signed token movements (spot + perp-cashflow as USDC)
  l3_token_price_usd_1m — 1-minute OHLCV USD price per token (what FIFO/networth read)

plus one small EXTENSION table for perp open positions (unrealized PnL / margin),
which the spot/token balance model cannot represent:

  l4x_perp_position_snapshot — point-in-time perp positions from clearinghouseState

CSV columns match the Iceberg DDL column order exactly so these files lift into
Iceberg unchanged. Timestamps are emitted as ISO-8601 UTC strings.

Verified facts this module encodes (see memory reference-hyperliquid-data-sources):
  - group hash = xxh64(addr, seed=0).abs() % 4000   (Rust production formula,
    services/rust/parse-raw/src/wallet_ledger/tables.rs::addr_group)
  - canonical l1 event_type values are lowercase {mint, burn, transfer}; we ADD
    perp/spot-accounting types below (safe: event_type is a partition column,
    FIFO keys off amount_signed, watchlist counts only 'transfer').
"""
from __future__ import annotations

import xxhash

# --- namespace -----------------------------------------------------------------
# TWO namespaces, mirroring the ethereum/arbitrum convention (see PROPOSED_SCHEMA):
# HyperCore and HyperEVM are SEPARATE namespaces. This module targets HyperCore.
# The raw/L0 layer differs per source; L1+L3 share the exact ethereum DDL so the
# L4 balance/FIFO/networth layer runs unchanged on top.
NS_HYPERCORE = "hypercore"
NS_HYPEREVM = "hyperevm"
# Back-compat alias: earlier POC output landed under out/hyperliquid/. Kept so a
# consumer of the old path doesn't break; new writes go to NS_HYPERCORE.
NS_HYPERLIQUID = "hyperliquid"

# Source tag for provenance in logs/reconciliation (NOT a ledger column — the
# row's source is already implied by event_type).
SRC_CORE = "hypercore"
SRC_EVM = "hyperevm"

# --- standard column -----------------------------------------------------------
# CRITICAL: downstream consumers (FIFO PnL, balance, networth, statement/watchlist)
# hard-gate rows on standard == "ERC20". HyperCore has no ERC-20 standard, but to
# pass those gates every HyperCore ledger row MUST carry standard="ERC20". The HL
# coin symbol is stored in l1_token_metadata.symbol instead (NOT in `standard`).
STANDARD_ERC20 = "ERC20"

# --- USDC on HyperCore ---------------------------------------------------------
# HyperCore quotes spot in USDC (token index 0). There is no real ERC-20 address
# on HyperCore itself, so we use a synthetic stable "address" as the ledger's
# token_address for all USDC-denominated cashflow. USDC price is pinned to 1.0.
USDC_TOKEN = "USDC"
USDC_ADDRESS = "hlcore:usdc"          # synthetic, stable across runs
USDC_DECIMALS = 6                      # HyperCore USDC display precision

# --- event_type taxonomy -------------------------------------------------------
# Canonical (existing pipeline): only these appear on EVM chains.
EVT_TRANSFER = "transfer"
EVT_MINT = "mint"
EVT_BURN = "burn"

# EXTENSION types for HyperCore accounting. All are USDC-denominated cashflows
# except spot fills (which move the base token + USDC legs).
EVT_SPOT_FILL = "spot_fill"            # spot trade leg (base token in/out)
EVT_PERP_PNL = "perp_realized_pnl"     # closedPnl on a reducing perp fill (USDC)
EVT_PERP_FEE = "fee"                   # trading fee (USDC, always out)
EVT_FUNDING = "funding"                # funding payment/receipt (USDC)
EVT_DEPOSIT = "deposit"                # ledger: onto HyperCore (USDC in)
EVT_WITHDRAW = "withdraw"              # ledger: off HyperCore (USDC out)
EVT_TRANSFER_INTERNAL = "transfer"     # account/sub-account/vault moves -> transfer
EVT_STAKING_REWARD = "staking_reward"  # HYPE staking income (delegator rewards)
EVT_INTEREST = "interest"              # lending interest (borrowLend.interestAmount)

DIRECTION_IN = "in"
DIRECTION_OUT = "out"

# --- event_type classification (used by rollups; keep in ONE place) ------------
# Perp cashflow types: USDC rows that ARE realized PnL. Must be EXCLUDED from the
# spot FIFO engine and from net_flow, and SUMMED into realized-PnL rollups.
PERP_CASHFLOW_TYPES = frozenset({EVT_PERP_PNL, EVT_FUNDING, EVT_PERP_FEE})
# Realized-PnL contributors at L4 (perp side). Staking reward is income too.
REALIZED_PNL_EVENT_TYPES = frozenset(
    {EVT_PERP_PNL, EVT_FUNDING, EVT_PERP_FEE, EVT_STAKING_REWARD, EVT_INTEREST})
# Capital-flow types (deposit/withdraw the account's own capital). These are the
# ONLY types net_flow may sum — perp PnL/fee/funding are NOT flow.
FLOW_EVENT_TYPES = frozenset({EVT_DEPOSIT, EVT_WITHDRAW, EVT_TRANSFER})
# Types the spot FIFO / balance engine should treat as token movements.
SPOT_BALANCE_EVENT_TYPES = frozenset(
    {EVT_SPOT_FILL, EVT_TRANSFER, EVT_MINT, EVT_BURN})

ZERO_HASH = "0x" + "0" * 64

# --- CSV column orders (must match ICEBERG_SCHEMA.sql column order) -------------

# ICEBERG_SCHEMA.sql:115-135  nessie.<chain>.l1_wallet_ledger
L1_WALLET_LEDGER_COLUMNS = [
    "block_number",
    "block_timestamp",
    "block_date",
    "tx_hash",
    "tx_index",
    "event_index",
    "event_type",
    "wallet_address",
    "wallet_group",
    "counterparty_address",
    "token_address",
    "token_group",
    "direction",
    "amount_raw",
    "decimals",
    "standard",
    "amount",
    "amount_signed",
    "created_at",
    "updated_at",
]

# ICEBERG_SCHEMA.sql:321-340  nessie.<chain>.l3_token_price_usd_1m
L3_TOKEN_PRICE_USD_1M_COLUMNS = [
    "token_address",
    "token_group",
    "bucket_minute",
    "bucket_date",
    "open_price_usd",
    "high_price_usd",
    "low_price_usd",
    "close_price_usd",
    "vwap_price_usd",
    "volume_token",
    "volume_usd",
    "num_trades",
    "created_at",
    "updated_at",
]

# ICEBERG_SCHEMA.sql:143-165  nessie.<chain>.l1_token_metadata
# We emit the minimal subset needed for: symbol resolution (moved off `standard`),
# decimals, and is_stablecoin (so stable_coin_pct computes). Remaining columns are
# left empty and lift fine (all nullable).
L1_TOKEN_METADATA_COLUMNS = [
    "token_address",
    "token_group",
    "standard",
    "symbol",
    "name",
    "decimals",
    "first_seen",
    "source",
    "logo_url",
    "total_supply_raw",
    "total_supply",
    "supply_as_of",
    "supply_source",
    "supply_confidence",
    "category",
    "is_stablecoin",
    "is_native_wrapper",
    "coingecko_id",
    "token_created_at",
    "is_watchlist",
    "created_at",
    "updated_at",
]

# ICEBERG_SCHEMA.sql:710-720  nessie.<chain>.l4_wallet_metadata
L4_WALLET_METADATA_COLUMNS = [
    "wallet_address",
    "first_block_timestamp",
    "latest_block_timestamp",
    "count_tx",
    "first_tx",
    "latest_tx",
    "active_days",
    "last_active",
    "active_on_avg",
    "updated_at",
]

# ICEBERG_SCHEMA.sql:485-491  nessie.<chain>.wallet_watchlist  (consumer gate)
WALLET_WATCHLIST_COLUMNS = [
    "wallet_address", "wallet_group", "is_active", "whitelist_reason",
    "created_at", "updated_at",
]

# ICEBERG_SCHEMA.sql:450-456  nessie.<chain>.token_whitelist  (consumer gate)
TOKEN_WHITELIST_COLUMNS = [
    "token_address", "token_group", "is_active", "whitelist_reason",
    "created_at", "updated_at",
]

# EXTENSION table (not in current DDL) — perp open positions for unrealized PnL.
# Proposed DDL lives in docs/PROPOSED_SCHEMA.sql. Keyed (as_of, wallet, coin).
L4X_PERP_POSITION_SNAPSHOT_COLUMNS = [
    "as_of_ts",
    "as_of_date",
    "wallet_address",
    "wallet_group",
    "coin",
    "dex",                 # main | HIP-3 dex name (xyz, flx, ...); margin accounts are per dex
    "szi",                 # signed size (+long / -short); NOT a token balance
    "entry_px",
    "position_value",
    "mark_px",             # derived: |position_value / szi|
    "unrealized_pnl_usd",
    "return_on_equity",
    "liquidation_px",
    "margin_used_usd",
    "leverage_type",       # cross | isolated
    "leverage_value",
    "cum_funding_all_time",
    "cum_funding_since_open",
    "account_value_usd",   # marginSummary.accountValue (whole-account, repeated)
    "withdrawable_usd",
    "created_at",
]


def addr_group(addr: str) -> int:
    """Deterministic shard 0..3999 — matches Rust production addr_group().

    services/rust/parse-raw/src/wallet_ledger/tables.rs:
        let h = xxh64(addr.as_bytes(), 0) as i64; h.wrapping_abs() % 4000
    xxh64 returns u64; casting to i64 then abs()%4000 is equivalent to
    (unsigned_digest interpreted as signed).abs() % 4000.
    """
    if addr is None:
        return 0
    u = xxhash.xxh64(addr.encode("utf-8"), seed=0).intdigest()  # u64
    signed = u - (1 << 64) if u >= (1 << 63) else u             # reinterpret as i64
    return abs(signed) % 4000


# --- natural key scheme (content-addressed, WALLET-SCOPED) ----------------------
# Downstream Rust consumers dedup on (tx_hash, event_index[, direction]) WITHOUT
# wallet_address (parse-raw core.rs existing_keys; fifo-pnl tables.rs seen_keys).
# Any surrogate that is not wallet-scoped therefore collides across wallets and
# silently drops rows on multi-wallet ingest. Every HyperCore tx_hash is prefixed
# "hlcore:{wallet}:" so the key is unique chain-wide, and event_index is a FIXED
# per-leg ordinal (never an iteration counter) so re-runs, window shifts and the
# Info-API -> S3 node_fills source switch all produce byte-identical keys.

# fill legs (tx_hash keyed by trade id `tid`, unique per fill)
LEG_PERP_PNL = 0
LEG_PERP_FEE = 1
LEG_PERP_BUILDER_FEE = 2
LEG_SPOT_BASE = 0
LEG_SPOT_QUOTE = 1
LEG_SPOT_FEE = 2
LEG_SPOT_BUILDER_FEE = 3   # distinct from LEG_SPOT_FEE so a spot fill with both fees never collides
# funding: one row per (coin, hour)
LEG_FUNDING = 0
# ledger updates: single leg, except internal perp<->spot which nets via two legs
LEG_LEDGER = 0
LEG_LEDGER_INTERNAL_OUT = 0
LEG_LEDGER_INTERNAL_IN = 1
LEG_LEDGER_INTEREST = 2    # borrowLend interest income, alongside the two internal legs


def _w(wallet: str) -> str:
    return (wallet or "").lower()


def fill_tx_hash(wallet: str, tid, *, time_ms, coin: str, sz) -> str:
    """tx_hash for a fill. `tid` is HL's unique trade id. Dust conversions carry
    tid=0 and the zero hash, so they fall back to a (time, coin, size) key —
    without this, all of a wallet's dust fills collapse into one tx_hash."""
    if tid:
        return f"hlcore:{_w(wallet)}:tid:{tid}"
    return f"hlcore:{_w(wallet)}:dust:{time_ms}:{coin}:{sz}"


def funding_tx_hash(wallet: str, coin: str, time_ms) -> str:
    """Funding entries carry no hash; (wallet, coin, hour) identifies one."""
    return f"hlcore:{_w(wallet)}:funding:{coin}:{time_ms}"


def ledger_tx_hash(wallet: str, hash_val, typ: str, time_ms) -> str:
    """Ledger updates usually carry a real L1 hash — but a `send` shows the SAME
    hash on both sender and receiver, so even real hashes must be wallet-scoped.
    The delta type is appended so two deltas in one tx stay distinct."""
    if hash_val and hash_val != ZERO_HASH:
        return f"hlcore:{_w(wallet)}:{hash_val}:{typ}"
    return f"hlcore:{_w(wallet)}:{typ}:{time_ms}"
