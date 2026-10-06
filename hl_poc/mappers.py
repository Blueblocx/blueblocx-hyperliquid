"""
Map raw Hyperliquid Info-API records into l1_wallet_ledger rows.

Design (see memory hypercore-pnl-model — "USDC cashflow + snapshot"):

A perp position is NOT a token balance, so we DO NOT put perp size into the
ledger. Instead every perp fill contributes only its USDC cashflow legs:

  - closedPnl  -> one signed USDC ledger row (event_type perp_realized_pnl)
  - fee        -> one USDC 'out' row (event_type fee), skipped when zero
  - funding    -> one signed USDC row per funding event (event_type funding)

Because Hyperliquid gives realized PnL directly per fill (closedPnl), we do NOT
need FIFO to reconstruct perp PnL — these USDC rows already ARE realized PnL.

SPOT fills move the base token (event_type spot_fill) and the QUOTE counter-leg
(usually USDC, but ~5% of pairs quote in another token — resolved via spotMeta),
mirroring the two-row in/out convention of the EVM ledger.

CRITICAL invariants encoded here (see hypercore-completeness-audit):
  - EVERY row carries standard="ERC20" so downstream consumers that gate on it
    do not drop HyperCore data. The HL symbol goes into l1_token_metadata.
  - tx_hash is WALLET-SCOPED and content-addressed (schema.fill_tx_hash /
    funding_tx_hash / ledger_tx_hash); event_index is a FIXED per-leg ordinal
    (schema.LEG_*). No iteration counters anywhere — so re-runs are idempotent,
    multi-wallet ingest never collides on the Rust (tx_hash, event_index) dedup
    key, and the Info-API -> S3 node_fills switch yields identical keys.
  - internal perp<->spot moves (accountClassTransfer) net to ZERO via two legs.

Sign conventions verified against live data:
  fill.side "B"=buy/"A"=sell; fill.closedPnl signed; fill.fee positive=cost;
  funding.delta.usdc signed (+received / -paid).
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

from . import schema as S
from .spot_meta import SpotMetaResolver, TokenInfo

log = logging.getLogger("hl_poc.mappers")


def _iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()


def _date(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")


def _is_spot(coin: str) -> bool:
    # Spot coins are named "BASE/USDC" (canonical) or "@index" (non-canonical).
    return "/" in coin or coin.startswith("@")


class TokenRegistry:
    """Accumulates token identity/metadata seen while mapping, so extract.py can
    emit l1_token_metadata. Keyed by token_address."""

    def __init__(self) -> None:
        self._by_addr: dict[str, dict] = {}

    def note(self, token_address: str, symbol: str, decimals: int,
             is_stablecoin: bool) -> None:
        cur = self._by_addr.get(token_address)
        if cur is None or (decimals and not cur.get("decimals")):
            self._by_addr[token_address] = {
                "symbol": symbol,
                "decimals": int(decimals or 0),
                "is_stablecoin": bool(is_stablecoin),
            }

    def note_token(self, t: TokenInfo) -> None:
        self.note(t.token_address, t.symbol, t.decimals, t.is_stablecoin)

    def items(self) -> dict[str, dict]:
        return dict(self._by_addr)


def _ledger_row(*, block_timestamp_ms, tx_hash, event_index, event_type,
                wallet, counterparty, token_address, token_symbol, decimals,
                amount, direction, now_iso, registry: TokenRegistry | None = None,
                is_stablecoin: bool = False) -> dict:
    """Build one l1_wallet_ledger row. `standard` is ALWAYS "ERC20"."""
    signed = amount if direction == S.DIRECTION_IN else -amount
    if registry is not None:
        registry.note(token_address, token_symbol, decimals, is_stablecoin)
    wl = wallet.lower()
    return {
        "block_number": None,                 # HyperCore has no EVM block number
        "block_timestamp": _iso(block_timestamp_ms),
        "block_date": _date(block_timestamp_ms),
        "tx_hash": tx_hash,
        "tx_index": None,
        "event_index": int(event_index),
        "event_type": event_type,
        "wallet_address": wl,
        "wallet_group": S.addr_group(wl),
        "counterparty_address": (counterparty or "").lower() or None,
        "token_address": token_address,
        "token_group": S.addr_group(token_address),
        "direction": direction,
        "amount_raw": float(amount) * (10 ** int(decimals or 0)),
        "decimals": int(decimals or 0),
        "standard": S.STANDARD_ERC20,          # gate-passing; NOT the symbol
        "amount": float(amount),
        "amount_signed": float(signed),
        "created_at": now_iso,
        "updated_at": now_iso,
    }


def _token_tuple(ti: TokenInfo | None, fallback_symbol: str):
    """(address, symbol, decimals, is_stable) for a resolved token or fallback."""
    if ti is not None:
        return ti.token_address, ti.symbol, ti.decimals, ti.is_stablecoin
    return f"hlcore:spot:{fallback_symbol}", fallback_symbol, 0, False


_USDC_TUPLE = (S.USDC_ADDRESS, S.USDC_TOKEN, S.USDC_DECIMALS, True)


def perp_cash_token(coin: str, collateral_by_prefix: dict | None) -> tuple:
    """(address, symbol, decimals, is_stable) that a perp coin's PnL/funding are
    denominated in. Main-dex coins -> USDC. HIP-3 coins 'dex:COIN' -> that dex's
    collateral token (USDH/USDE/USDT0/...), resolved from meta(dex) by the caller."""
    if collateral_by_prefix and ":" in coin:
        ti = collateral_by_prefix.get(coin.split(":", 1)[0] + ":")
        if ti is not None:
            return ti.token_address, ti.symbol, ti.decimals, ti.is_stablecoin
    return _USDC_TUPLE


def map_fills_to_ledger(fills: list[dict], wallet: str, now_iso: str,
                        resolver: SpotMetaResolver, registry: TokenRegistry,
                        collateral_by_prefix: dict | None = None) -> list[dict]:
    """Perp + spot fills -> collateral cashflow rows (+ spot base/quote token legs).

    tx_hash = schema.fill_tx_hash (wallet + tid); event_index = fixed LEG_* ordinal.
    """
    rows: list[dict] = []

    for f in fills:
        coin = f["coin"]
        t = f["time"]
        h = S.fill_tx_hash(wallet, f.get("tid"), time_ms=t, coin=coin, sz=f.get("sz"))
        fee = float(f.get("fee", 0) or 0)
        # builderFee / deployerFee: separate deductions present in node data (absent
        # on the public Info API today). Folded into ONE builder-fee leg.
        builder_fee = (float(f.get("builderFee", 0) or 0)
                       + float(f.get("deployerFee", 0) or 0))
        closed = float(f.get("closedPnl", 0) or 0)
        fee_token = f.get("feeToken", S.USDC_TOKEN)
        cash = perp_cash_token(coin, collateral_by_prefix)

        if _is_spot(coin):
            pair = resolver.resolve_coin(coin)
            sz = float(f["sz"])
            px = float(f["px"])
            quote_notional = sz * px
            buy = f.get("side") == "B"

            if pair is not None:
                base_t = _token_tuple(pair.base, pair.base.symbol)
                quote_t = _token_tuple(pair.quote, pair.quote.symbol)
                registry.note_token(pair.base)
                registry.note_token(pair.quote)
            else:
                base_sym = coin.split("/")[0] if "/" in coin else coin
                base_t = _token_tuple(None, base_sym)
                quote_t = _USDC_TUPLE
                log.warning("spot coin %s not in spotMeta — decimals=0 fallback", coin)

            rows.append(_ledger_row(
                block_timestamp_ms=t, tx_hash=h, event_index=S.LEG_SPOT_BASE,
                event_type=S.EVT_SPOT_FILL, wallet=wallet, counterparty=None,
                token_address=base_t[0], token_symbol=base_t[1], decimals=base_t[2],
                amount=sz, direction=S.DIRECTION_IN if buy else S.DIRECTION_OUT,
                now_iso=now_iso, registry=registry, is_stablecoin=base_t[3]))
            rows.append(_ledger_row(
                block_timestamp_ms=t, tx_hash=h, event_index=S.LEG_SPOT_QUOTE,
                event_type=S.EVT_SPOT_FILL, wallet=wallet, counterparty=None,
                token_address=quote_t[0], token_symbol=quote_t[1], decimals=quote_t[2],
                amount=quote_notional,
                direction=S.DIRECTION_OUT if buy else S.DIRECTION_IN,
                now_iso=now_iso, registry=registry, is_stablecoin=quote_t[3]))
            fee_leg = S.LEG_SPOT_FEE
        else:
            if closed != 0.0:
                rows.append(_ledger_row(
                    block_timestamp_ms=t, tx_hash=h, event_index=S.LEG_PERP_PNL,
                    event_type=S.EVT_PERP_PNL, wallet=wallet, counterparty=coin,
                    token_address=cash[0], token_symbol=cash[1],
                    decimals=cash[2], amount=abs(closed),
                    direction=S.DIRECTION_IN if closed > 0 else S.DIRECTION_OUT,
                    now_iso=now_iso, registry=registry, is_stablecoin=cash[3]))
            fee_leg = S.LEG_PERP_FEE

        # Fee leg. HL charges the fee in the RECEIVED asset: USDC for perps and
        # spot sells, the BASE token for spot buys — emit it in its real token.
        if fee != 0.0:
            if fee_token == S.USDC_TOKEN:
                ft = _USDC_TUPLE
            else:
                ti = resolver.resolve_symbol(fee_token)
                ft = _token_tuple(ti, fee_token)
                if ti is not None:
                    registry.note_token(ti)
                else:
                    log.warning("fee token %s not in spotMeta — decimals=0", fee_token)
            rows.append(_ledger_row(
                block_timestamp_ms=t, tx_hash=h, event_index=fee_leg,
                event_type=S.EVT_PERP_FEE, wallet=wallet, counterparty=coin,
                token_address=ft[0], token_symbol=ft[1], decimals=ft[2],
                amount=abs(fee), direction=S.DIRECTION_OUT if fee > 0 else S.DIRECTION_IN,
                now_iso=now_iso, registry=registry, is_stablecoin=ft[3]))

        if builder_fee != 0.0:
            rows.append(_ledger_row(
                block_timestamp_ms=t, tx_hash=h,
                event_index=(S.LEG_SPOT_BUILDER_FEE if _is_spot(coin)
                             else S.LEG_PERP_BUILDER_FEE),
                event_type=S.EVT_PERP_FEE, wallet=wallet, counterparty=coin,
                token_address=cash[0], token_symbol=cash[1],
                decimals=cash[2], amount=abs(builder_fee),
                direction=S.DIRECTION_OUT if builder_fee > 0 else S.DIRECTION_IN,
                now_iso=now_iso, registry=registry, is_stablecoin=cash[3]))

    return rows


def map_funding_to_ledger(funding: list[dict], wallet: str, now_iso: str,
                          registry: TokenRegistry,
                          collateral_by_prefix: dict | None = None) -> list[dict]:
    """Funding events -> signed collateral rows (USDC on the main dex; the dex's
    collateral token for HIP-3 coins). delta.usdc is already signed.
    tx_hash = schema.funding_tx_hash(wallet, coin, time); event_index = LEG_FUNDING."""
    rows: list[dict] = []
    for e in funding:
        d = e["delta"]
        usdc = float(d.get("usdc", 0) or 0)
        if usdc == 0.0:
            continue
        coin = d.get("coin") or ""
        cash = perp_cash_token(coin, collateral_by_prefix)
        rows.append(_ledger_row(
            block_timestamp_ms=e["time"],
            tx_hash=S.funding_tx_hash(wallet, coin, e["time"]),
            event_index=S.LEG_FUNDING,
            event_type=S.EVT_FUNDING, wallet=wallet, counterparty=coin,
            token_address=cash[0], token_symbol=cash[1],
            decimals=cash[2], amount=abs(usdc),
            direction=S.DIRECTION_IN if usdc > 0 else S.DIRECTION_OUT,
            now_iso=now_iso, registry=registry, is_stablecoin=cash[3]))
    return rows


# Ledger delta.type -> canonical event_type. Enum confirmed live (2026-07-28):
#   deposit/withdraw/accountClassTransfer/spotTransfer/send/internalTransfer/
#   subAccountTransfer/vaultCreate/vaultDeposit/vaultWithdraw/spotGenesis/borrowLend
# Unknown types fall back to transfer AND are surfaced to the caller (alerting).
_LEDGER_EVENT_TYPE = {
    "deposit": S.EVT_DEPOSIT,              # onto HyperCore from the bridge (flow)
    "withdraw": S.EVT_WITHDRAW,            # off HyperCore to the bridge (flow)
    "vaultDeposit": S.EVT_TRANSFER,        # USDC into a vault (own capital move)
    "vaultWithdraw": S.EVT_TRANSFER,       # USDC back from a vault
    "vaultCreate": S.EVT_TRANSFER,         # seed a vault
    "send": S.EVT_TRANSFER,                # spot token send (in or out)
    "spotTransfer": S.EVT_TRANSFER,        # spot token transfer (in or out)
    "spotGenesis": S.EVT_TRANSFER,         # genesis token distribution
    "internalTransfer": S.EVT_TRANSFER,
    "accountClassTransfer": S.EVT_TRANSFER,  # perp<->spot INTERNAL — net-zero below
    "cStakingTransfer": S.EVT_TRANSFER,    # spot<->staking INTERNAL — net-zero below
    "subAccountTransfer": S.EVT_TRANSFER,
    "borrowLend": S.EVT_TRANSFER,          # spot borrow/lend move
}

# Moves that stay within the wallet's OWN books (perp<->spot, spot<->staking, and
# a self-`send`/`spotTransfer` between dexes where user == destination == wallet).
# They are emitted as TWO opposite legs so the wallet nets to zero — never as a
# one-sided inflow (which would inflate net_flow and balances).
_NET_ZERO_INTERNAL_TYPES = {"accountClassTransfer", "cStakingTransfer", "borrowLend"}


def _is_self_move(d: dict, wl: str) -> bool:
    user = (d.get("user") or "").lower()
    dest = (d.get("destination") or "").lower()
    return bool(user) and user == wl and dest == wl


def _resolve_ledger_delta(d: dict, wallet: str, resolver: SpotMetaResolver) -> tuple:
    """Return (token_symbol, token_address, decimals, amount, direction, stable)
    for a ledger delta, handling the several field shapes HL uses."""
    typ = d.get("type")
    wl = wallet.lower()

    token = d.get("token")
    if token and token != S.USDC_TOKEN and d.get("amount") is not None:
        amount = abs(float(d["amount"]))
        addr, sym, dec, stable = _token_tuple(resolver.resolve_symbol(token), token)
        user = (d.get("user") or "").lower()
        dest = (d.get("destination") or "").lower()
        if wl == user and wl != dest:
            direction = S.DIRECTION_OUT
        elif wl == dest and wl != user:
            direction = S.DIRECTION_IN
        else:
            direction = S.DIRECTION_OUT  # self/ambiguous: treat as outflow
        return sym, addr, dec, amount, direction, stable

    usdc = d.get("usdc")
    if usdc is None:
        usdc = d.get("usdcValue")
    if usdc is None:
        usdc = d.get("requestedUsd")
    if usdc is None:
        # borrowLend / cStakingTransfer-style deltas carry a plain `amount` for a
        # USDC token with no usdc* field — without this fallback they were dropped.
        usdc = d.get("amount")
    amt = float(usdc or 0)

    if typ in ("deposit", "vaultWithdraw"):
        direction = S.DIRECTION_IN
    elif typ in ("withdraw", "vaultDeposit", "vaultCreate"):
        direction = S.DIRECTION_OUT
    elif typ == "accountClassTransfer":
        direction = S.DIRECTION_OUT if d.get("toPerp") else S.DIRECTION_IN
    else:
        user = (d.get("user") or "").lower()
        dest = (d.get("destination") or "").lower()
        if wl == dest and wl != user:
            direction = S.DIRECTION_IN
        elif wl == user and wl != dest:
            direction = S.DIRECTION_OUT
        else:
            direction = S.DIRECTION_IN if amt >= 0 else S.DIRECTION_OUT
    return S.USDC_TOKEN, S.USDC_ADDRESS, S.USDC_DECIMALS, abs(amt), direction, True


def map_non_funding_ledger(updates: list[dict], wallet: str, now_iso: str,
                           resolver: SpotMetaResolver, registry: TokenRegistry
                           ) -> tuple[list[dict], dict]:
    """Deposits/withdrawals/transfers -> ledger rows (USDC or spot token).

    tx_hash = schema.ledger_tx_hash(wallet, hash, type, time). Internal
    perp<->spot moves emit TWO legs (ordinals 0 out / 1 in) so the wallet nets
    to zero. Returns (rows, unknown_type_counts) for alerting on unmapped types.
    """
    rows: list[dict] = []
    unknown: dict[str, int] = {}
    wl = wallet.lower()
    for e in updates:
        d = e["delta"]
        typ = d.get("type")
        event_type = _LEDGER_EVENT_TYPE.get(typ)
        if event_type is None:
            unknown[typ] = unknown.get(typ, 0) + 1
            event_type = S.EVT_TRANSFER
        symbol, token_addr, decimals, amount, direction, stable = \
            _resolve_ledger_delta(d, wallet, resolver)
        if amount == 0.0:
            continue
        h = S.ledger_tx_hash(wallet, e.get("hash"), typ, e["time"])

        if typ in _NET_ZERO_INTERNAL_TYPES or (
                typ in ("send", "spotTransfer") and _is_self_move(d, wl)):
            for leg, dirn in ((S.LEG_LEDGER_INTERNAL_OUT, S.DIRECTION_OUT),
                              (S.LEG_LEDGER_INTERNAL_IN, S.DIRECTION_IN)):
                rows.append(_ledger_row(
                    block_timestamp_ms=e["time"], tx_hash=h, event_index=leg,
                    event_type=S.EVT_TRANSFER, wallet=wallet,
                    counterparty=typ,
                    token_address=token_addr, token_symbol=symbol,
                    decimals=decimals, amount=amount, direction=dirn,
                    now_iso=now_iso, registry=registry, is_stablecoin=stable))
            # lending interest is real income (realized), emitted as its own leg
            interest = float(d.get("interestAmount", 0) or 0)
            if interest != 0.0:
                rows.append(_ledger_row(
                    block_timestamp_ms=e["time"], tx_hash=h,
                    event_index=S.LEG_LEDGER_INTEREST, event_type=S.EVT_INTEREST,
                    wallet=wallet, counterparty=typ,
                    token_address=token_addr, token_symbol=symbol,
                    decimals=decimals, amount=abs(interest),
                    direction=S.DIRECTION_IN if interest > 0 else S.DIRECTION_OUT,
                    now_iso=now_iso, registry=registry, is_stablecoin=stable))
            continue

        if direction == S.DIRECTION_IN:
            counterparty = d.get("user") or d.get("vault")
        else:
            counterparty = d.get("destination") or d.get("vault")
        if not counterparty or counterparty.lower() == wl:
            counterparty = d.get("vault") or typ
        rows.append(_ledger_row(
            block_timestamp_ms=e["time"], tx_hash=h, event_index=S.LEG_LEDGER,
            event_type=event_type, wallet=wallet, counterparty=counterparty,
            token_address=token_addr, token_symbol=symbol,
            decimals=decimals, amount=amount, direction=direction,
            now_iso=now_iso, registry=registry, is_stablecoin=stable))
    return rows, unknown


def natural_key(r: dict) -> tuple:
    """The dedup key downstream Rust uses (NO wallet). Must be unique chain-wide."""
    return (r["tx_hash"], r["event_index"], r["direction"])
