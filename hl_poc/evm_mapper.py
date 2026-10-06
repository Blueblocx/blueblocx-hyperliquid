"""
Map HyperEVM ERC-20 Transfer logs into l1_wallet_ledger rows.

Mirrors the atlas production parser (services/rust/parse-raw/src/wallet_ledger/
core.rs) so HyperEVM rows are byte-compatible with HyperCore rows in the same
l1_wallet_ledger:
  - event_type: mint (from 0x0) / burn (to 0x0) / transfer
  - each Transfer -> TWO rows: 'out' (wallet=from) and 'in' (wallet=to)
  - wallet_group/token_group = addr_group() (xxh64 seed=0 % 4000)
  - amount_raw = uint256(data); amount = amount_raw / 10^decimals (decimals from
    a token-metadata lookup; None until resolved -> amount left null)

For the POC we only keep the row whose wallet_address == the wallet we queried
(the getLogs filter already guarantees the wallet is on one side), so we emit a
single directional row per log rather than both sides — the counterparty is the
other address. This keeps the per-wallet dump focused; the full both-rows form is
what a whole-chain scan would produce.
"""
from __future__ import annotations

from datetime import datetime, timezone

from . import schema as S

ZERO_ADDR = "0x0000000000000000000000000000000000000000"


def _iso(unix_s: int) -> str:
    return datetime.fromtimestamp(unix_s, tz=timezone.utc).isoformat()


def _date(unix_s: int) -> str:
    return datetime.fromtimestamp(unix_s, tz=timezone.utc).strftime("%Y-%m-%d")


def _topic_to_addr(topic: str) -> str:
    """32-byte topic -> 0x + last 40 hex chars (20-byte address)."""
    return "0x" + topic[-40:]


def _uint256(data_hex: str) -> float:
    h = (data_hex or "0x0").replace("0x", "") or "0"
    # a Transfer's data is a single 32-byte word; take the first word if longer
    word = h[:64] if len(h) >= 64 else h
    return float(int(word, 16)) if word else 0.0


def event_type_for(from_addr: str, to_addr: str) -> str:
    if from_addr == ZERO_ADDR:
        return S.EVT_MINT
    if to_addr == ZERO_ADDR:
        return S.EVT_BURN
    return S.EVT_TRANSFER


def map_evm_transfers_to_ledger(
    logs: list[dict],
    wallet: str,
    block_ts: dict[int, int],
    token_meta: dict[str, dict],
    now_iso: str,
) -> list[dict]:
    """logs: raw eth_getLogs entries. block_ts: {block_number: unix_seconds}.
    token_meta: {token_address_lower: {"decimals": int, "symbol": str}}.

    Emits the wallet's own directional row per Transfer (see module docstring).
    """
    wallet = wallet.lower()
    rows: list[dict] = []
    seen: set[tuple] = set()

    for lg in logs:
        topics = lg.get("topics", [])
        if len(topics) < 3 or topics[0].lower() != \
                "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef":
            continue
        from_addr = _topic_to_addr(topics[1]).lower()
        to_addr = _topic_to_addr(topics[2]).lower()
        token_addr = lg.get("address", "").lower()
        block_num = int(lg["blockNumber"], 16)
        log_index = int(lg["logIndex"], 16)
        tx_hash = lg.get("transactionHash", "")
        amount_raw = _uint256(lg.get("data", "0x0"))

        # which side is our wallet? emit that directional row.
        if wallet == to_addr:
            direction, counterparty = S.DIRECTION_IN, from_addr
        elif wallet == from_addr:
            direction, counterparty = S.DIRECTION_OUT, to_addr
        else:
            continue  # shouldn't happen given the topic filter

        key = (tx_hash, log_index, direction)
        if key in seen:
            continue
        seen.add(key)

        meta = token_meta.get(token_addr, {})
        decimals = meta.get("decimals")
        amount = (amount_raw / (10 ** decimals)) if decimals is not None else None
        signed = None
        if amount is not None:
            signed = amount if direction == S.DIRECTION_IN else -amount

        ts = block_ts.get(block_num, 0)
        tx_index = lg.get("transactionIndex")
        rows.append({
            "block_number": block_num,
            "block_timestamp": _iso(ts),
            "block_date": _date(ts),
            "tx_hash": tx_hash,
            "tx_index": int(tx_index, 16) if isinstance(tx_index, str) else tx_index,
            "event_index": log_index,
            "event_type": event_type_for(from_addr, to_addr),
            "wallet_address": wallet,
            "wallet_group": S.addr_group(wallet),
            "counterparty_address": counterparty,
            "token_address": token_addr,
            "token_group": S.addr_group(token_addr),
            "direction": direction,
            "amount_raw": amount_raw,
            "decimals": decimals,
            # ALWAYS "ERC20": downstream consumers gate on it. The symbol lives in
            # l1_token_metadata (evm_extract writes it), never in this column.
            "standard": S.STANDARD_ERC20,
            "amount": amount,
            "amount_signed": signed,
            "created_at": now_iso,
            "updated_at": now_iso,
        })
    return rows
