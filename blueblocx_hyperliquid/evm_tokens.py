"""
Resolve HyperEVM ERC-20 token metadata (decimals, symbol) via eth_call.

This is the L1 metadata step for the EVM source — equivalent to atlas's
l1_token_metadata (which comes from Moralis there). Here we read it
straight off-chain with standard ERC-20 view calls, which work on the default
non-archive RPC because they only need latest state.

  decimals()  selector 0x313ce567  -> uint8
  symbol()    selector 0x95d89b41  -> string (ABI-encoded) or bytes32 (legacy)
"""
from __future__ import annotations

import logging

log = logging.getLogger(__name__)

_SEL_DECIMALS = "0x313ce567"
_SEL_SYMBOL = "0x95d89b41"


def _decode_uint(hexstr: str) -> int | None:
    h = (hexstr or "0x").replace("0x", "")
    return int(h, 16) if h else None


def _decode_string(hexstr: str) -> str | None:
    """Decode an ABI-encoded string return, with a bytes32 fallback for legacy
    tokens (e.g. MKR-style symbol() that returns a fixed bytes32)."""
    h = (hexstr or "0x").replace("0x", "")
    if not h:
        return None
    b = bytes.fromhex(h)
    # ABI dynamic string: [offset(32)][length(32)][data...]
    if len(b) >= 64:
        try:
            offset = int.from_bytes(b[0:32], "big")
            if offset == 32 and len(b) >= 64:
                length = int.from_bytes(b[32:64], "big")
                if 0 < length <= len(b) - 64:
                    return b[64:64 + length].decode("utf-8", "replace").strip()
        except Exception:  # noqa: BLE001
            pass
    # legacy bytes32: strip trailing zeros
    try:
        s = b.rstrip(b"\x00").decode("utf-8", "replace").strip()
        return s or None
    except Exception:  # noqa: BLE001
        return None


def resolve_token_metadata(evm, token_addresses: list[str]) -> dict[str, dict]:
    """Return {token_address_lower: {"decimals": int|None, "symbol": str|None}}.

    Uses batched eth_call (decimals + symbol interleaved) to avoid the public
    RPC's per-request rate limit. Per-token failures yield null fields.
    """
    calls: list[tuple[str, list]] = []
    for addr in token_addresses:
        calls.append(("eth_call", [{"to": addr, "data": _SEL_DECIMALS}, "latest"]))
        calls.append(("eth_call", [{"to": addr, "data": _SEL_SYMBOL}, "latest"]))
    results = evm._batch_call(calls)

    meta: dict[str, dict] = {}
    for idx, addr in enumerate(token_addresses):
        dec_raw = results[2 * idx]
        sym_raw = results[2 * idx + 1]
        meta[addr.lower()] = {
            "decimals": _decode_uint(dec_raw) if dec_raw else None,
            "symbol": _decode_string(sym_raw) if sym_raw else None,
        }
    return meta
