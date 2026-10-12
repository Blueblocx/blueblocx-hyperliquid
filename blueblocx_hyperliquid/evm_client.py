"""
Minimal HyperEVM JSON-RPC client (chain id 999).

Ingestion strategy here = PER-WALLET, not whole-chain. atlas production scans
every block into raw_logs; here we only need one wallet's portfolio, so we filter
eth_getLogs by the wallet address in the Transfer topics. This is far cheaper
(no 40M-block scan) and matches the "dump this wallet" use case.

Verified constraints (memory reference-hyperliquid-data-sources):
  - eth_getLogs capped at 50 blocks per query, <=4 topics.
  - default public RPC is non-archive (latest state only) — fine, we read logs.
  - chain id 999 = 0x3e7.
"""
from __future__ import annotations

import time
import logging

import requests

log = logging.getLogger(__name__)

DEFAULT_RPC = "https://rpc.hyperliquid.xyz/evm"
GETLOGS_MAX_SPAN = 50           # blocks per eth_getLogs (hard cap on public RPC)
ERC20_TRANSFER_TOPIC = (
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
)
_MAX_RETRIES = 5


def addr_to_topic(addr: str) -> str:
    """Left-pad a 20-byte address to a 32-byte topic (for from/to filtering)."""
    a = addr.lower().replace("0x", "")
    return "0x" + a.rjust(64, "0")


class EvmClient:
    def __init__(self, url: str = DEFAULT_RPC, timeout: int = 30):
        self.url = url
        self.timeout = timeout
        self._session = requests.Session()
        self._session.headers.update({"Content-Type": "application/json"})
        self._id = 0

    def _call(self, method: str, params: list) -> object:
        self._id += 1
        body = {"jsonrpc": "2.0", "id": self._id, "method": method, "params": params}
        backoff = 1.0
        last_err: Exception | None = None
        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                r = self._session.post(self.url, json=body, timeout=self.timeout)
                if r.status_code == 429 or r.status_code >= 500:
                    raise requests.HTTPError(f"HTTP {r.status_code}")
                r.raise_for_status()
                payload = r.json()
                if "error" in payload:
                    raise RuntimeError(f"RPC error: {payload['error']}")
                return payload["result"]
            except Exception as e:  # noqa: BLE001
                last_err = e
                if attempt == _MAX_RETRIES:
                    break
                log.warning("evm %s attempt %d failed: %s; retry in %.1fs",
                            method, attempt, e, backoff)
                time.sleep(backoff)
                backoff = min(backoff * 2, 16.0)
        raise RuntimeError(f"RPC {method} failed after {_MAX_RETRIES} tries: {last_err}")

    def _batch_call(self, calls: list[tuple[str, list]],
                    chunk: int = 25, pace_s: float = 0.5) -> list[object]:
        """Send many JSON-RPC calls as batched HTTP requests (chunked + paced).

        Collapses N per-item calls into ceil(N/chunk) HTTP round-trips — the fix
        for the public RPC's per-request rate limit. Returns results in the same
        order as `calls`; a per-item error yields None for that slot.
        """
        results: list[object] = [None] * len(calls)
        for base in range(0, len(calls), chunk):
            batch = calls[base:base + chunk]
            payload = [{"jsonrpc": "2.0", "id": i, "method": m, "params": p}
                       for i, (m, p) in enumerate(batch)]
            backoff = 1.0
            for attempt in range(1, _MAX_RETRIES + 1):
                try:
                    r = self._session.post(self.url, json=payload, timeout=self.timeout)
                    if r.status_code == 429 or r.status_code >= 500:
                        raise requests.HTTPError(f"HTTP {r.status_code}")
                    r.raise_for_status()
                    resp = r.json()
                    if not isinstance(resp, list):
                        # some RPCs return a single error object for the whole batch
                        raise requests.HTTPError(f"non-list batch response: {resp}")
                    by_id = {item["id"]: item for item in resp
                             if isinstance(item, dict) and "id" in item}
                    rate_limited = False
                    for i in range(len(batch)):
                        item = by_id.get(i, {})
                        err = item.get("error")
                        if err and err.get("code") == -32005:  # rate limited
                            rate_limited = True
                            break
                        results[base + i] = item.get("result")
                    if rate_limited:
                        raise requests.HTTPError("batch rate limited")
                    break
                except Exception as e:  # noqa: BLE001
                    if attempt == _MAX_RETRIES:
                        log.warning("batch chunk @%d failed after retries: %s", base, e)
                        break
                    time.sleep(backoff)
                    backoff = min(backoff * 2, 16.0)
            time.sleep(pace_s)
        return results

    def get_block_timestamps(self, block_numbers: list[int]) -> dict[int, int]:
        """Batched block -> unix-seconds map, with a single-call fallback for any
        block a batch chunk failed to return (so a flaky chunk can't zero-stamp
        hundreds of rows)."""
        calls = [("eth_getBlockByNumber", [hex(bn), False]) for bn in block_numbers]
        out: dict[int, int] = {}
        missing: list[int] = []
        for bn, blk in zip(block_numbers, self._batch_call(calls)):
            if isinstance(blk, dict) and blk.get("timestamp"):
                out[bn] = int(blk["timestamp"], 16)
            else:
                missing.append(bn)
        for bn in missing:
            try:
                out[bn] = self.get_block_timestamp(bn)
            except Exception as e:  # noqa: BLE001
                log.warning("block %d timestamp unresolved: %s", bn, e)
                out[bn] = 0
        if missing:
            log.info("block-ts fallback resolved %d blocks the batch missed",
                     len(missing))
        return out

    def chain_id(self) -> int:
        return int(self._call("eth_chainId", []), 16)

    def block_number(self) -> int:
        return int(self._call("eth_blockNumber", []), 16)

    def get_block_timestamp(self, block_number: int) -> int:
        """Unix seconds for a block (used to stamp ledger rows)."""
        blk = self._call("eth_getBlockByNumber", [hex(block_number), False])
        return int(blk["timestamp"], 16) if blk else 0

    def get_transfer_logs_for_wallet(self, wallet: str, from_block: int,
                                     to_block: int, side: str) -> list[dict]:
        """ERC-20 Transfer logs where wallet is the sender (side='from') or
        receiver (side='to'), scanning [from_block, to_block] in <=50-block windows.

        Two topic slots after topic0: [Transfer, from, to]. We set exactly one to
        the wallet topic and leave the other null (match any).
        """
        wallet_topic = addr_to_topic(wallet)
        if side == "from":
            topics = [ERC20_TRANSFER_TOPIC, wallet_topic]         # from == wallet
        elif side == "to":
            topics = [ERC20_TRANSFER_TOPIC, None, wallet_topic]   # to == wallet
        else:
            raise ValueError("side must be 'from' or 'to'")

        out: list[dict] = []
        start = from_block
        while start <= to_block:
            end = min(start + GETLOGS_MAX_SPAN - 1, to_block)
            result = self._call("eth_getLogs", [{
                "fromBlock": hex(start),
                "toBlock": hex(end),
                "topics": topics,
            }])
            if isinstance(result, list):
                out.extend(result)
            start = end + 1
            time.sleep(0.25)   # public RPC rate-limits tight getLogs loops
        return out
