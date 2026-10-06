"""
HyperEVM (chain 999) per-wallet extract -> nessie.hyperevm.* CSVs.

HyperEVM is a SEPARATE namespace from HyperCore (see README / spec §0). In
production it should flow through the atlas EVM path (python-ingestion raw_logs
-> Rust parse-raw) exactly like ethereum/arbitrum; this module is the per-wallet
POC equivalent so the namespace is populated with rows that obey the same
invariants as HyperCore:

  * standard = "ERC20" (gate), symbol/decimals in l1_token_metadata
  * natural key (tx_hash, log_index, direction) — real on-chain identifiers
  * every CSV upserted by key (shared extract.upsert_csv)

Scope: ERC-20 Transfer logs where the wallet is sender or receiver, over the
last --blocks blocks (public RPC caps eth_getLogs at 50 blocks per call).

Usage:
    python -m hl_poc.evm_extract --wallet 0xA --blocks 3000
"""
from __future__ import annotations

import argparse
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from . import schema as S
from .evm_client import EvmClient
from . import evm_mapper, evm_tokens
from .extract import upsert_csv
from .prices import token_metadata_rows

log = logging.getLogger("hl_poc.evm_extract")

OUT_ROOT = Path(__file__).resolve().parent.parent / "out"


def run(wallets: list[str], blocks: int, out_dir: Path, rpc_url: str | None = None) -> dict:
    wallets = [w.lower() for w in wallets]
    evm = EvmClient(url=rpc_url or os.environ.get("HYPEREVM_RPC_URL", EvmClient().url))
    now_iso = datetime.now(timezone.utc).isoformat()
    head = evm.block_number()
    from_block = max(0, head - blocks + 1)
    ns_dir = out_dir / S.NS_HYPEREVM
    log.info("HyperEVM: blocks %d..%d for %d wallet(s)", from_block, head, len(wallets))

    all_rows: list[dict] = []
    registry: dict[str, dict] = {}
    report: dict[str, dict] = {}
    for wallet in wallets:
        logs = (evm.get_transfer_logs_for_wallet(wallet, from_block, head, "from")
                + evm.get_transfer_logs_for_wallet(wallet, from_block, head, "to"))
        if not logs:
            report[wallet] = {"logs": 0, "rows": 0}
            continue
        block_ts = evm.get_block_timestamps(sorted({int(lg["blockNumber"], 16) for lg in logs}))
        token_addrs = sorted({lg["address"].lower() for lg in logs})
        meta = evm_tokens.resolve_token_metadata(evm, token_addrs)
        stats: dict = {}
        rows = evm_mapper.map_evm_transfers_to_ledger(logs, wallet, block_ts, meta, now_iso,
                                                      stats=stats)
        for addr, m in meta.items():
            # decimals stay NULL when unresolved — never persisted as 0 (a consumer
            # deriving amount = amount_raw / 10^decimals would inflate by 10^18)
            registry[addr] = {"symbol": m.get("symbol"), "decimals": m.get("decimals"),
                              "is_stablecoin": (m.get("symbol") or "").upper() in
                              {"USDC", "USDT0", "USDT", "USDHL", "FEUSD", "USDE", "USDH"}}
        all_rows += rows
        unresolved = sorted(a for a, m in meta.items() if m.get("decimals") is None)
        report[wallet] = {"logs": len(logs), "rows": len(rows), "tokens": len(token_addrs),
                          "rows_dropped_no_timestamp": stats.get("dropped_no_timestamp", 0),
                          "rows_amount_null": stats.get("amount_null", 0),
                          "tokens_unresolved_decimals": unresolved}
        if stats.get("dropped_no_timestamp") or unresolved:
            log.warning("[%s] dropped_no_timestamp=%d unresolved_decimals=%s", wallet[:10],
                        stats.get("dropped_no_timestamp", 0), unresolved)
        log.info("[%s] %d logs -> %d rows (%d tokens)", wallet[:10], len(logs), len(rows), len(token_addrs))

    def s(v):
        return "" if v is None else str(v)

    upsert_csv(ns_dir / "l1_wallet_ledger.csv", S.L1_WALLET_LEDGER_COLUMNS, all_rows,
               key_fn=lambda r: (s(r["tx_hash"]), s(r["event_index"]), s(r["direction"])),
               sort_fn=lambda r: (int(r["block_number"]), s(r["tx_hash"]), int(r["event_index"])))
    meta_rows = token_metadata_rows(registry, now_iso)
    for m in meta_rows:
        m["source"] = "hyperevm:eth_call"
    upsert_csv(ns_dir / "l1_token_metadata.csv", S.L1_TOKEN_METADATA_COLUMNS, meta_rows,
               key_fn=lambda r: (s(r["token_address"]),), sort_fn=lambda r: s(r["token_address"]))
    upsert_csv(ns_dir / "wallet_watchlist.csv", S.WALLET_WATCHLIST_COLUMNS, [{
        "wallet_address": w, "wallet_group": S.addr_group(w), "is_active": True,
        "whitelist_reason": "hyperevm_ingest", "created_at": now_iso, "updated_at": now_iso,
    } for w in wallets], key_fn=lambda r: (s(r["wallet_address"]),), sort_fn=lambda r: s(r["wallet_address"]))
    upsert_csv(ns_dir / "token_whitelist.csv", S.TOKEN_WHITELIST_COLUMNS, [{
        "token_address": a, "token_group": S.addr_group(a), "is_active": True,
        "whitelist_reason": "hyperevm_token", "created_at": now_iso, "updated_at": now_iso,
    } for a in registry], key_fn=lambda r: (s(r["token_address"]),), sort_fn=lambda r: s(r["token_address"]))
    return {"head_block": head, "from_block": from_block, "by_wallet": report}


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    ap = argparse.ArgumentParser(description="HyperEVM per-wallet ERC-20 extract")
    ap.add_argument("--wallet", required=True)
    ap.add_argument("--blocks", type=int, default=2000)
    ap.add_argument("--out", default=str(OUT_ROOT))
    args = ap.parse_args()
    from .s3_backfill import load_dotenv
    load_dotenv()
    wallets = [w.strip() for w in args.wallet.split(",") if w.strip()]
    rep = run(wallets, args.blocks, Path(args.out))
    log.info("done: %s", rep)


if __name__ == "__main__":
    main()
