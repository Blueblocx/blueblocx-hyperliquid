"""l1_token_categories for HyperCore (decision 4A): categories provable on chain,
source 'hypercore_rules'. A token may carry several rows (PK token, group, category).

  stablecoin        l1_token_metadata.is_stablecoin (USDC, USDT0, USDHL, FEUSD, ...)
  native            HYPE
  wrapped           U-prefixed unit tokens of a major asset (UETH, UBTC, USOL, UFART...)
  spot              every token with a spot market (all tokens the ledger references)
  hip3-collateral   collateral of a HIP-3 builder dex (symbol seen as a dex collateral)
"""
from __future__ import annotations

import pyarrow as pa

from .. import schema as S
from .common import ddl_columns, ensure_inputs, table_from_rows

SOURCE = "hypercore_rules"
WRAPPED = {"UETH", "UBTC", "USOL", "UFART", "UPUMP", "UXPL", "UBNB", "UDOGE", "UXRP", "USUI", "ULTC", "UAVAX",
           "UADA", "UENA", "UDOT", "UTON", "UTRX", "UNEAR", "UPEPE", "UAPT", "ULINK"}


def categories_for(symbol: str | None, is_stable: bool, hip3_collateral: set[str]) -> list[str]:
    sym = (symbol or "").upper()
    cats = ["spot"]
    if is_stable:
        cats.append("stablecoin")
    if sym == "HYPE":
        cats.append("native")
    # Unit names every bridged asset U<ASSET> (UBTC, UETH, UZEC, UMON, UANSEM, ...); the
    # stablecoins are the only U-prefixed tokens that are not Unit assets (v1.10.3: the
    # curated set alone let UZEC / UMON through to market cap with their pre-minted pool)
    if sym in WRAPPED or (sym.startswith("U") and len(sym) >= 4 and not is_stable
                          and not sym.startswith("USD")):
        cats.append("wrapped")
    if sym in hip3_collateral:
        cats.append("hip3-collateral")
    return cats


def compute(ctx) -> dict[str, pa.Table]:
    ensure_inputs(ctx, ledger=False, meta=True)
    meta = ctx.sql("SELECT token_address, token_group, symbol, is_stablecoin FROM meta").to_pylist()
    hip3 = {"USDC", "USDH", "USDE", "USDT0"}        # collateral symbols of the HIP-3 dexes seen so far
    rows = []
    for m in meta:
        for cat in categories_for(m["symbol"], bool(m["is_stablecoin"]), hip3):
            rows.append({"token_address": m["token_address"],
                         "token_group": m["token_group"] if m["token_group"] is not None else S.addr_group(m["token_address"]),
                         "category": cat, "source": SOURCE, "created_at": ctx.now})
    return {"l1_token_categories": table_from_rows(rows, ddl_columns("l1_token_categories"))}
