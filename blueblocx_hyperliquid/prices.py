"""
Map Hyperliquid pricing + positions + balances into lakehouse-compatible rows.

  candleSnapshot        -> l3_token_price_usd_1m  (the table L4 FIFO/networth read)
  clearinghouseState    -> l4x_perp_position_snapshot (extension: unrealized PnL)
  spotClearinghouseState-> spot opening balances (absolute level, not window delta)
  token registry        -> l1_token_metadata (symbol/decimals/is_stablecoin)

HyperCore has no DEX pools/WETH, so the swap-derived price chain (l0->l2->l3) does
not apply. We write l3_token_price_usd_1m DIRECTLY from native 1-minute candles,
keyed by the SAME token_address the ledger uses (so the ASOF price join actually
hits). USDC and other stables are pinned to 1.0 across EVERY minute of the ledger
span (not sampled) so USDC-denominated rows can always be valued.
"""
from __future__ import annotations

from datetime import datetime, timezone

from . import schema as S


def _iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()


def _date(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")


def _minute_floor(ms: int) -> int:
    return ms - (ms % 60000)


def candles_to_price_1m(token_address: str, candles: list[dict],
                        now_iso: str, closed_before_ms: int | None = None) -> list[dict]:
    """One candle -> one l3_token_price_usd_1m row, keyed by token_address.

    Candle fields (verified): t(open ms), T(close ms), o,h,l,c (strings),
    v(volume base), n(trade count). vwap approximated as (h+l+c)/3.

    Only CLOSED candles are emitted when `closed_before_ms` is given (candle
    close T <= that instant): the in-progress candle would otherwise be frozen
    forever by the insert-if-absent Iceberg lift while the CSV later corrects it.
    """
    rows: list[dict] = []
    tg = S.addr_group(token_address)
    for c in candles:
        if closed_before_ms is not None and int(c.get("T", 0) or 0) > closed_before_ms:
            continue
        o = float(c["o"]); h = float(c["h"]); low = float(c["l"]); cl = float(c["c"])
        vol_token = float(c.get("v", 0) or 0)
        vwap = (h + low + cl) / 3.0
        rows.append({
            "token_address": token_address,
            "token_group": tg,
            "bucket_minute": _iso(c["t"]),
            "bucket_date": _date(c["t"]),
            "open_price_usd": o,
            "high_price_usd": h,
            "low_price_usd": low,
            "close_price_usd": cl,
            "vwap_price_usd": vwap,
            "volume_token": vol_token,
            "volume_usd": vol_token * vwap,
            "num_trades": int(c.get("n", 0) or 0),
            "created_at": now_iso,
            "updated_at": now_iso,
        })
    return rows


def stable_price_1m(token_address: str, minutes_ms: list[int],
                    now_iso: str) -> list[dict]:
    """Pin a stablecoin to $1.00 across the given minute buckets so
    stable-denominated ledger rows can be valued by the ASOF price join."""
    tg = S.addr_group(token_address)
    rows = []
    for t in minutes_ms:
        rows.append({
            "token_address": token_address, "token_group": tg,
            "bucket_minute": _iso(t), "bucket_date": _date(t),
            "open_price_usd": 1.0, "high_price_usd": 1.0, "low_price_usd": 1.0,
            "close_price_usd": 1.0, "vwap_price_usd": 1.0,
            "volume_token": 0.0, "volume_usd": 0.0, "num_trades": 0,
            "created_at": now_iso, "updated_at": now_iso,
        })
    return rows


def minute_grid(start_ms: int, end_ms: int, step_min: int = 1) -> list[int]:
    """Every minute bucket in [start, end] — used to pin stables across the whole
    ledger span (MH-9: no sampling, no coverage holes for USDC)."""
    start = _minute_floor(start_ms)
    end = _minute_floor(end_ms)
    step = step_min * 60000
    return list(range(start, end + step, step))


def clearinghouse_to_snapshot(state: dict, wallet: str, as_of_ms: int,
                              now_iso: str, dex: str = "main") -> list[dict]:
    """clearinghouseState -> one l4x_perp_position_snapshot row per open position.

    unrealizedPnl is marked at markPx by Hyperliquid. We derive mark_px from
    positionValue / |szi|. accountValue/withdrawable are per-DEX margin-account
    figures (main or a HIP-3 dex), repeated on each of that dex's rows.
    """
    ms = state.get("marginSummary", {}) or {}
    account_value = float(ms.get("accountValue", 0) or 0)
    withdrawable = float(state.get("withdrawable", 0) or 0)
    wg = S.addr_group(wallet.lower())
    rows: list[dict] = []
    for ap in state.get("assetPositions", []) or []:
        p = ap.get("position", {}) or {}
        szi = float(p.get("szi", 0) or 0)
        if szi == 0.0:
            continue
        pos_value = float(p.get("positionValue", 0) or 0)
        mark_px = abs(pos_value / szi) if szi != 0 else None
        lev = p.get("leverage", {}) or {}
        cf = p.get("cumFunding", {}) or {}
        rows.append({
            "as_of_ts": _iso(as_of_ms),
            "as_of_date": _date(as_of_ms),
            "wallet_address": wallet.lower(),
            "wallet_group": wg,
            "coin": p.get("coin"),
            "dex": dex or "main",
            "szi": szi,
            "entry_px": float(p["entryPx"]) if p.get("entryPx") else None,
            "position_value": pos_value,
            "mark_px": mark_px,
            "unrealized_pnl_usd": float(p.get("unrealizedPnl", 0) or 0),
            "return_on_equity": float(p.get("returnOnEquity", 0) or 0),
            "liquidation_px": float(p["liquidationPx"]) if p.get("liquidationPx") else None,
            "margin_used_usd": float(p.get("marginUsed", 0) or 0),
            "leverage_type": lev.get("type"),
            "leverage_value": float(lev.get("value", 0) or 0),
            "cum_funding_all_time": float(cf.get("allTime", 0) or 0),
            "cum_funding_since_open": float(cf.get("sinceOpen", 0) or 0),
            "account_value_usd": account_value,
            "withdrawable_usd": withdrawable,
            "created_at": now_iso,
        })
    return rows


def token_metadata_rows(registry_items: dict, now_iso: str) -> list[dict]:
    """token registry -> l1_token_metadata rows (symbol/decimals/is_stablecoin).

    This is where the HL coin symbol lives now that `standard`=="ERC20" on the
    ledger. is_stablecoin lets stable_coin_pct compute downstream.
    """
    rows = []
    for addr, meta in registry_items.items():
        rows.append({
            "token_address": addr,
            "token_group": S.addr_group(addr),
            "standard": S.STANDARD_ERC20,
            "symbol": meta.get("symbol"),
            "name": meta.get("symbol"),
            # NULL when unresolved (nullable INT in the DDL) — never a fake 0
            "decimals": (int(meta["decimals"]) if meta.get("decimals") is not None else None),
            "first_seen": now_iso,
            "source": "hypercore:spotMeta",
            "logo_url": None,
            "total_supply_raw": None,
            "total_supply": None,
            "supply_as_of": None,
            "supply_source": None,
            "supply_confidence": None,
            "category": "stablecoin" if meta.get("is_stablecoin") else None,
            "is_stablecoin": bool(meta.get("is_stablecoin")),
            "is_native_wrapper": False,
            "coingecko_id": None,
            "token_created_at": None,
            "is_watchlist": True,
            "created_at": now_iso,
            "updated_at": now_iso,
        })
    return rows


def wallet_metadata_row(l1_rows: list[dict], wallet: str, now_iso: str) -> list[dict]:
    """l1 ledger -> one l4_wallet_metadata row (activity summary).

    HyperCore has no block_number, so first/latest are keyed on block_timestamp.
    NOTE: bounded by the ingested window — wallet_age is truncated until S3
    backfill provides full history.
    """
    if not l1_rows:
        return []
    ts = sorted(r["block_timestamp"] for r in l1_rows)
    dates = {r["block_date"] for r in l1_rows}
    hashes_first = min(l1_rows, key=lambda r: r["block_timestamp"])
    hashes_last = max(l1_rows, key=lambda r: r["block_timestamp"])
    return [{
        "wallet_address": wallet.lower(),
        "first_block_timestamp": ts[0],
        "latest_block_timestamp": ts[-1],
        "count_tx": len(l1_rows),
        "first_tx": hashes_first["tx_hash"],
        "latest_tx": hashes_last["tx_hash"],
        "active_days": len(dates),
        "last_active": None,
        "active_on_avg": None,
        "updated_at": now_iso,
    }]


def spot_symbol_prices(resolver, spot_meta: dict, ctxs: list[dict],
                       symbols) -> dict[str, float]:
    """Current USD price per base-token symbol from ONE spotMetaAndAssetCtxs call.

    IMPORTANT: ctxs are NOT positionally aligned with spot_meta['universe']
    (verified live: only 71/330 indices matched; ctxs also contains ~670 extra
    markets). Each ctx carries its own `coin` = the pair name ("PURR/USDC" or
    "@207"), unique across ctxs — key on that. Replaces per-token candleSnapshot
    fetches on the per-wallet path (one call vs N). Unpriceable symbols are omitted.
    """
    pair_px: dict[str, float] = {}
    for c in ctxs or []:
        name = c.get("coin")
        # markPx is HL's own mark (present for every pair, used for its margining);
        # midPx can be absent on thin books and diverges >2% from mark on ~40% of
        # pairs — prefer mark, fall back to mid.
        px = c.get("markPx") or c.get("midPx")
        if not name or px is None:
            continue
        try:
            px = float(px)
        except (TypeError, ValueError):
            continue
        if px > 0:
            pair_px[name] = px
    out: dict[str, float] = {}
    for sym in symbols:
        if not sym:
            continue
        pair = resolver.usdc_pair_for_base(sym)
        if pair is None:
            continue
        # pair.coin is the universe display name; pair.candle_coin is "@index".
        px = pair_px.get(pair.coin) or pair_px.get(pair.candle_coin)
        if px is not None:
            out[sym] = px
    return out
