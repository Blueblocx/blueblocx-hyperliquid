"""
Shared per-token L3 price asset for HyperCore (l3_token_price_usd_1m).

WHY A SEPARATE JOB: price rows are per TOKEN, not per wallet. Materialising them
inside the per-wallet extract made ~86% of that run's CPU and ~85% of its API
weight wallet-independent work repeated for every wallet (audit finding). The
per-wallet path now values holdings from one spotMetaAndAssetCtxs call; this job
builds the historical L3 series once for the token universe you care about.

API CONSTRAINT: candleSnapshot returns only the ~5000 MOST RECENT candles of the
requested range and cannot be paged backward. So the interval is chosen from the
range length (interval_for_range): 1m covers ~3.5 days, 1h ~208 days, 1d years.
Rows for coarser intervals are sparse within l3_token_price_usd_1m — downstream
ASOF (backward) joins tolerate that; bucket_minute is the candle open.

Stablecoins are pinned to $1 on the same interval grid across the range.

Usage:
    python -m hl_poc.price_job --spot HYPE,UBTC --perp BTC,ETH --days 30
    python -m hl_poc.price_job --spot HYPE --days 3        # 1m granularity
Output (upsert by (token_address, bucket_minute)):
    out/hypercore/l3_token_price_usd_1m.csv
"""
from __future__ import annotations

import argparse
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from . import schema as S
from . import prices
from .info_client import InfoClient, interval_for_range
from .spot_meta import SpotMetaResolver

log = logging.getLogger("hl_poc.price_job")

OUT_ROOT = Path(__file__).resolve().parent.parent / "out"

_INTERVAL_MINUTES = {"1m": 1, "5m": 5, "15m": 15, "1h": 60, "4h": 240, "1d": 1440}
_STABLE_SYMBOLS = {"USDC", "USDT0", "USDT", "USDHL", "FEUSD"}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_price_rows(client: InfoClient, resolver: SpotMetaResolver, *,
                     spot_symbols, perp_coins, start_ms: int, end_ms: int,
                     now_iso: str, stable_addrs=None) -> tuple[list[dict], dict]:
    """Return (rows, report). report lists coverage per token + unpriceable symbols."""
    days = max((end_ms - start_ms) / 86_400_000, 1 / 1440)
    interval = interval_for_range(days)
    step_min = _INTERVAL_MINUTES[interval]
    rows: list[dict] = []
    report = {"interval": interval, "coverage": {}, "unpriceable": []}

    for sym in sorted({s for s in spot_symbols if s and s.upper() not in _STABLE_SYMBOLS}):
        pair = resolver.usdc_pair_for_base(sym)
        if pair is None:
            report["unpriceable"].append(sym)
            continue
        try:
            candles = client.candle_snapshot(pair.candle_coin, interval, start_ms, end_ms)
        except Exception as e:  # noqa: BLE001
            log.warning("spot candles failed for %s (%s): %s", sym, pair.candle_coin, e)
            report["unpriceable"].append(sym)
            continue
        rows += prices.candles_to_price_1m(pair.base.token_address, candles, now_iso)
        if candles:
            report["coverage"][sym] = {
                "token_address": pair.base.token_address, "rows": len(candles),
                "first": prices._iso(candles[0]["t"]), "last": prices._iso(candles[-1]["t"]),
            }

    for coin in sorted({c for c in perp_coins if c}):
        try:
            candles = client.candle_snapshot(coin, interval, start_ms, end_ms)
        except Exception as e:  # noqa: BLE001
            log.warning("perp candles failed for %s: %s", coin, e)
            continue
        addr = f"hlcore:perp:{coin}"
        rows += prices.candles_to_price_1m(addr, candles, now_iso)
        if candles:
            report["coverage"][f"perp:{coin}"] = {
                "token_address": addr, "rows": len(candles),
                "first": prices._iso(candles[0]["t"]), "last": prices._iso(candles[-1]["t"]),
            }

    grid = prices.minute_grid(start_ms, end_ms, step_min=step_min)
    addrs = set(stable_addrs or ())
    addrs.add(S.USDC_ADDRESS)
    for addr in sorted(addrs):
        rows += prices.stable_price_1m(addr, grid, now_iso)
    report["stable_pins"] = {"addresses": sorted(addrs), "rows_each": len(grid)}
    return rows, report


def main() -> None:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    ap = argparse.ArgumentParser(description="HyperCore shared L3 price job")
    ap.add_argument("--spot", default="", help="comma-separated spot base symbols")
    ap.add_argument("--perp", default="", help="comma-separated perp coins")
    ap.add_argument("--days", type=float, default=7)
    ap.add_argument("--out", default=str(OUT_ROOT))
    args = ap.parse_args()

    from .extract import upsert_csv  # shared writer (avoid circular import at top)

    client = InfoClient(url=os.environ.get("HL_INFO_URL", InfoClient().url))
    resolver = SpotMetaResolver(client.spot_meta())
    now_ms = int(time.time() * 1000)
    start_ms = now_ms - int(args.days * 86_400_000)
    now_iso = _now_iso()
    spot = [s for s in args.spot.split(",") if s]
    perp = [c for c in args.perp.split(",") if c]
    stable_addrs = {t.token_address for t in
                    (resolver.resolve_symbol(s) for s in _STABLE_SYMBOLS) if t}
    rows, report = build_price_rows(client, resolver, spot_symbols=spot, perp_coins=perp,
                                    start_ms=start_ms, end_ms=now_ms, now_iso=now_iso,
                                    stable_addrs=stable_addrs)
    path = Path(args.out) / S.NS_HYPERCORE / "l3_token_price_usd_1m.csv"
    upsert_csv(path, S.L3_TOKEN_PRICE_USD_1M_COLUMNS, rows,
               key_fn=lambda r: (str(r["token_address"]), str(r["bucket_minute"])),
               sort_fn=lambda r: (str(r["token_address"]), str(r["bucket_minute"])))
    log.info("price job: interval=%s tokens=%d unpriceable=%s",
             report["interval"], len(report["coverage"]), report["unpriceable"])


if __name__ == "__main__":
    main()
