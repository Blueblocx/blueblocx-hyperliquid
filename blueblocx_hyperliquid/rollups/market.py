"""market_volume_24h, market_cap_latest, market_fear_index (atlas vw_market_volume_24h,
vw_market_cap_1d, vw_market_fear_index v3; formulas: ATLAS_WALLET_TABLE_FORMULAS.md,
"token / market").

HyperCore inputs: the native candles (market-wide) and l1_token_metadata.total_supply
(Info tokenDetails, refreshed by the price step). market_volume_24h sums the 1h
candles of every priceable token (the EVM view sums the chain's transfers priced at
the minute: same quantity, trade volume of the market); the two others follow the
views line by line.
"""
from __future__ import annotations

import datetime as dt
import math

import pyarrow as pa

from .common import SUPPLY_EXPR, as_date, ddl_columns, ensure_inputs, midrank_percentile, sample_stdev, table_from_rows

CAP_DAYS = 35               # market_cap_latest days (re)written per run (the ETL loads a 7-day window)
# v1.10.9 liquidity gate: a non-stable token's market cap may not exceed this many days of
# its own trailing-7-day average spot volume (turnover >= 0.02 %/day). HyperCore deployers
# mint the whole cap up front and Info reports circulating == total for most of them, so
# supply x price alone admitted DRV ($38 B on $1.2 M/day, 30,795 days), ONEAR ($42 B, 14,066),
# ZAMA (19,352) and LINK0 (676,556) while every genuinely traded token sits far below:
# HYPE 548, KNTQ 119, TREAD 382, HFUN 864, HPL 636, XMR1 1,716 (2026-10-23 calibration).
CAP_MAX_TURNOVER_DAYS = 5000
FEAR_HISTORY_DAYS = 820     # the view's scan: current_date - 820 .. current_date - 1
FEAR_WINDOW_DAYS = 730      # point-in-time calibration window (calendar days, inclusive)
FEAR_MIN_WINDOW = 60        # days in the window before a score is printed


def market_volume_24h(ctx) -> pa.Table:
    """The last 24 COMPLETE hours anchored to the 1h data tip: Σ volume_token and
    Σ volume_usd over every token with a candle that hour (0 for a silent hour)."""
    ensure_inputs(ctx, ledger=False, meta=True, px1h=True)
    arrow = ctx.sql(f"""
        WITH tip AS (
            SELECT max(bucket_minute) AS tip FROM px1h
            WHERE bucket_date >= CAST(TIMESTAMPTZ '{ctx.now_iso}' AS DATE) - 3
              AND bucket_minute < TIMESTAMPTZ '{ctx.now_iso}'),
        hours AS (
            SELECT tip - to_hours(23 - i) AS hour_ts FROM tip, generate_series(0, 23) t(i) WHERE tip IS NOT NULL),
        agg AS (
            SELECT p.bucket_minute AS hour_ts, sum(coalesce(p.volume_token, 0)) AS vt, sum(coalesce(p.volume_usd, 0)) AS vu
            FROM px1h p JOIN meta m ON m.token_address = p.token_address, tip
            WHERE p.bucket_minute > tip.tip - INTERVAL 24 HOUR AND p.bucket_minute <= tip.tip
            GROUP BY 1)
        SELECT h.hour_ts, coalesce(a.vt, 0.0) AS volume_token_1h, coalesce(a.vu, 0.0) AS volume_usd_1h
        FROM hours h LEFT JOIN agg a ON a.hour_ts = h.hour_ts ORDER BY 1
    """)
    return table_from_rows(arrow.to_pylist(), ddl_columns("market_volume_24h"))


def market_cap(ctx, since: dt.date) -> pa.Table:
    """vw_market_cap_1d for bucket_date in [since, today): close x normalized supply with
    the admission gates (supply 1..1e15; stables: close in [0.5, 2]; others: close <
    1e5, cap < 5e11, >= $10k volume and >= 50 trades that day, and cap <=
    CAP_MAX_TURNOVER_DAYS x the trailing-7-day average daily volume - v1.10.9)."""
    ensure_inputs(ctx, ledger=False, meta=True, px1d=True)
    return ctx.sql(f"""
        WITH n AS (
            SELECT token_address, coalesce(is_stablecoin, false) AS is_stablecoin, {SUPPLY_EXPR} AS total_supply
            FROM meta WHERE (total_supply IS NOT NULL OR total_supply_raw IS NOT NULL)
              -- Unit-bridged assets report their pre-minted pool, not the bridged amount
              AND coalesce(supply_confidence, '') <> 'low'),
        liq AS (
            SELECT token_address, bucket_date,
                   avg(coalesce(volume_usd, 0)) OVER (PARTITION BY token_address ORDER BY bucket_date
                                                      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS vol_7d
            FROM px1d)
        SELECT p.token_address, p.bucket_date AS as_of_date, n.total_supply,
               n.total_supply * p.close_price_usd AS total_market_cap, n.is_stablecoin
        FROM px1d p JOIN n ON n.token_address = p.token_address
        JOIN liq l ON l.token_address = p.token_address AND l.bucket_date = p.bucket_date
        WHERE p.close_price_usd IS NOT NULL AND n.total_supply IS NOT NULL
          AND p.bucket_date >= DATE '{since.isoformat()}' AND p.bucket_date < DATE '{ctx.today.isoformat()}'
          AND n.total_supply BETWEEN 1 AND 1e15
          AND (CASE WHEN n.is_stablecoin THEN p.close_price_usd BETWEEN 0.5 AND 2.0
                    ELSE p.close_price_usd < 1e5 AND n.total_supply * p.close_price_usd < 5e11
                         AND coalesce(p.volume_usd, 0) >= 1e4 AND coalesce(p.num_trades, 0) >= 50
                         AND n.total_supply * p.close_price_usd <= {CAP_MAX_TURNOVER_DAYS} * l.vol_7d END)
        ORDER BY 1, 2
    """)


def market_cap_latest(ctx) -> pa.Table:
    arrow = market_cap(ctx, ctx.today - dt.timedelta(days=CAP_DAYS))
    return table_from_rows(arrow.to_pylist(), ddl_columns("market_cap_latest"))


def _trailing_mean(values: list[float], n: int) -> float:
    w = values[-n:]
    return sum(w) / len(w)


def fear_index_rows(base: list[dict], caps: dict[tuple, float], today: dt.date) -> list[dict]:
    """The view's arithmetic on python rows. `base` = whitelisted tokens' daily rows
    {token_address, day_date, close, volume_usd, is_stable} (close > 0, days before
    today), `caps` = (token, day) -> total_market_cap."""
    by_token: dict[str, list[dict]] = {}
    for r in base:
        by_token.setdefault(r["token_address"], []).append(r)
    xsec: dict[dt.date, dict] = {}
    for tok, rows in by_token.items():
        rows.sort(key=lambda r: r["day_date"])
        closes: list[float] = []
        lrs: list[float | None] = []
        prev = None
        for r in rows:
            close = float(r["close"])
            closes.append(close)
            lr = None
            if prev is not None and prev > 0:
                lr = max(-0.5, min(0.5, math.log(close / prev)))
            lrs.append(lr)
            prev = close
            ma30 = _trailing_mean(closes, 30)
            ma90 = _trailing_mean(closes, 90)
            rv30 = sample_stdev([x for x in lrs[-30:] if x is not None])
            mcap = caps.get((tok, r["day_date"]))
            x = xsec.setdefault(r["day_date"], {"mom_num": 0.0, "mom_den": 0.0, "br_num": 0.0, "br_den": 0.0,
                                                "rv_num": 0.0, "rv_den": 0.0, "vol": 0.0, "stable_vol": 0.0})
            vol = float(r["volume_usd"] or 0.0)
            x["vol"] += vol
            if r["is_stable"]:
                x["stable_vol"] += vol
                continue
            if mcap is None or mcap <= 0:
                continue
            if ma90 > 0:
                x["mom_num"] += mcap * max(-0.95, min(10.0, close / ma90 - 1))
                x["mom_den"] += mcap
            if ma30 > 0:
                x["br_den"] += mcap
                if close > ma30:
                    x["br_num"] += mcap
            if rv30 is not None:
                x["rv_num"] += mcap * rv30
                x["rv_den"] += mcap
    days = sorted(xsec)
    vols = [xsec[d]["vol"] for d in days]
    calib: list[dict] = []
    for i, d in enumerate(days):
        x = xsec[d]
        mom = x["mom_num"] / x["mom_den"] if x["mom_den"] else None
        breadth = x["br_num"] / x["br_den"] if x["br_den"] else None
        rv = x["rv_num"] / x["rv_den"] if x["rv_den"] else None
        v30 = _trailing_mean(vols[:i + 1], 30)
        v90 = _trailing_mean(vols[:i + 1], 90)
        volmom = v30 / v90 if v90 else None
        scs = x["stable_vol"] / x["vol"] if x["vol"] else None
        if mom is None or breadth is None or rv is None or volmom is None:
            continue
        calib.append({"day": d, "mom": mom, "breadth": breadth, "rv": rv, "volmom": volmom, "scs": scs})
    out: list[dict] = []
    for i, c in enumerate(calib):
        lo = c["day"] - dt.timedelta(days=FEAR_WINDOW_DAYS)
        window = [w for w in calib[:i + 1] if w["day"] >= lo]
        if len(window) < FEAR_MIN_WINDOW:
            continue
        mom_s = 100.0 * midrank_percentile([w["mom"] for w in window], c["mom"])
        br_s = 100.0 * midrank_percentile([w["breadth"] for w in window], c["breadth"])
        rv_s = 100.0 * (1 - midrank_percentile([w["rv"] for w in window], c["rv"]))
        vol_s = 100.0 * midrank_percentile([w["volmom"] for w in window], c["volmom"])
        scs_window = [w["scs"] for w in window if w["scs"] is not None]
        mc_s = 100.0 * (1 - midrank_percentile(scs_window, c["scs"])) if c["scs"] is not None and scs_window else None
        fear = (round(0.30 * mom_s + 0.20 * br_s + 0.20 * rv_s + 0.15 * vol_s + 0.15 * mc_s, 2)
                if mc_s is not None else None)
        if fear is None:
            profile = "Unknown"
        elif fear <= 25:
            profile = "Extreme Fear"
        elif fear <= 45:
            profile = "Fear"
        elif fear <= 55:
            profile = "Neutral"
        elif fear <= 75:
            profile = "Greed"
        else:
            profile = "Extreme Greed"
        out.append({"as_of_date": c["day"], "momentum_score": round(mom_s, 2), "breadth_score": round(br_s, 2),
                    "volatility_score": round(rv_s, 2), "volume_score": round(vol_s, 2),
                    "market_comp_score": round(mc_s, 2) if mc_s is not None else None,
                    "fear_index": fear, "market_profile": profile})
    return out


def market_fear_index(ctx) -> pa.Table:
    ensure_inputs(ctx, ledger=False, meta=True, px1d=True, whitelist=True)
    since = ctx.today - dt.timedelta(days=FEAR_HISTORY_DAYS)
    caps_arrow = market_cap(ctx, since)
    caps = {(t, as_date(d)): float(c) for t, d, c in zip(*(caps_arrow.column(k).to_pylist()
                                                            for k in ("token_address", "as_of_date", "total_market_cap")))
            if c is not None}
    base = ctx.sql(f"""
        SELECT p.token_address, p.bucket_date AS day_date, p.close_price_usd AS close,
               coalesce(p.volume_usd, 0) AS volume_usd, coalesce(m.is_stablecoin, false) AS is_stable
        FROM px1d p
        JOIN whitelist w ON w.token_address = p.token_address AND w.is_active
        JOIN meta m ON m.token_address = p.token_address
        WHERE p.bucket_date >= DATE '{since.isoformat()}' AND p.bucket_date < DATE '{ctx.today.isoformat()}'
          AND p.close_price_usd > 0
    """).to_pylist()
    for r in base:
        r["day_date"] = as_date(r["day_date"])
    return table_from_rows(fear_index_rows(base, caps, ctx.today), ddl_columns("market_fear_index"))


def compute(ctx) -> dict[str, pa.Table]:
    return {"market_volume_24h": market_volume_24h(ctx),
            "market_cap_latest": market_cap_latest(ctx),
            "market_fear_index": market_fear_index(ctx)}
