"""token_stats, token_price_gainers, token_new_rank, token_beta_volatility_sharp,
token_activity_trending, token_active_address, token_summary (the atlas vw_token_*
views; formulas: ATLAS_WALLET_TABLE_FORMULAS.md, "token / market").

HyperCore inputs and the deviations the handoff documents:
  * the "as-of now / tip" prices come from the 1-minute candles exactly as the views
    read l3_token_price_usd_1m; the ALL-TIME high / low / average of token_stats come
    from the 1d candles (820 days of history) instead of the 1m table, which on
    HyperCore only starts at v1.10.0;
  * a token's first appearance (token_new_rank) is its deploy time from Info
    tokenDetails (l1_token_metadata.token_created_at); tokens without one (genesis
    tokens, USDC) are not ranked - the EVM view uses the first priced minute;
  * active addresses count the TRACKED wallets (the lake holds only their ledgers);
  * the ETH benchmark is UETH (decision 3A).
"""
from __future__ import annotations

import datetime as dt
import math
import statistics

import pyarrow as pa

from .common import (SUPPLY_EXPR, as_date, daily_prices, ddl_columns, ensure_inputs, eth_token, sample_stdev,
                     table_from_rows, utc)


def _asof_1m(ctx, col: str, at: dt.datetime, since: dt.date) -> dict[str, tuple[dt.datetime, float]]:
    """token -> (bucket_minute, price): the latest 1m bucket at or before `at`, looking
    back to `since` (the views' 3 / 30 / 31-day scan bounds)."""
    arrow = ctx.sql(f"""
        SELECT token_address, bucket_minute, {col} AS price FROM (
            SELECT token_address, bucket_minute, {col},
                   row_number() OVER (PARTITION BY token_address ORDER BY bucket_minute DESC) rn
            FROM px1m
            WHERE bucket_date >= DATE '{since.isoformat()}' AND bucket_minute <= TIMESTAMPTZ '{at.isoformat()}'
              AND {col} IS NOT NULL)
        WHERE rn = 1""")
    return {t: (utc(m), float(p)) for t, m, p in zip(*(arrow.column(c).to_pylist()
                                                        for c in ("token_address", "bucket_minute", "price")))}


def _pct(latest, prev, scale: float = 1.0):
    """(latest - prev) / prev x scale with the views' NULL rules: no 24h-old bucket, the
    same bucket on both sides, or a zero previous price -> None."""
    if latest is None or prev is None or latest[0] == prev[0] or not prev[1]:
        return None
    return (latest[1] - prev[1]) / prev[1] * scale


def token_stats(ctx) -> pa.Table:
    ensure_inputs(ctx, ledger=False, meta=True, px1d=True, px1m=True)
    now = ctx.now
    latest = _asof_1m(ctx, "vwap_price_usd", now, ctx.today - dt.timedelta(days=31))
    p24 = _asof_1m(ctx, "vwap_price_usd", now - dt.timedelta(hours=24), ctx.today - dt.timedelta(days=30))
    alltime = ctx.sql("""
        SELECT p.token_address, max(p.high_price_usd) AS ath, min(p.low_price_usd) AS atl,
               sum(p.vwap_price_usd) / nullif(count(p.vwap_price_usd), 0) AS avg
        FROM px1d p JOIN meta m ON m.token_address = p.token_address GROUP BY 1 ORDER BY 1""").to_pylist()
    eth = eth_token(ctx)
    weth_pct = _pct(latest.get(eth), p24.get(eth)) if eth else None
    out = []
    for r in alltime:
        tok = r["token_address"]
        cur = latest.get(tok)
        pct = _pct(cur, p24.get(tok))
        rel = (pct / weth_pct) if (pct is not None and weth_pct) else None
        if pct is None or weth_pct is None:
            status = None
        else:
            status = "Outperforming ETH" if (rel is not None and rel > 1) else "Underperforming ETH"
        out.append({"token_address": tok, "last_updated_at": cur[0] if cur else None,
                    "current_price": cur[1] if cur else None, "all_time_high": r["ath"], "all_time_low": r["atl"],
                    "all_time_avg": r["avg"], "price_pct_change_24h": pct, "eth_relative_strength": rel,
                    "performance_status": status})
    return table_from_rows(out, ddl_columns("token_stats"))


def token_price_gainers(ctx) -> pa.Table:
    ensure_inputs(ctx, ledger=False, meta=True, px1m=True)
    tip = ctx.sql(f"SELECT max(bucket_minute) AS tip FROM px1m "
                  f"WHERE bucket_date >= DATE '{(ctx.today - dt.timedelta(days=7)).isoformat()}'").to_pylist()[0]["tip"]
    if tip is None:
        return table_from_rows([], ddl_columns("token_price_gainers"))
    tip = utc(tip)
    latest = _asof_1m(ctx, "close_price_usd", tip, tip.date() - dt.timedelta(days=3))
    p24 = _asof_1m(ctx, "close_price_usd", tip - dt.timedelta(hours=24), tip.date() - dt.timedelta(days=30))
    known = {r["token_address"] for r in ctx.sql("SELECT token_address FROM meta").to_pylist()}
    calc = []
    for tok, cur in latest.items():
        prev = p24.get(tok)
        if prev is None or tok not in known:
            continue
        pct = _pct(cur, prev, 100.0)
        if pct is None or pct <= 0:
            continue
        calc.append({"token_address": tok, "timeframe": "24h", "current_ts": cur[0], "previous_ts": prev[0],
                     "current_price_usd": cur[1], "previous_price_usd": prev[1], "price_change_pct": pct})
    calc.sort(key=lambda r: (-r["price_change_pct"], -r["current_price_usd"]))
    for i, r in enumerate(calc, 1):
        r["rank_price_change"] = i
    return table_from_rows(calc, ddl_columns("token_price_gainers"))


def token_new_rank(ctx) -> pa.Table:
    ensure_inputs(ctx, ledger=False, meta=True, px1d=True, px1m=True)
    now = ctx.now
    since31 = ctx.today - dt.timedelta(days=31)
    latest = _asof_1m(ctx, "vwap_price_usd", now, since31)
    p24 = _asof_1m(ctx, "vwap_price_usd", now - dt.timedelta(hours=24), since31)
    # a token's birth is its deploy time (Info tokenDetails); `first_seen` (when THIS lake
    # first saw it) must not stand in for it, or every token looks new for 30 days after
    # go-live. Tokens without a deploy time (genesis tokens, USDC) are not rankable here.
    first_seen = {r["token_address"]: utc(r["fs"]) for r in ctx.sql(
        "SELECT token_address, token_created_at AS fs FROM meta WHERE token_created_at IS NOT NULL").to_pylist()}
    txn = {r["token_address"]: int(r["n"] or 0) for r in ctx.sql(f"""
        SELECT token_address, sum(n) AS n FROM (
            SELECT token_address, num_trades AS n FROM px1d WHERE bucket_date >= DATE '{since31.isoformat()}'
            UNION ALL
            SELECT token_address, num_trades AS n FROM px1m WHERE bucket_date = DATE '{ctx.today.isoformat()}')
        GROUP BY 1""").to_pylist()}
    cutoff = now - dt.timedelta(days=30)
    rows = []
    for tok, cur in latest.items():
        fs = first_seen.get(tok)
        if fs is None or fs < cutoff:
            continue
        prev = p24.get(tok)
        ret = None
        if prev is not None and cur[0] != prev[0] and prev[1] > 0:
            ret = (cur[1] / prev[1] - 1) * 100
        age_days = int((now - fs).total_seconds() / 3600 / 24.0)
        n = txn.get(tok, 0)
        rows.append({"token_address": tok, "current_ts": cur[0], "previous_24h_ts": prev[0] if prev else None,
                     "current_price_usd": cur[1], "price_24h_ago_usd": prev[1] if prev else None,
                     "price_change_24h_pct": ret, "first_seen_ts": fs, "token_age_days": age_days, "tx_count_24h": n,
                     "_ret": min(5000.0, max(-100.0, ret if ret is not None else 0.0)),
                     "_new": max(0.0, 1.0 - min(age_days, 30) / 30.0), "_ln": math.log(1 + n)})
    if not rows:
        return table_from_rows([], ddl_columns("token_new_rank"))

    def norm(key):
        lo, hi = min(r[key] for r in rows), max(r[key] for r in rows)
        return {id(r): ((r[key] - lo) / (hi - lo) if hi > lo else 0.0) for r in rows}
    ret_s, ln_s = norm("_ret"), norm("_ln")
    for r in rows:
        r["ranking_score"] = 0.50 * ret_s[id(r)] + 0.30 * r["_new"] + 0.20 * ln_s[id(r)]
    scores = sorted({r["ranking_score"] for r in rows}, reverse=True)
    dense = {s: i + 1 for i, s in enumerate(scores)}
    for r in rows:
        r["rank_new_token"] = dense[r["ranking_score"]]
    rows.sort(key=lambda r: (r["rank_new_token"], r["token_address"]))
    return table_from_rows(rows, ddl_columns("token_new_rank"))


def _beta_rows(series: dict[str, list[float]], eth: list[float] | None) -> list[dict]:
    """30 daily log returns per token (0 where a side is missing, like the view's
    COALESCE): beta = cov_pop / var_pop vs ETH, sample stddev x sqrt(365), mean / stddev."""
    out = []
    var_eth = None
    if eth is not None and len(eth) > 1:
        me = statistics.fmean(eth)
        var_eth = sum((e - me) ** 2 for e in eth) / len(eth)
    for tok, lr in series.items():
        beta = None
        if var_eth:
            ml = statistics.fmean(lr)
            cov = sum((a - ml) * (b - me) for a, b in zip(lr, eth)) / len(lr)
            beta = cov / var_eth
        if beta is None:
            desc = None
        elif beta > 1:
            desc = "Over Volatility than ETH"
        elif beta < 1:
            desc = "Lower Volatility than ETH"
        else:
            desc = "Same Volatility than ETH"
        sd = sample_stdev(lr)
        out.append({"token_address": tok, "beta": beta, "desc_beta": desc,
                    "annual_volatility": sd * math.sqrt(365) if sd is not None else None,
                    "sharp_ratio": statistics.fmean(lr) / sd if sd else None})
    return out


def _log_returns(closes_by_date: dict[dt.date, float], dates: list[dt.date]) -> list[float]:
    ffill, last = [], 0.0
    for d in dates:
        v = closes_by_date.get(d)
        if v is not None:
            last = float(v)
        ffill.append(last)
    out = []
    for i, c in enumerate(ffill):
        prev = ffill[i - 1] if i else None
        out.append(math.log(c / prev) if (c > 0 and prev is not None and prev > 0) else 0.0)
    return out


def token_beta_volatility_sharp(ctx) -> pa.Table:
    ensure_inputs(ctx, ledger=False, meta=True, px1d=True, whitelist=True)
    active = {r["token_address"] for r in ctx.sql(
        "SELECT w.token_address FROM whitelist w JOIN meta m ON m.token_address = w.token_address WHERE w.is_active").to_pylist()}
    dates = [ctx.today - dt.timedelta(days=k) for k in range(30, 0, -1)]
    px = daily_prices(ctx)
    series = {tok: _log_returns(dict(px.get(tok, [])), dates) for tok in sorted(active)}
    eth = eth_token(ctx)
    eth_lr = _log_returns(dict(px.get(eth, [])), dates) if eth and eth in active else None
    return table_from_rows(_beta_rows(series, eth_lr), ddl_columns("token_beta_volatility_sharp"))


def token_activity_trending(ctx) -> pa.Table:
    ensure_inputs(ctx, ledger=False, meta=True, px1d=True)
    today = ctx.today.isoformat()
    arrow = ctx.sql(f"""
        WITH d AS (SELECT max(bucket_date) AS today_date FROM px1d
                   WHERE bucket_date >= DATE '{today}' - 7 AND bucket_date < DATE '{today}'),
        t AS (SELECT p.token_address, d.today_date AS activity_date, sum(p.num_trades) AS tx_today,
                     sum(coalesce(p.volume_usd, 0)) AS vu_today, sum(coalesce(p.volume_token, 0)) AS vt_today
              FROM px1d p, d WHERE p.bucket_date = d.today_date
                AND EXISTS (SELECT 1 FROM meta m WHERE m.token_address = p.token_address)
              GROUP BY 1, 2),
        y AS (SELECT p.token_address, sum(p.num_trades) AS tx_y, sum(coalesce(p.volume_usd, 0)) AS vu_y
              FROM px1d p, d WHERE p.bucket_date = d.today_date - 1 GROUP BY 1),
        pr AS (SELECT p.token_address, p.close_price_usd AS price_today FROM px1d p, d WHERE p.bucket_date = d.today_date),
        pp AS (SELECT p.token_address, p.close_price_usd AS price_y FROM px1d p, d WHERE p.bucket_date = d.today_date - 1)
        SELECT t.token_address, t.activity_date, t.tx_today AS tx_count_today,
               coalesce(y.tx_y, 0) AS tx_count_yesterday,
               (t.tx_today - coalesce(y.tx_y, 0)) / nullif(CAST(coalesce(y.tx_y, 0) AS DOUBLE), 0) * 100 AS tx_change_pct,
               pr.price_today AS price_usd,
               (pr.price_today - pp.price_y) / nullif(pp.price_y, 0) * 100 AS price_change_pct,
               t.vt_today AS volume_token,
               (t.vu_today - coalesce(y.vu_y, 0)) / nullif(coalesce(y.vu_y, 0), 0) * 100 AS volume_change_pct,
               rank() OVER (ORDER BY CASE WHEN coalesce(y.tx_y, 0) >= 5
                                          THEN (t.tx_today - coalesce(y.tx_y, 0)) / nullif(CAST(coalesce(y.tx_y, 0) AS DOUBLE), 0) END
                            DESC NULLS LAST) AS rank_tx_change,
               rank() OVER (ORDER BY CASE WHEN coalesce(y.vu_y, 0) >= 1000
                                          THEN (t.vu_today - coalesce(y.vu_y, 0)) / nullif(coalesce(y.vu_y, 0), 0) END
                            DESC NULLS LAST) AS rank_volume_change
        FROM t LEFT JOIN y ON y.token_address = t.token_address
               LEFT JOIN pr ON pr.token_address = t.token_address
               LEFT JOIN pp ON pp.token_address = t.token_address
        ORDER BY 1""")
    return table_from_rows(arrow.to_pylist(), ddl_columns("token_activity_trending"))


def token_active_address(ctx) -> pa.Table:
    ensure_inputs(ctx, ledger=False, meta=True, ledger_all_days=3)
    arrow = ctx.sql(f"""
        SELECT l.token_address, count(DISTINCT l.wallet_address) AS active_address
        FROM ledger_all_3d l JOIN meta m ON m.token_address = l.token_address
        WHERE l.block_timestamp >= TIMESTAMPTZ '{ctx.now_iso}' - INTERVAL 24 HOUR
        GROUP BY 1 ORDER BY 1""")
    return table_from_rows(arrow.to_pylist(), ddl_columns("token_active_address"))


def token_summary(ctx, stats: pa.Table, trending: pa.Table, new_rank: pa.Table, gainers: pa.Table) -> pa.Table:
    ensure_inputs(ctx, ledger=False, meta=True, px1d=True, ledger_all_days=3)
    for alias, arrow in (("o_stats", stats), ("o_trending", trending), ("o_new_rank", new_rank), ("o_gainers", gainers)):
        ctx.register(alias, arrow)
    today = ctx.today.isoformat()
    arrow = ctx.sql(f"""
        WITH ts AS (SELECT token_address, {SUPPLY_EXPR} AS supply FROM meta
                    WHERE total_supply IS NOT NULL OR total_supply_raw IS NOT NULL),
        v AS (SELECT token_address, coalesce(volume_usd, 0) AS volume_usd_24h FROM px1d WHERE bucket_date = DATE '{today}' - 1),
        ac AS (SELECT token_address,
                      count(DISTINCT CASE WHEN block_date = DATE '{today}' - 1 THEN wallet_address END) AS active_today,
                      count(DISTINCT CASE WHEN block_date = DATE '{today}' - 2 THEN wallet_address END) AS active_yesterday
               FROM ledger_all_3d WHERE block_date >= DATE '{today}' - 2 GROUP BY 1),
        t AS (SELECT token_address, rank_tx_change, rank_volume_change FROM o_trending WHERE activity_date = DATE '{today}' - 1)
        SELECT s.token_address, s.current_price AS current_price_usd, s.price_pct_change_24h AS price_change_24h_pct,
               CASE WHEN s.current_price IS NOT NULL AND ts.supply IS NOT NULL AND ts.supply BETWEEN 1 AND 1e15
                         AND s.current_price < 1e5 AND ts.supply * s.current_price < 5e11
                    THEN ts.supply * s.current_price END AS market_cap_usd,
               v.volume_usd_24h,
               ac.active_today AS active_address_count_24h,
               (ac.active_today - ac.active_yesterday) / nullif(CAST(ac.active_yesterday AS DOUBLE), 0) * 100
                   AS active_address_change_24h_pct,
               t.rank_tx_change AS rank_trending, t.rank_volume_change AS rank_mover,
               nr.rank_new_token AS rank_new_coin, g.rank_price_change AS rank_gainer
        FROM o_stats s
        LEFT JOIN ts ON ts.token_address = s.token_address
        LEFT JOIN v ON v.token_address = s.token_address
        LEFT JOIN ac ON ac.token_address = s.token_address
        LEFT JOIN t ON t.token_address = s.token_address
        LEFT JOIN o_new_rank nr ON nr.token_address = s.token_address
        LEFT JOIN o_gainers g ON g.token_address = s.token_address
        WHERE EXISTS (SELECT 1 FROM meta m WHERE m.token_address = s.token_address)
          AND (s.last_updated_at >= TIMESTAMPTZ '{ctx.now_iso}' - INTERVAL 3 DAY
               OR nr.rank_new_token IS NOT NULL OR g.rank_price_change IS NOT NULL
               OR t.rank_volume_change IS NOT NULL OR v.volume_usd_24h IS NOT NULL)
        ORDER BY 1""")
    return table_from_rows(arrow.to_pylist(), ddl_columns("token_summary"))


def compute(ctx) -> dict[str, pa.Table]:
    stats = token_stats(ctx)
    gainers = token_price_gainers(ctx)
    new_rank = token_new_rank(ctx)
    trending = token_activity_trending(ctx)
    return {"token_stats": stats, "token_price_gainers": gainers, "token_new_rank": new_rank,
            "token_beta_volatility_sharp": token_beta_volatility_sharp(ctx),
            "token_activity_trending": trending, "token_active_address": token_active_address(ctx),
            "token_summary": token_summary(ctx, stats, trending, new_rank, gainers)}
