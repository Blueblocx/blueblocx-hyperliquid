"""wallet_overview_pnl_percentile_1d, wallet_performance_30d, l4_token_wallet_sharpe_1d
(formulas sections 8, 9 and the pnl-percentile view).

Cross-wallet, dated: they read the rollup tables the earlier engines just wrote
(every wallet, not only the changed ones) and are replaced per date.
"""
from __future__ import annotations

import datetime as dt
import math
import statistics

import pyarrow as pa

from .common import ETH_SYMBOL, as_date, asof, daily_prices, ddl_columns, ensure_inputs, table_from_rows, utc


def _load_rollup(ctx, name: str, alias: str, date_col: str | None, days: int) -> int:
    if not ctx.has_table(name):
        return 0
    return ctx.load(name, date_col=date_col, wallet_col=None,
                    since=ctx.today - dt.timedelta(days=days), alias=alias)


def pnl_percentile(ctx) -> pa.Table:
    """vw_wallet_overview_pnl_percentile_1d: for each computed_date of the last 30 days,
    rolling 1d/7d/30d realized PnL per wallet and PERCENT_RANK across wallets."""
    if not _load_rollup(ctx, "l4_wallet_pnl_realized_1d", "realized_all", "snapshot_date", 61):
        return table_from_rows([], ddl_columns("wallet_overview_pnl_percentile_1d"))
    today = ctx.today.isoformat()
    arrow = ctx.sql(f"""
        WITH dates AS (SELECT CAST(d AS DATE) AS computed_date
                       FROM generate_series(TIMESTAMP '{today} 00:00:00' - INTERVAL 30 DAY,
                                            TIMESTAMP '{today} 00:00:00', INTERVAL 1 DAY) t(d)),
        pnl_windows AS (
            SELECT dt.computed_date, p.wallet_address,
                   sum(CASE WHEN p.snapshot_date > dt.computed_date - 1  AND p.snapshot_date <= dt.computed_date THEN p.realized_pnl_usd ELSE 0 END) AS pnl_1d_usd,
                   sum(CASE WHEN p.snapshot_date > dt.computed_date - 7  AND p.snapshot_date <= dt.computed_date THEN p.realized_pnl_usd ELSE 0 END) AS pnl_7d_usd,
                   sum(CASE WHEN p.snapshot_date > dt.computed_date - 30 AND p.snapshot_date <= dt.computed_date THEN p.realized_pnl_usd ELSE 0 END) AS pnl_30d_usd
            FROM dates dt JOIN realized_all p
              ON p.snapshot_date > dt.computed_date - 30 AND p.snapshot_date <= dt.computed_date
            GROUP BY 1, 2)
        SELECT computed_date, wallet_address, pnl_1d_usd, pnl_7d_usd, pnl_30d_usd,
               CASE WHEN pnl_1d_usd <> 0 THEN percent_rank() OVER (PARTITION BY computed_date ORDER BY pnl_1d_usd) END AS pnl_percentile_1d,
               CASE WHEN pnl_7d_usd <> 0 THEN percent_rank() OVER (PARTITION BY computed_date ORDER BY pnl_7d_usd) END AS pnl_percentile_7d,
               percent_rank() OVER (PARTITION BY computed_date ORDER BY pnl_30d_usd) AS pnl_percentile_30d
        FROM pnl_windows
    """)
    return table_from_rows(arrow.to_pylist(), ddl_columns("wallet_overview_pnl_percentile_1d"))


def performance_30d(ctx) -> pa.Table:
    """vw_wallet_performance_30d, chain-scoped: daily log returns of the closing
    networth over the last 30 days (wallet-days worth <= $10 dropped, >= 7 return
    points), benchmark = the UETH spot market's daily close (decision 3A)."""
    ensure_inputs(ctx, ledger=False, meta=True, px1d=True)
    if not _load_rollup(ctx, "l4_wallet_networth_1d_dense", "networth_all", "snapshot_date", 31):
        return table_from_rows([], ddl_columns("wallet_performance_30d"))
    eth = ctx.sql(f"SELECT token_address FROM meta WHERE upper(symbol) = '{ETH_SYMBOL}' LIMIT 1").to_pylist()
    eth_prices = daily_prices(ctx).get(eth[0]["token_address"], []) if eth else []
    r_eth: dict[dt.date, float] = {}
    for (d0, p0), (d1, p1) in zip(eth_prices, eth_prices[1:]):
        if p0 > 0 and p1 > 0:
            r_eth[d1] = math.log(p1 / p0)
    start = ctx.today - dt.timedelta(days=30)
    nw = ctx.sql(f"SELECT wallet_address, snapshot_date, sum(closing_networth_usd) AS nw_usd FROM networth_all "
                 f"WHERE snapshot_date >= DATE '{start.isoformat()}' GROUP BY 1, 2 HAVING sum(closing_networth_usd) > 10 "
                 "ORDER BY 1, 2")
    by_wallet: dict[str, list[tuple[dt.date, float]]] = {}
    for w, d, v in zip(*(nw.column(c).to_pylist() for c in ("wallet_address", "snapshot_date", "nw_usd"))):
        by_wallet.setdefault(w, []).append((as_date(d), float(v)))
    out: list[dict] = []
    for w, series in by_wallet.items():
        pts = []       # (date, nw, r_wallet, r_eth)
        for (d0, v0), (d1, v1) in zip(series, series[1:]):
            if v0 > 0:
                pts.append((d1, v1, math.log(v1 / v0), r_eth.get(d1)))
        if len(pts) < 7:
            continue
        n = len(pts)
        rw = [p[2] for p in pts]
        re_ = [p[3] for p in pts if p[3] is not None]
        avg_r = statistics.fmean(rw)
        vol = statistics.stdev(rw) if n > 1 else None
        avg_eth = statistics.fmean(re_) if re_ else None
        pairs = [(p[2], p[3]) for p in pts if p[3] is not None]
        corr = None
        if len(pairs) >= 2:
            mx = statistics.fmean(a for a, _ in pairs)
            my = statistics.fmean(b for _, b in pairs)
            cov = sum((a - mx) * (b - my) for a, b in pairs) / len(pairs)
            sx = math.sqrt(sum((a - mx) ** 2 for a, _ in pairs) / len(pairs))
            sy = math.sqrt(sum((b - my) ** 2 for _, b in pairs) / len(pairs))
            corr = cov / (sx * sy) if sx * sy else None
        nw_start, nw_end = pts[0][1], pts[-1][1]
        out.append({
            "wallet_address": w, "as_of_date": pts[-1][0], "start_date": pts[0][0], "end_date": pts[-1][0],
            "nw_start": nw_start, "nw_end": nw_end,
            "wallet_return_30d": (nw_end - nw_start) / nw_start if nw_start else None,
            "avg_annualized_return_30d": avg_r * 365.0,
            "volatility_daily_30d": vol, "volatility_annualized_30d": (vol * math.sqrt(365.0)) if vol is not None else None,
            "sharpe_ratio_annualized_30d": (avg_r / vol * math.sqrt(365.0)) if vol else 0.0,
            "correlation_with_eth_30d": corr,
            "eth_return_30d": (avg_eth * n) if avg_eth is not None else None,
            "outperform_market_30d": (avg_r * n > avg_eth * n) if avg_eth is not None else None,
            "alpha_vs_eth_30d": ((avg_r - avg_eth) * n) if avg_eth is not None else None,
            "num_return_points_used": n,
        })
    return table_from_rows(out, ddl_columns("wallet_performance_30d"))


def token_wallet_sharpe(ctx) -> pa.Table:
    """l4_token_wallet_sharpe_1d for snapshot_date = yesterday (atlas default)."""
    ensure_inputs(ctx, ledger=False, meta=True, px1d=True, ledger_all_days=31)
    snap = ctx.today - dt.timedelta(days=1)
    if not _load_rollup(ctx, "l4_wallet_balance_per_tx", "per_tx_all", "block_date", 31) or \
            not _load_rollup(ctx, "l4_wallet_balance_1d_dense", "dense_all", "snapshot_date", 31):
        return table_from_rows([], ddl_columns("l4_token_wallet_sharpe_1d"))
    prices = daily_prices(ctx)
    supply = {r["token_address"]: r["total_supply"] for r in ctx.sql(
        "SELECT token_address, total_supply FROM meta WHERE total_supply IS NOT NULL AND total_supply > 0").to_pylist()}
    closing = ctx.sql(f"""
        SELECT wallet_address, token_address, block_date, balance_amount FROM (
            SELECT *, row_number() OVER (PARTITION BY wallet_address, token_address, block_date
                                         ORDER BY block_timestamp DESC, event_index DESC) rn
            FROM per_tx_all WHERE block_date BETWEEN DATE '{(snap - dt.timedelta(days=30)).isoformat()}' AND DATE '{snap.isoformat()}')
        WHERE rn = 1 ORDER BY wallet_address, token_address, block_date""")
    series: dict[tuple, list[tuple[dt.date, float]]] = {}
    for w, t, d, b in zip(*(closing.column(c).to_pylist() for c in ("wallet_address", "token_address", "block_date", "balance_amount"))):
        series.setdefault((w, t), []).append((as_date(d), float(b or 0.0)))
    dense = ctx.sql(f"SELECT wallet_address, token_address, snapshot_date, closing_balance_amount FROM dense_all "
                    f"WHERE snapshot_date IN (DATE '{(snap - dt.timedelta(days=7)).isoformat()}', DATE '{(snap - dt.timedelta(days=30)).isoformat()}')")
    past_bal: dict[tuple, float] = {(w, t, as_date(d)): float(b or 0.0) for w, t, d, b in zip(
        *(dense.column(c).to_pylist() for c in ("wallet_address", "token_address", "snapshot_date", "closing_balance_amount")))}
    first_in = ctx.sql(f"SELECT wallet_address, token_address, min(block_timestamp) AS first_in FROM ledger_all_31d "
                       f"WHERE direction = 'in' AND block_date >= DATE '{(snap - dt.timedelta(days=30)).isoformat()}' GROUP BY 1, 2")
    first_in_map = {(w, t): utc(ts) for w, t, ts in zip(*(first_in.column(c).to_pylist() for c in ("wallet_address", "token_address", "first_in")))}
    out: list[dict] = []
    for (w, t), bal_series in series.items():
        px_series = prices.get(t)
        if not px_series:
            continue
        today_bal = asof(bal_series, snap)
        if today_bal is None or today_bal <= 0:
            continue
        today_px = asof(px_series, snap)
        if today_px is None:
            continue
        vals = []
        d = snap - dt.timedelta(days=30)
        while d <= snap:
            bal = asof(bal_series, d, 0.0)
            px = asof(px_series, d)
            if px is not None and bal > 0:
                vals.append(bal * px)
            d += dt.timedelta(days=1)
        if len(vals) < 2:
            continue
        lrs = [math.log(b / a) for a, b in zip(vals, vals[1:]) if a > 0 and b > 0]
        sharpe = None
        if len(lrs) >= 2:
            sd = statistics.stdev(lrs)
            if sd > 0:
                sharpe = statistics.fmean(lrs) / sd
        today_val = today_bal * today_px

        def past(days: int):
            pb = past_bal.get((w, t, snap - dt.timedelta(days=days)))
            if pb is None or pb <= 0:
                return None
            pp = asof(px_series, snap - dt.timedelta(days=days))
            if pp is None or pp <= 0:
                return None
            return pb * pp

        def change(p):
            return ((today_val - p) / p * 100.0) if p is not None and p >= 1.0 else None

        sup = supply.get(t)
        out.append({
            "token_address": t, "wallet_address": w, "snapshot_date": snap, "balance_amount": today_bal,
            "ownership_pct": (today_bal / sup * 100.0) if sup else 0.0, "position_value_usd": today_val,
            "value_change_pct_7d": change(past(7)), "value_change_pct_30d": change(past(30)),
            "first_in_timestamp": first_in_map.get((w, t)), "sharpe_ratio": sharpe,
        })
    return table_from_rows(out, ddl_columns("l4_token_wallet_sharpe_1d"))


def compute(ctx) -> dict[str, pa.Table]:
    return {"wallet_overview_pnl_percentile_1d": pnl_percentile(ctx),
            "wallet_performance_30d": performance_30d(ctx),
            "l4_token_wallet_sharpe_1d": token_wallet_sharpe(ctx)}
