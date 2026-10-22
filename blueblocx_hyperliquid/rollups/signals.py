"""token_market_signals: the atlas market-signals Rust job on the 1-minute candles.

Per token, rows in bucket order: EMA9 / EMA21 seeded with the first close of the
loaded window; the volume spike ratio = this bucket's volume / the mean of the last 20
buckets (>= 2x = 'spike'); the resistance = the max close of the last 20 buckets and
'breakout' when the close exceeds it. One 'global' row per minute: volume-weighted
average close, total volumes and their spike ratios over the global minute series.
The last SIGNAL_DAYS UTC days are (re)written per run after WARMUP_DAYS of warm-up.
"""
from __future__ import annotations

import datetime as dt

import pyarrow as pa

from .common import as_date, ddl_columns, ensure_inputs, table_from_rows, utc

EMA9_K = 2.0 / (9.0 + 1.0)
EMA21_K = 2.0 / (21.0 + 1.0)
SPIKE_WINDOW = 20
RESISTANCE_WINDOW = 20
SPIKE_THRESHOLD = 2.0
SIGNAL_DAYS = 3       # output days per run (replaced per bucket_date)
WARMUP_DAYS = 5       # extra days read so the EMAs and windows are converged on the first output day


def _mean(buf: list[float]) -> float:
    return sum(buf) / len(buf) if buf else 0.0


def token_signals(rows: list[dict], first_day: dt.date) -> list[dict]:
    """`rows` = one token's 1m rows in bucket order; returns the signal rows for
    bucket_date >= first_day."""
    if not rows:
        return []
    ema9 = ema21 = float(rows[0]["close_price_usd"])
    vol_t: list[float] = []
    vol_u: list[float] = []
    closes: list[float] = []
    out = []
    for r in rows:
        close = float(r["close_price_usd"])
        vt = float(r["volume_token"] or 0.0)
        vu = float(r["volume_usd"] or 0.0)
        ema9 = close * EMA9_K + ema9 * (1.0 - EMA9_K)
        ema21 = close * EMA21_K + ema21 * (1.0 - EMA21_K)
        for buf, val, size in ((vol_t, vt, SPIKE_WINDOW), (vol_u, vu, SPIKE_WINDOW), (closes, close, RESISTANCE_WINDOW)):
            if len(buf) >= size:
                buf.pop(0)
            buf.append(val)
        avg_t, avg_u = _mean(vol_t), _mean(vol_u)
        resistance = max(closes)
        rt = vt / avg_t if avg_t > 0 else 1.0
        ru = vu / avg_u if avg_u > 0 else 1.0
        day = as_date(r["bucket_date"])
        if day < first_day:
            continue
        out.append({"global_flag": "", "token_address": r["token_address"], "bucket_minute": utc(r["bucket_minute"]),
                    "bucket_date": day, "close_price_usd": close, "ema9": ema9, "ema21": ema21,
                    "ema_signal": "bullish" if ema9 >= ema21 else "bearish",
                    "volume_token": vt, "volume_token_spike_ratio": rt,
                    "volume_token_spike_signal": "spike" if rt >= SPIKE_THRESHOLD else "normal",
                    "volume_usd": vu, "volume_usd_spike_ratio": ru,
                    "volume_usd_spike_signal": "spike" if ru >= SPIKE_THRESHOLD else "normal",
                    "resistance_price": resistance,
                    "price_breakout": "breakout" if close > resistance else "normal"})
    return out


def global_signals(token_rows: list[dict]) -> list[dict]:
    by_minute: dict[dt.datetime, list[dict]] = {}
    for r in token_rows:
        by_minute.setdefault(r["bucket_minute"], []).append(r)
    vol_t: list[float] = []
    vol_u: list[float] = []
    out = []
    for minute in sorted(by_minute):
        rows = by_minute[minute]
        tv = sum(r["volume_token"] for r in rows)
        tu = sum(r["volume_usd"] for r in rows)
        num = sum(r["close_price_usd"] * r["volume_token"] for r in rows)
        vwap = num / tv if tv > 0 else sum(r["close_price_usd"] for r in rows) / len(rows)
        for buf, val in ((vol_t, tv), (vol_u, tu)):
            if len(buf) >= SPIKE_WINDOW:
                buf.pop(0)
            buf.append(val)
        avg_t, avg_u = _mean(vol_t), _mean(vol_u)
        rt = tv / avg_t if avg_t > 0 else 1.0
        ru = tu / avg_u if avg_u > 0 else 1.0
        out.append({"global_flag": "global", "token_address": "", "bucket_minute": minute,
                    "bucket_date": rows[0]["bucket_date"], "close_price_usd": vwap, "ema9": 0.0, "ema21": 0.0,
                    "ema_signal": "", "volume_token": tv, "volume_token_spike_ratio": rt,
                    "volume_token_spike_signal": "spike" if rt >= SPIKE_THRESHOLD else "normal",
                    "volume_usd": tu, "volume_usd_spike_ratio": ru,
                    "volume_usd_spike_signal": "spike" if ru >= SPIKE_THRESHOLD else "normal",
                    "resistance_price": 0.0, "price_breakout": ""})
    return out


def compute(ctx) -> dict[str, pa.Table]:
    ensure_inputs(ctx, ledger=False, meta=False, px1m=True)
    first_day = ctx.today - dt.timedelta(days=SIGNAL_DAYS - 1)
    since = first_day - dt.timedelta(days=WARMUP_DAYS)
    arrow = ctx.sql(f"""
        SELECT token_address, bucket_minute, bucket_date, close_price_usd, volume_token, volume_usd
        FROM px1m WHERE bucket_date >= DATE '{since.isoformat()}' AND close_price_usd IS NOT NULL
        ORDER BY token_address, bucket_minute""")
    out: list[dict] = []
    cur: list[dict] = []
    cur_tok = None
    for r in arrow.to_pylist():
        if r["token_address"] != cur_tok:
            out += token_signals(cur, first_day)
            cur, cur_tok = [], r["token_address"]
        cur.append(r)
    out += token_signals(cur, first_day)
    out += global_signals(out)
    for r in out:
        r["created_at"] = ctx.now
    return {"token_market_signals": table_from_rows(out, ddl_columns("token_market_signals"))}
