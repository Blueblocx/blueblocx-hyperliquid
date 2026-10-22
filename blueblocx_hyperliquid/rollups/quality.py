"""token_quality: the atlas token_quality Rust job (version v1_trades_jump) on the
1-minute candles. Per token per UTC day: trades in the day and the largest
minute-over-minute |vwap / previous vwap - 1| (the previous minute may belong to the
day before). score 100 = >= 10 trades AND jump <= 0.80, 60 = trades only, 40 =
controlled jump only, 0 = neither. The last QUALITY_DAYS days are (re)written per run.
"""
from __future__ import annotations

import datetime as dt

import pyarrow as pa

from .common import as_date, ddl_columns, ensure_inputs, table_from_rows, utc

# v1.10.5: USDC (and any other stablecoin) is the QUOTE asset of every HyperCore
# spot market, never the base - it has no real trade count of its own (its 1m rows
# are a synthetic $1.00 pin, num_trades always 0), so the trade-count gate the atlas
# formula assumes for a traded base asset fails it every day (score 40,
# fail_reason low_trades:0). Confirmed on 2026-10-23: because every dex pair is
# quoted in USDC, that made token_quality.is_pass false for EVERY pair on the
# chain, and /token-pulse/flow-history (which reads it) was empty for all of them.
# A stablecoin's quality signal is its peg, not its own trade volume: treated as
# always-passing.

QUALITY_VERSION = "v1_trades_jump"
TRADE_THRESHOLD = 10
JUMP_THRESHOLD = 0.80
LOOKBACK_HOURS = 24
QUALITY_DAYS = 3


def _fmt6(x: float) -> str:
    s = f"{x:.6f}".rstrip("0").rstrip(".")
    return s if s else "0"


def quality_rows(rows: list[dict], first_day: dt.date) -> list[dict]:
    """`rows` = 1m rows ordered by (token, bucket_minute), the day before first_day
    included for the boundary LAG."""
    agg: dict[tuple, dict] = {}
    cur_tok, prev = None, None
    for r in rows:
        if r["token_address"] != cur_tok:
            cur_tok, prev = r["token_address"], None
        p, v = prev, r["vwap_price_usd"]
        prev = v
        day = as_date(r["bucket_date"])
        if day < first_day:
            continue
        jump = abs((v - p) / p) if (p is not None and v is not None and p > 0) else None
        e = agg.setdefault((day, cur_tok), {"trades": 0, "max_jump": None, "latest": None})
        e["trades"] += int(r["num_trades"] or 0)
        if jump is not None:
            e["max_jump"] = jump if e["max_jump"] is None else max(e["max_jump"], jump)
        m = utc(r["bucket_minute"])
        e["latest"] = m if e["latest"] is None else max(e["latest"], m)
    out = []
    for (day, tok), e in sorted(agg.items()):
        trades, max_jump = e["trades"], e["max_jump"] if e["max_jump"] is not None else 0.0
        enough, controlled = trades >= TRADE_THRESHOLD, max_jump <= JUMP_THRESHOLD
        score = 100 if enough and controlled else 60 if enough else 40 if controlled else 0
        parts = []
        if not enough:
            parts.append(f"low_trades:{trades}")
        if max_jump > JUMP_THRESHOLD:
            parts.append(f"high_jump:{_fmt6(max_jump)}")
        out.append({"as_of_date": day, "quality_version": QUALITY_VERSION, "token_address": tok,
                    "lookback_hours": LOOKBACK_HOURS, "trades_in_window": trades, "max_jump_from_prev": max_jump,
                    "trade_threshold": TRADE_THRESHOLD, "jump_threshold": JUMP_THRESHOLD, "score": score,
                    "is_pass": enough and controlled, "fail_reason": ",".join(parts), "latest_bucket_minute": e["latest"]})
    return out


def apply_stablecoin_override(rows: list[dict], stable_addresses) -> None:
    """In place: a stablecoin's row always passes (see the module note above)."""
    for r in rows:
        if r["token_address"] in stable_addresses:
            r["is_pass"] = True
            r["score"] = 100
            r["fail_reason"] = ""


def compute(ctx) -> dict[str, pa.Table]:
    ensure_inputs(ctx, ledger=False, meta=True, px1m=True)
    first_day = ctx.today - dt.timedelta(days=QUALITY_DAYS - 1)
    arrow = ctx.sql(f"""
        SELECT token_address, bucket_minute, bucket_date, vwap_price_usd, num_trades
        FROM px1m WHERE bucket_date >= DATE '{(first_day - dt.timedelta(days=1)).isoformat()}'
        ORDER BY token_address, bucket_minute""")
    rows = quality_rows(arrow.to_pylist(), first_day)
    stables = {r["token_address"] for r in ctx.sql("SELECT token_address FROM meta WHERE is_stablecoin").to_pylist()}
    apply_stablecoin_override(rows, stables)
    for r in rows:
        r["created_at"] = ctx.now
    return {"token_quality": table_from_rows(rows, ddl_columns("token_quality"))}
