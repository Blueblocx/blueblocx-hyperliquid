"""
Token Heatmap inputs (v1.11.0, TOKEN_HEATMAP_DATA_SPEC.md section 2): three small
bucket-keyed snapshot tables the hourly run stages from data it already holds.

    l4x_perp_position_snap    one row per open perp position per wallet per bucket
                              (clearinghouseState of the main dex + every HIP-3 dex the
                              wallet touched; as_of_ts = when THAT wallet was fetched)
    l4x_wallet_cohort_snap    one row per PLANNED wallet per bucket: fetched or not
                              (position_snapshot_present), perp equity -> size cohort,
                              leaderboard windows -> perf cohort, tracked / pinned flags
    l4x_perp_market_ctx_snap  one row per perp asset per bucket from metaAndAssetCtxs
                              (open interest, funding, mark / oracle px) per dex in
                              HL_HEATMAP_MARKET_DEXES - the ONLY new Info API weight
                              (20 per dex, after the pull phase)

Everything here that builds rows is a pure function of the API payloads (tested with
synthetic clearinghouseState / leaderboard / metaAndAssetCtxs fixtures); extract.run wires
them in behind `enabled()` (HL_HEATMAP=1 and HL_HEATMAP_WRITER=hourly; the default is OFF
and the run then behaves exactly as v1.10.14). The AGGREGATION (open-state, cohort x coin
cells) is downstream in Dagster: one rollup implementation, many writers, a `source` column.

Grain: as_of_bucket = date_trunc('hour', run start), bucket_minutes = 60 (MVP; the Stage-2
sweep writes the same tables with 30 / 15 and source = sweep; the daily seed below writes
1440 / l4x_daily). `created_at` is the run stamp (_STAMP_COL); `run_id` = <source>-<run
start epoch ms>. HIP-3 coins carry their dex prefix (xyz:SP500), so `dex` is a column, not
part of the key.

Skip guard (spec 2.6): every heatmap step in extract.run is try/except -> a run-summary
warning (`heatmap:*`), never a failed run; the three tables are lifted in their OWN
lift_directory call, skipped when the run is already past HL_HEATMAP_LIFT_DEADLINE_S on
entering the lift phase (the bucket's as_of_ts is kept as `heatmap.pending_since` in
_lift_state.json and the next run lifts from there), and metaAndAssetCtxs is skipped when
the pull phase saw HTTP 429s.

Cohort rules (the same values as the API's token-heatmap.constants.ts; change both and bump
the version):
    size_cohort(perp equity E), HL_HEATMAP_SIZE_BOUNDS = 100k, 500k, 1M, 5M  (size_v1)
        under_100k < 100k <= small_whale < 500k <= whale < 1M <= tidal_whale < 5M <= leviathan
        size_cohort_fine = HyperTracker tier id: 16 (< 250), 1 (250-10k), 2 (10k-50k),
        3 (50k-100k), then 4 / 5 / 6 / 7 for the four named bands
    perf_cohort(all-time pnl P), HL_HEATMAP_PNL_BOUNDS = -10k, 10k, 100k, 1M  (perf_v1)
        losing_trader <= -10k < average_trader < 10k <= consistent_trader < 100k <=
        smart_money < 1M <= money_printer; unknown when the wallet is not on the leaderboard
        perf_cohort_fine = HyperTracker tier: 8 money printer, 9 smart money, 10 consistent,
        11 (0 <= P < 10k), 12 (-10k < P < 0), 13 (-100k < P <= -10k), 14 (-1M < P <= -100k),
        15 (P <= -1M) - the negative sub-tiers mirror the positive bounds
    is_active = vlm_month > 0 OR has_open_position (HyperTracker's "active trader")

Daily seed (spec 4.6, run ONCE after Gate 1, by hand between ticks, under the run lock):
    python -m blueblocx_hyperliquid.heatmap --seed-daily [--until YYYY-MM-DD] [--since YYYY-MM-DD] [--dry-run]
reads hypercore.l4x_perp_position_snapshot (the daily EOD history since 2026-10-13) and
commits one l4x_perp_position_snap / l4x_wallet_cohort_snap row set per closed day BEFORE
the first hourly bucket (as_of_bucket = the day at 00:00 UTC, bucket_minutes = 1440,
source = l4x_daily, perf_cohort = unknown, tracked flags NULL) DIRECTLY into Iceberg
(ensure_table + load_table + write_watermarks, like the rollups) - never through the
staged CSVs, whose bounded lift would drop rows this old.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import math
import sys
import time
from pathlib import Path

from . import config
from . import schema as S

log = logging.getLogger("blueblocx_hyperliquid.heatmap")

TABLE_POSITIONS = "l4x_perp_position_snap"
TABLE_COHORT = "l4x_wallet_cohort_snap"
TABLE_MARKET_CTX = "l4x_perp_market_ctx_snap"
TABLES = (TABLE_POSITIONS, TABLE_COHORT, TABLE_MARKET_CTX)

SIZE_COHORT_VERSION = "size_v1"
PERF_COHORT_VERSION = "perf_v1"
SOURCE_HOURLY = "hourly"
SOURCE_SWEEP = "sweep"
SOURCE_L4X_DAILY = "l4x_daily"
BUCKET_MINUTES_HOURLY = 60
BUCKET_MINUTES_DAILY = 1440
MAIN_DEX = "main"
STATE_KEY = "heatmap"              # the key in _lift_state.json (pending_since, last_lift_iso, error)
STAGED_RETENTION_DAYS = 7          # staged CSVs are rewritten whole every run; the lake is the store
COHORT_UNKNOWN = "unknown"
SIZE_NAMES = ("under_100k", "small_whale", "whale", "tidal_whale", "leviathan")
PERF_NAMES = ("losing_trader", "average_trader", "consistent_trader", "smart_money", "money_printer")
# HyperTracker tier ids below the first size bound (< 250, 250-10k, 10k-50k, then up to b0)
_SIZE_FINE_SUB = ((250.0, 16), (10_000.0, 1), (50_000.0, 2))
_SIZE_FINE_BANDS = (3, 4, 5, 6, 7)       # id of [sub-tiers' tail, small_whale, whale, tidal_whale, leviathan]
_USDC_COLLATERAL = {"USDC", "USDC(ASSUMED)"}


# --- settings ---------------------------------------------------------------------------

def writer() -> str:
    return (config.env("HL_HEATMAP_WRITER") or SOURCE_HOURLY).strip().lower()


def enabled() -> bool:
    """The hourly run stages and lifts the three tables: HL_HEATMAP on AND the hourly run is
    the writer (HL_HEATMAP_WRITER=sweep hands the tables to the Stage-2 sweep)."""
    return config.env_bool("HL_HEATMAP") and writer() == SOURCE_HOURLY


def parse_bounds(raw: str, name: str = "bounds", n: int = 4) -> list[float]:
    """Exactly `n` finite, strictly ascending numbers from a comma-separated string."""
    parts = [p.strip() for p in str(raw or "").split(",") if p.strip()]
    try:
        vals = [float(p) for p in parts]
    except ValueError:
        raise ValueError(f"{name} must be {n} comma-separated numbers, got {raw!r}") from None
    if len(vals) != n or any(not math.isfinite(v) for v in vals) or any(b <= a for a, b in zip(vals, vals[1:])):
        raise ValueError(f"{name} must be {n} strictly ascending finite numbers, got {raw!r}")
    return vals


def size_bounds() -> list[float]:
    return parse_bounds(config.env("HL_HEATMAP_SIZE_BOUNDS"), "HL_HEATMAP_SIZE_BOUNDS")


def pnl_bounds() -> list[float]:
    return parse_bounds(config.env("HL_HEATMAP_PNL_BOUNDS"), "HL_HEATMAP_PNL_BOUNDS")


def market_dexes() -> list[str]:
    seen: list[str] = []
    for d in (config.env("HL_HEATMAP_MARKET_DEXES") or "").split(","):
        d = d.strip()
        if d and d not in seen:
            seen.append(d)
    return seen


def lift_deadline_s() -> float:
    return config.env_float("HL_HEATMAP_LIFT_DEADLINE_S")


def settings() -> dict:
    """Resolved settings (validated), for preflight and the run report."""
    return {"enabled": config.env_bool("HL_HEATMAP"), "writer": writer(), "size_bounds": size_bounds(),
            "pnl_bounds": pnl_bounds(), "market_dexes": market_dexes(), "lift_deadline_s": lift_deadline_s(),
            "lake_retention_days": config.env_int("HL_HEATMAP_LAKE_RETENTION_DAYS"),
            "ctx_retention_days": config.env_int("HL_HEATMAP_CTX_RETENTION_DAYS")}


# --- cohorts ----------------------------------------------------------------------------

def size_cohort(perp_equity_usd, bounds: list[float] | None = None) -> tuple[str | None, int | None]:
    """(size_cohort, size_cohort_fine) of a perp equity; (None, None) when unknown."""
    e = _f(perp_equity_usd)
    if e is None:
        return None, None
    b = bounds or size_bounds()
    if e < b[0]:
        for cap, tier in _SIZE_FINE_SUB:
            if e < cap:
                return SIZE_NAMES[0], tier
        return SIZE_NAMES[0], _SIZE_FINE_BANDS[0]
    for i in range(1, len(b)):
        if e < b[i]:
            return SIZE_NAMES[i], _SIZE_FINE_BANDS[i]
    return SIZE_NAMES[len(b)], _SIZE_FINE_BANDS[len(b)]


def perf_cohort(pnl_all, bounds: list[float] | None = None) -> tuple[str, int | None]:
    """(perf_cohort, perf_cohort_fine) of an all-time pnl; ('unknown', None) when the wallet
    is not on the leaderboard (no pnl)."""
    p = _f(pnl_all)
    if p is None:
        return COHORT_UNKNOWN, None
    b = bounds or pnl_bounds()           # [-10k, 10k, 100k, 1M]
    if p >= b[3]:
        return "money_printer", 8
    if p >= b[2]:
        return "smart_money", 9
    if p >= b[1]:
        return "consistent_trader", 10
    if p >= 0:
        return "average_trader", 11
    if p > b[0]:
        return "average_trader", 12
    if p > -b[2]:
        return "losing_trader", 13
    if p > -b[3]:
        return "losing_trader", 14
    return "losing_trader", 15


# --- time / ids -------------------------------------------------------------------------

def bucket_start_ms(now_ms: int, bucket_minutes: int = BUCKET_MINUTES_HOURLY) -> int:
    """Floor of `now_ms` to the bucket grid (UTC): date_trunc('hour') for 60."""
    step = int(bucket_minutes) * 60_000
    return (int(now_ms) // step) * step


def iso(ms: int) -> str:
    return dt.datetime.fromtimestamp(ms / 1000, tz=dt.timezone.utc).isoformat()


def date_of(ms: int) -> str:
    return iso(ms)[:10]


def make_run_id(source: str, run_start_ms: int) -> str:
    return f"{source}-{int(run_start_ms)}"


def _f(v):
    """float(v) or None (None, '', non-numeric, non-finite)."""
    if v is None or v == "":
        return None
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return x if math.isfinite(x) else None


def _i(v):
    x = _f(v)
    return None if x is None else int(x)


# --- positions ---------------------------------------------------------------------------

def account_value(chs: dict | None) -> float:
    return _f(((chs or {}).get("marginSummary") or {}).get("accountValue")) or 0.0


def wallet_perp_equity(chs: dict | None, chs_by_dex: dict | None) -> float:
    """SUM over the main dex and every HIP-3 margin account of marginSummary.accountValue
    (spot, staking and vaults excluded on purpose - the size cohort is perp equity)."""
    total = account_value(chs)
    for dchs in (chs_by_dex or {}).values():
        if dchs:
            total += account_value(dchs)
    return total


def equity_has_non_usd(chs_by_dex: dict | None, collateral_by_dex: dict | None) -> bool:
    """True when a HIP-3 dex with non-USDC collateral (USDH, USDE, USDT0, ...) contributed a
    non-zero account value - its collateral is counted at 1.0 USD."""
    for dx, dchs in (chs_by_dex or {}).items():
        if not dchs or not account_value(dchs):
            continue
        sym = str((collateral_by_dex or {}).get(dx) or "USDC").upper()
        if sym not in _USDC_COLLATERAL:
            return True
    return False


def _position_rows_one_dex(state: dict, wallet: str, dex: str, equity: float, *, bucket_ms: int, bucket_minutes: int,
                           fetched_at_ms: int, run_id: str, now_iso: str, source: str) -> list[dict]:
    av = account_value(state)
    wg = S.addr_group(wallet)
    rows: list[dict] = []
    for ap in (state or {}).get("assetPositions") or []:
        p = (ap or {}).get("position") or {}
        szi = _f(p.get("szi")) or 0.0
        coin = p.get("coin")
        if szi == 0.0 or not coin:
            continue
        pos_value = _f(p.get("positionValue"))
        lev = p.get("leverage") or {}
        cf = p.get("cumFunding") or {}
        rows.append({
            "as_of_bucket": iso(bucket_ms), "bucket_minutes": int(bucket_minutes), "as_of_ts": iso(fetched_at_ms),
            "as_of_date": date_of(bucket_ms), "wallet_address": wallet, "wallet_group": wg,
            "coin": str(coin), "dex": dex or MAIN_DEX, "side": "long" if szi > 0 else "short", "szi": szi,
            "entry_px": _f(p.get("entryPx")), "position_value": pos_value,
            "mark_px": (abs(pos_value / szi) if pos_value is not None else None),
            "unrealized_pnl_usd": _f(p.get("unrealizedPnl")), "return_on_equity": _f(p.get("returnOnEquity")),
            "liquidation_px": _f(p.get("liquidationPx")), "margin_used_usd": _f(p.get("marginUsed")),
            "leverage_type": lev.get("type"), "leverage_value": _f(lev.get("value")),
            "max_leverage": _i(p.get("maxLeverage")),
            "cum_funding_all_time": _f(cf.get("allTime")), "cum_funding_since_open": _f(cf.get("sinceOpen")),
            "cum_funding_since_change": _f(cf.get("sinceChange")),
            "account_value_usd": av, "wallet_perp_equity_usd": equity,
            "source": source, "run_id": run_id, "created_at": now_iso,
        })
    return rows


def position_rows(chs: dict | None, chs_by_dex: dict | None, wallet: str, *, bucket_ms: int,
                  fetched_at_ms: int, run_id: str, now_iso: str, bucket_minutes: int = BUCKET_MINUTES_HOURLY,
                  source: str = SOURCE_HOURLY) -> list[dict]:
    """l4x_perp_position_snap rows of one wallet for one bucket from the RAW clearinghouseState
    payloads (main + {dex: state}); szi == 0 is skipped. `fetched_at_ms` = when the wallet's
    pull returned (the row's as_of_ts), not the run start."""
    wallet = wallet.lower()
    equity = wallet_perp_equity(chs, chs_by_dex)
    kw = dict(bucket_ms=bucket_ms, bucket_minutes=bucket_minutes, fetched_at_ms=fetched_at_ms, run_id=run_id,
              now_iso=now_iso, source=source)
    rows = _position_rows_one_dex(chs or {}, wallet, MAIN_DEX, equity, **kw)
    for dx in sorted(chs_by_dex or {}):
        if chs_by_dex[dx]:
            rows += _position_rows_one_dex(chs_by_dex[dx], wallet, dx, equity, **kw)
    return rows


# --- cohort rows -------------------------------------------------------------------------

def cohort_row(wallet: str, *, bucket_ms: int, run_start_ms: int, run_id: str, now_iso: str,
               snapshot: dict | None, lb_row=None, is_whale=None, is_smart=None, is_pinned=None,
               leaderboard_fetched_at: str | None = None, leaderboard_last_modified: str | None = None,
               size_b: list[float] | None = None, pnl_b: list[float] | None = None,
               bucket_minutes: int = BUCKET_MINUTES_HOURLY, source: str = SOURCE_HOURLY) -> dict:
    """One l4x_wallet_cohort_snap row. `snapshot` = {"equity", "has_non_usd", "open_positions"}
    when the wallet's clearinghouseState was fetched this bucket, None when it was deferred
    or skipped (position_snapshot_present = false, equity / size NULL: the rollup carries the
    previous bucket forward). `lb_row` = tracked.Row or None (not on the leaderboard)."""
    wallet = wallet.lower()
    present = snapshot is not None
    equity = _f(snapshot.get("equity")) if present else None
    size_name, size_fine = size_cohort(equity, size_b) if present else (None, None)
    has_open = bool(snapshot.get("open_positions")) if present else None
    win = {w: (lb_row.window(w) if lb_row is not None else (None, None, None)) for w in ("day", "week", "month", "all")}
    pnl_all = win["all"][0]
    perf_name, perf_fine = perf_cohort(pnl_all, pnl_b) if lb_row is not None else (COHORT_UNKNOWN, None)
    vlm_month = win["month"][2]
    if lb_row is None and not present:
        is_active = None
    else:
        is_active = bool((vlm_month or 0) > 0) or bool(has_open)
    return {
        "as_of_bucket": iso(bucket_ms), "bucket_minutes": int(bucket_minutes), "as_of_ts": iso(run_start_ms),
        "as_of_date": date_of(bucket_ms), "wallet_address": wallet,
        "position_snapshot_present": present, "perp_equity_usd": equity,
        "equity_source": "snapshot" if present else None,
        "equity_has_non_usd": (bool(snapshot.get("has_non_usd")) if present else None),
        "leaderboard_account_value_usd": (lb_row.account_value if lb_row is not None else None),
        "rank_account_value": (lb_row.rank_account_value if lb_row is not None else None),
        "size_cohort": size_name, "size_cohort_fine": size_fine,
        "pnl_day": win["day"][0], "pnl_week": win["week"][0], "pnl_month": win["month"][0], "pnl_all": pnl_all,
        "roi_day": win["day"][1], "roi_week": win["week"][1], "roi_month": win["month"][1], "roi_all": win["all"][1],
        "vlm_day": win["day"][2], "vlm_week": win["week"][2], "vlm_month": vlm_month, "vlm_all": win["all"][2],
        "perf_cohort": perf_name, "perf_cohort_fine": perf_fine,
        "is_tracked_whale": is_whale, "is_tracked_smart": is_smart, "is_pinned": is_pinned,
        "is_active": is_active, "has_open_position": has_open,
        "leaderboard_fetched_at": leaderboard_fetched_at, "leaderboard_last_modified": leaderboard_last_modified,
        "size_cohort_version": SIZE_COHORT_VERSION, "perf_cohort_version": PERF_COHORT_VERSION,
        "source": source, "run_id": run_id, "created_at": now_iso,
    }


def cohort_rows(planned: list[str], snapshots: dict[str, dict], *, bucket_ms: int, run_start_ms: int, run_id: str,
                now_iso: str, leaderboard_rows: dict | None = None, whales=(), smart=(), pins=(),
                leaderboard_fetched_at: str | None = None, leaderboard_last_modified: str | None = None,
                size_b: list[float] | None = None, pnl_b: list[float] | None = None,
                bucket_minutes: int = BUCKET_MINUTES_HOURLY, source: str = SOURCE_HOURLY) -> list[dict]:
    """One row per planned wallet (pins + tracked, fetched or not). The tracked flags are
    known only when the leaderboard was read this run (`leaderboard_rows` not None); a
    cached / failed refresh leaves them NULL rather than guessing."""
    size_b = size_b or size_bounds()
    pnl_b = pnl_b or pnl_bounds()
    have_lb = leaderboard_rows is not None
    whales_s = {w.lower() for w in whales or ()}
    smart_s = {w.lower() for w in smart or ()}
    pins_s = {w.lower() for w in pins or ()}
    out: list[dict] = []
    for w in dict.fromkeys(x.lower() for x in planned):
        lb = (leaderboard_rows or {}).get(w)
        out.append(cohort_row(w, bucket_ms=bucket_ms, run_start_ms=run_start_ms, run_id=run_id, now_iso=now_iso,
                              snapshot=snapshots.get(w), lb_row=lb,
                              is_whale=(w in whales_s) if have_lb else None,
                              is_smart=(w in smart_s) if have_lb else None,
                              is_pinned=(w in pins_s),
                              leaderboard_fetched_at=leaderboard_fetched_at,
                              leaderboard_last_modified=leaderboard_last_modified,
                              size_b=size_b, pnl_b=pnl_b, bucket_minutes=bucket_minutes, source=source))
    return out


# --- market context ------------------------------------------------------------------------

def market_ctx_rows(meta: dict | None, ctxs: list | None, dex: str, *, bucket_ms: int, as_of_ms: int, run_id: str,
                    now_iso: str, bucket_minutes: int = BUCKET_MINUTES_HOURLY, source: str = SOURCE_HOURLY
                    ) -> list[dict]:
    """l4x_perp_market_ctx_snap rows from one metaAndAssetCtxs response: universe[i] pairs
    with ctxs[i]; delisted assets are skipped; a HIP-3 coin without its dex prefix gets it
    (the key is dex-qualified)."""
    universe = (meta or {}).get("universe") or []
    ctxs = ctxs or []
    rows: list[dict] = []
    for i, u in enumerate(universe):
        if not isinstance(u, dict) or i >= len(ctxs):
            break
        name = u.get("name")
        if not name or u.get("isDelisted"):
            continue
        c = ctxs[i] or {}
        coin = str(name)
        if dex and dex != MAIN_DEX and ":" not in coin:
            coin = f"{dex}:{coin}"
        oi, mark = _f(c.get("openInterest")), _f(c.get("markPx"))
        rows.append({
            "as_of_bucket": iso(bucket_ms), "bucket_minutes": int(bucket_minutes), "as_of_ts": iso(as_of_ms),
            "as_of_date": date_of(bucket_ms), "coin": coin, "dex": dex or MAIN_DEX,
            "open_interest": oi, "open_interest_usd": (oi * mark if oi is not None and mark is not None else None),
            "funding_1h": _f(c.get("funding")), "mark_px": mark, "oracle_px": _f(c.get("oraclePx")),
            "prev_day_px": _f(c.get("prevDayPx")), "day_ntl_vlm": _f(c.get("dayNtlVlm")), "premium": _f(c.get("premium")),
            "max_leverage": _i(u.get("maxLeverage")),
            "source": source, "run_id": run_id, "created_at": now_iso,
        })
    return rows


def fetch_market_ctx(client, dexes: list[str], *, bucket_ms: int, run_id: str, now_iso: str, clock=time,
                     bucket_minutes: int = BUCKET_MINUTES_HOURLY, source: str = SOURCE_HOURLY) -> tuple[list[dict], dict]:
    """One metaAndAssetCtxs call per dex (20 weight each) -> rows, {dex: error}. A dex that
    fails is reported and the others still land."""
    rows: list[dict] = []
    errors: dict[str, str] = {}
    for dx in dexes:
        try:
            meta, ctxs = client.meta_and_asset_ctxs(None if dx == MAIN_DEX else dx)
            rows += market_ctx_rows(meta, ctxs, dx, bucket_ms=bucket_ms, as_of_ms=int(clock.time() * 1000),
                                    run_id=run_id, now_iso=now_iso, bucket_minutes=bucket_minutes, source=source)
        except Exception as e:  # noqa: BLE001 - one dex down must not lose the others
            errors[dx] = f"{type(e).__name__}: {str(e)[:120]}"
            log.warning("heatmap: metaAndAssetCtxs(%s) failed: %s", dx, errors[dx])
    return rows, errors


def saw_rate_limit(errors_by_wallet: dict[str, dict] | None) -> bool:
    """True when any wallet pull of the run ended in an HTTP 429 (spec 2.6 item 3: then the
    market ctx call is skipped and the rollup falls back to the previous bucket's ctx)."""
    for errs in (errors_by_wallet or {}).values():
        for v in (errs or {}).values():
            if "429" in str(v):
                return True
    return False


# --- staging (extract.upsert_csv) --------------------------------------------------------------

def stage(ns_dir: Path, positions: list[dict], cohorts: list[dict], ctx: list[dict], *, bucket_iso: str,
          scope_wallets: set[str]) -> dict:
    """Upsert the three staged CSVs by their natural keys: positions replaced per (bucket,
    wallet) for the wallets fetched this run; cohort and market ctx replaced per bucket."""
    from .extract import _s, upsert_csv
    ns_dir = Path(ns_dir)
    scope = {(bucket_iso, w) for w in scope_wallets}
    n_pos = upsert_csv(ns_dir / f"{TABLE_POSITIONS}.csv", S.L4X_PERP_POSITION_SNAP_COLUMNS, positions,
                       key_fn=lambda r: (_s(r["as_of_bucket"]), _s(r["wallet_address"]), _s(r["coin"])),
                       sort_fn=lambda r: (_s(r["as_of_bucket"]), _s(r["wallet_address"]), _s(r["coin"])),
                       replace_scope_fn=lambda r: (_s(r["as_of_bucket"]), _s(r["wallet_address"])) in scope)
    n_coh = upsert_csv(ns_dir / f"{TABLE_COHORT}.csv", S.L4X_WALLET_COHORT_SNAP_COLUMNS, cohorts,
                       key_fn=lambda r: (_s(r["as_of_bucket"]), _s(r["wallet_address"])),
                       sort_fn=lambda r: (_s(r["as_of_bucket"]), _s(r["wallet_address"])),
                       replace_scope_fn=lambda r: _s(r["as_of_bucket"]) == bucket_iso)
    n_ctx = None
    if ctx:                       # a skipped ctx call (429 / errors) keeps the bucket's earlier rows, if any
        n_ctx = upsert_csv(ns_dir / f"{TABLE_MARKET_CTX}.csv", S.L4X_PERP_MARKET_CTX_SNAP_COLUMNS, ctx,
                           key_fn=lambda r: (_s(r["as_of_bucket"]), _s(r["coin"])),
                           sort_fn=lambda r: (_s(r["as_of_bucket"]), _s(r["dex"]), _s(r["coin"])),
                           replace_scope_fn=lambda r: _s(r["as_of_bucket"]) == bucket_iso)
    return {"positions": len(positions), "cohort": len(cohorts), "market_ctx": len(ctx),
            "staged_total": {TABLE_POSITIONS: n_pos, TABLE_COHORT: n_coh, TABLE_MARKET_CTX: n_ctx}}


def prune_staged(ns_dir: Path, now_ms: int, days: int = STAGED_RETENTION_DAYS) -> dict:
    """Staged-CSV retention for the three files on as_of_ts (7 days: ~290k position rows;
    the lake keeps the history)."""
    from .extract import prune_csv
    cutoff = int(now_ms) - int(days) * 86_400_000
    return {t: prune_csv(Path(ns_dir) / f"{t}.csv", "as_of_ts", cutoff) for t in TABLES}


# --- skip guard / lift ---------------------------------------------------------------------------

def lift_skipped(run_start_ts: float, now_ts: float, deadline_s: float | None = None) -> bool:
    """True when the run is already past HL_HEATMAP_LIFT_DEADLINE_S on entering the lift."""
    d = lift_deadline_s() if deadline_s is None else float(deadline_s)
    return d > 0 and (now_ts - run_start_ts) > d


def earlier_iso(a: str | None, b: str | None) -> str | None:
    """The earlier of two ISO instants (None = absent)."""
    from .extract import _earlier
    return _earlier(a, b)


def lift_since_iso(pending_since: str | None, prev_lift_iso: str | None, run_iso: str) -> str | None:
    """Cut-off for the heatmap lift: when a bucket is pending (a skipped / failed lift) read
    from its as_of_ts minus a minute, else the main lift's bounded cut-off."""
    from .iceberg_load import lift_since, since_with_margin
    if pending_since:
        return since_with_margin(pending_since, hours=1 / 60)
    return lift_since(prev_lift_iso, run_iso)


def pending_since(lift_state: dict | None) -> str | None:
    return ((lift_state or {}).get(STATE_KEY) or {}).get("pending_since")


# --- daily seed from l4x_perp_position_snapshot (spec 4.6) ------------------------------------------

def _day_ms(day: dt.date) -> int:
    return int(dt.datetime(day.year, day.month, day.day, tzinfo=dt.timezone.utc).timestamp() * 1000)


def seed_daily_rows(l4x_rows: list[dict], *, until: dt.date, since: dt.date | None, now_iso: str, run_id: str,
                    size_b: list[float] | None = None) -> tuple[list[dict], list[dict]]:
    """(position rows, cohort rows) with bucket_minutes = 1440 / source = l4x_daily from
    l4x_perp_position_snapshot rows (dicts with the l4x columns, typed or strings) for the
    days `since` <= as_of_date < `until`. wallet_perp_equity_usd = SUM of the DISTINCT (dex,
    account_value_usd) pairs of the (day, wallet); perf cohort unknown, tracked flags NULL."""
    size_b = size_b or size_bounds()
    by_key: dict[tuple, list[dict]] = {}
    for r in l4x_rows:
        d = r.get("as_of_date")
        day = d if isinstance(d, dt.date) else dt.date.fromisoformat(str(d)[:10])
        if day >= until or (since is not None and day < since):
            continue
        szi = _f(r.get("szi")) or 0.0
        if szi == 0.0 or not r.get("coin") or not r.get("wallet_address"):
            continue
        by_key.setdefault((day, str(r["wallet_address"]).lower()), []).append(r)
    positions: list[dict] = []
    cohorts: list[dict] = []
    for (day, wallet), rows in sorted(by_key.items()):
        bucket_ms = _day_ms(day)
        equity = sum(av for _, av in {(str(r.get("dex") or MAIN_DEX), _f(r.get("account_value_usd")) or 0.0)
                                      for r in rows})
        for r in rows:
            szi = _f(r.get("szi"))
            ts = r.get("as_of_ts")
            as_of_ts = ts.astimezone(dt.timezone.utc).isoformat() if isinstance(ts, dt.datetime) else str(ts)
            positions.append({
                "as_of_bucket": iso(bucket_ms), "bucket_minutes": BUCKET_MINUTES_DAILY, "as_of_ts": as_of_ts,
                "as_of_date": day.isoformat(), "wallet_address": wallet, "wallet_group": S.addr_group(wallet),
                "coin": str(r["coin"]), "dex": str(r.get("dex") or MAIN_DEX), "side": "long" if szi > 0 else "short",
                "szi": szi, "entry_px": _f(r.get("entry_px")), "position_value": _f(r.get("position_value")),
                "mark_px": _f(r.get("mark_px")), "unrealized_pnl_usd": _f(r.get("unrealized_pnl_usd")),
                "return_on_equity": _f(r.get("return_on_equity")), "liquidation_px": _f(r.get("liquidation_px")),
                "margin_used_usd": _f(r.get("margin_used_usd")), "leverage_type": r.get("leverage_type") or None,
                "leverage_value": _f(r.get("leverage_value")), "max_leverage": None,
                "cum_funding_all_time": _f(r.get("cum_funding_all_time")),
                "cum_funding_since_open": _f(r.get("cum_funding_since_open")), "cum_funding_since_change": None,
                "account_value_usd": _f(r.get("account_value_usd")), "wallet_perp_equity_usd": equity,
                "source": SOURCE_L4X_DAILY, "run_id": run_id, "created_at": now_iso,
            })
        size_name, size_fine = size_cohort(equity, size_b)
        cohorts.append({
            "as_of_bucket": iso(bucket_ms), "bucket_minutes": BUCKET_MINUTES_DAILY, "as_of_ts": positions[-1]["as_of_ts"],
            "as_of_date": day.isoformat(), "wallet_address": wallet, "position_snapshot_present": True,
            "perp_equity_usd": equity, "equity_source": "snapshot", "equity_has_non_usd": None,
            "leaderboard_account_value_usd": None, "rank_account_value": None,
            "size_cohort": size_name, "size_cohort_fine": size_fine,
            "pnl_day": None, "pnl_week": None, "pnl_month": None, "pnl_all": None,
            "roi_day": None, "roi_week": None, "roi_month": None, "roi_all": None,
            "vlm_day": None, "vlm_week": None, "vlm_month": None, "vlm_all": None,
            "perf_cohort": COHORT_UNKNOWN, "perf_cohort_fine": None,
            "is_tracked_whale": None, "is_tracked_smart": None, "is_pinned": None,
            "is_active": True, "has_open_position": True,
            "leaderboard_fetched_at": None, "leaderboard_last_modified": None,
            "size_cohort_version": SIZE_COHORT_VERSION, "perf_cohort_version": PERF_COHORT_VERSION,
            "source": SOURCE_L4X_DAILY, "run_id": run_id, "created_at": now_iso,
        })
    return positions, cohorts


def first_hourly_date(catalog, namespace: str) -> dt.date | None:
    """The as_of_date of the earliest non-daily bucket already in l4x_perp_position_snap
    (None when the table or such rows do not exist): daily seed rows must stop before it, or
    the day's 00:00 bucket would collide with the hourly series."""
    from pyiceberg.expressions import NotEqualTo
    try:
        table = catalog.load_table(f"{namespace}.{TABLE_POSITIONS}")
    except Exception:  # noqa: BLE001 - not created yet
        return None
    arrow = table.scan(row_filter=NotEqualTo("bucket_minutes", BUCKET_MINUTES_DAILY),
                       selected_fields=("as_of_date",)).to_arrow()
    if arrow.num_rows == 0:
        return None
    import pyarrow.compute as pc
    v = pc.min(arrow.column("as_of_date")).as_py()
    return v.date() if isinstance(v, dt.datetime) else v


def seed_daily(catalog, namespace: str = S.NS_HYPERCORE, *, until: dt.date | None = None, since: dt.date | None = None,
               dry_run: bool = False, now: dt.datetime | None = None) -> dict:
    """Read hypercore.l4x_perp_position_snapshot and commit the daily seed rows directly
    into the two heatmap tables (one replace_scope commit per table + one watermark commit).
    `until` defaults to the first hourly bucket's date and may never pass it."""
    from pyiceberg.expressions import AlwaysTrue, And, GreaterThanOrEqual, LessThan
    from . import iceberg_load as il
    now = now or dt.datetime.now(tz=dt.timezone.utc)
    now_iso = now.isoformat()
    first_hourly = first_hourly_date(catalog, namespace)
    if until is None:
        if first_hourly is None:
            raise ValueError("--until is required: no hourly bucket in l4x_perp_position_snap yet to stop before")
        until = first_hourly
    elif first_hourly is not None and until > first_hourly:
        raise ValueError(f"--until {until} passes the first hourly bucket's day {first_hourly}: the daily 00:00 "
                         f"bucket would collide with the hourly series (use --until {first_hourly} or earlier)")
    src = catalog.load_table(f"{namespace}.l4x_perp_position_snapshot")
    flt = LessThan("as_of_date", until.isoformat())
    if since is not None:
        flt = And(flt, GreaterThanOrEqual("as_of_date", since.isoformat()))
    l4x = src.scan(row_filter=flt if flt is not None else AlwaysTrue()).to_arrow().to_pylist()
    run_id = make_run_id(SOURCE_L4X_DAILY, int(now.timestamp() * 1000))
    positions, cohorts = seed_daily_rows(l4x, until=until, since=since, now_iso=now_iso, run_id=run_id)
    days = sorted({r["as_of_date"] for r in cohorts})
    report: dict = {"source_rows": len(l4x), "until": until.isoformat(), "since": since.isoformat() if since else None,
                    "days": len(days), "first_day": days[0] if days else None, "last_day": days[-1] if days else None,
                    "wallets": len({r["wallet_address"] for r in cohorts}), "run_id": run_id, "dry_run": dry_run,
                    TABLE_POSITIONS: {"incoming": len(positions)}, TABLE_COHORT: {"incoming": len(cohorts)}}
    if dry_run or not positions:
        return report
    ddl = il.parse_ddl()
    pending_wm: list[tuple] = []
    for name, rows in ((TABLE_POSITIONS, positions), (TABLE_COHORT, cohorts)):
        key_cols, mode, scope_cols = il.TABLE_PLAN[name]
        spec = ddl[name]
        arrow = il.rows_to_arrow(rows, spec["columns"], tz=True)
        table = il.ensure_table(catalog, namespace, name, spec, tz=True, required=il.REQUIRED_COLS.get(name, ()))
        drift = il.schema_drift(table, spec)
        if drift:
            raise il.SchemaDriftError(f"{namespace}.{name}: schema drift {drift}")
        counts = il.load_table(table, arrow, key_cols, mode, scope_cols, required_cols=il.REQUIRED_COLS.get(name, ()))
        table.refresh()
        snap = table.current_snapshot()
        counts["table_total"] = il._table_row_count(table)
        report[name] = counts
        pending_wm.append((name, il._STAMP_COL[name], now, counts["table_total"], snap.snapshot_id if snap else None,
                           dt.date.fromisoformat(days[-1]), il.WM_STATUS_OK))
        log.info("seed-daily %-26s written=%6d days=%d total=%s", name, counts["written"], len(days), counts["table_total"])
    report["watermarks"] = il.write_watermarks(catalog, namespace, pending_wm, run_id)
    return report


# --- maintenance: lake retention (spec 2.7) ----------------------------------------------------

def retention_days() -> dict[str, int]:
    lake = max(0, config.env_int("HL_HEATMAP_LAKE_RETENTION_DAYS"))
    ctx = max(0, config.env_int("HL_HEATMAP_CTX_RETENTION_DAYS"))
    return {TABLE_POSITIONS: lake, TABLE_COHORT: lake, TABLE_MARKET_CTX: ctx}


def drop_partitions(catalog, namespace: str, *, today: dt.date | None = None, days: dict[str, int] | None = None) -> dict:
    """Delete as_of_date partitions older than the retention of each heatmap table: ONE
    delete commit per table per call (nothing when the table is absent, the retention is 0
    or no partition is that old). Never raises for a table: errors are reported."""
    from pyiceberg.expressions import LessThan
    today = today or dt.datetime.now(tz=dt.timezone.utc).date()
    days = days or retention_days()
    report: dict = {}
    for name, n in days.items():
        ident = f"{namespace}.{name}"
        if not n:
            report[ident] = {"skipped": "retention 0 (keep everything)"}
            continue
        try:
            table = catalog.load_table(ident)
        except Exception:  # noqa: BLE001 - not created yet (HL_HEATMAP off)
            report[ident] = {"skipped": "table not created"}
            continue
        cutoff = today - dt.timedelta(days=n)
        entry: dict = {"cutoff": cutoff.isoformat(), "retention_days": n, "commits": 0}
        try:
            parts = table.inspect.partitions().to_pylist()
            old = [p for p in parts if (p.get("partition") or {}).get("as_of_date") is not None
                   and _as_date((p["partition"])["as_of_date"]) < cutoff]
            entry["partitions_dropped"] = len(old)
            entry["rows_dropped"] = sum(int(p.get("record_count") or 0) for p in old)
            if old:
                table.delete(LessThan("as_of_date", cutoff.isoformat()))
                table.refresh()
                entry["commits"] = 1
                log.info("drop-partitions %-34s %d partition(s) before %s, %d rows, one commit", ident, len(old),
                         cutoff, entry["rows_dropped"])
        except Exception as e:  # noqa: BLE001
            entry["error"] = f"{type(e).__name__}: {str(e)[:160]}"
            log.warning("drop-partitions %s failed: %s", ident, entry["error"])
        report[ident] = entry
    return report


def _as_date(v) -> dt.date:
    if isinstance(v, dt.datetime):
        return v.date()
    if isinstance(v, dt.date):
        return v
    return dt.date.fromisoformat(str(v)[:10])


# --- CLI ----------------------------------------------------------------------------------------

def main(argv=None) -> int:
    """Exit codes: 0 done, 1 error, 2 bad arguments, 3 another run holds the state dir's lock."""
    config.load_env()
    config.setup_logging()
    ap = argparse.ArgumentParser(description="Token Heatmap lake tables: one-time daily seed from l4x_perp_position_snapshot")
    ap.add_argument("--seed-daily", action="store_true",
                    help="commit daily (bucket_minutes=1440, source=l4x_daily) rows into l4x_perp_position_snap / "
                         "l4x_wallet_cohort_snap for every closed day before the first hourly bucket (spec 4.6)")
    ap.add_argument("--until", default=None, help="seed days strictly before this UTC date (YYYY-MM-DD); default = the "
                                                  "first hourly bucket's day; may never pass it")
    ap.add_argument("--since", default=None, help="seed days from this UTC date on (default: everything the l4x table holds)")
    ap.add_argument("--dry-run", action="store_true", help="read and count, commit nothing")
    ap.add_argument("--namespace", default=S.NS_HYPERCORE)
    ap.add_argument("--out", default=str(config.out_root()), help="state dir (for the run lock)")
    args = ap.parse_args(argv)
    if not args.seed_daily:
        ap.print_help()
        return 2
    try:
        until = dt.date.fromisoformat(args.until) if args.until else None
        since = dt.date.fromisoformat(args.since) if args.since else None
    except ValueError as e:
        log.error("bad date: %s", e)
        return 2
    from . import ops
    from .iceberg_load import dispose_catalog, load_catalog_from_env
    ns_dir = Path(args.out) / S.NS_HYPERCORE
    try:
        with ops.RunLock(ns_dir / ".daily.lock"):
            catalog, _ = load_catalog_from_env()
            try:
                report = seed_daily(catalog, args.namespace, until=until, since=since, dry_run=args.dry_run)
            finally:
                dispose_catalog(catalog)
    except ops.LockHeld as e:
        log.error("%s - another run owns the state dir; retry between ticks", e)
        return 3
    except ValueError as e:
        log.error("%s", e)
        return 2
    print(json.dumps(report, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
