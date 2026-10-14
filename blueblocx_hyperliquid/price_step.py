"""
Native candle prices for the ETL's token_price_usd_1h / token_price_usd_1d (v1.7.0):
hypercore.l3_token_price_usd_1h and l3_token_price_usd_1d, one row per CLOSED candle.

Runs FIRST in extract.run (before any wallet) under its own absolute time budget
(HL_PRICE_TIME_BUDGET_MIN, default 3 min), so the wallet phase keeps its slot and the
two tables are lifted right after this step (watermarks follow with the main lift) -
they land even when the wallet phase defers or fails.

Universe = every non-stable token the ledger has ever referenced (the staged
l1_token_metadata.csv) that has a USDC spot market in spotMeta, plus the stable pins
(USDC and every stablecoin in the registry) at 1.0 on the same grids. No perp
reference rows (hlcore:perp:*): perps are not tokens, they are valued through their
collateral. Tokens without a USDC market are reported as `unpriceable`.

Two-tier rotation under the budget: tier 1 = tokens referenced by ledger rows in the
last HOT_DAYS days (extract records them per run via record_seen), fetched every run,
most stale first; tier 2 = the rest, most stale first, as far as the budget allows
(never-visited tokens come first within a tier). Per token:
  1h  one gap fetch from the last stored closed candle to now (first visit:
      HL_PRICE_HISTORY_DAYS back, default 32 = the ETL's 32-day load scope)
  1d  one fetch once a new UTC day has closed (same first-visit depth)
Only closed candles are emitted (the lift is insert-if-absent: an in-progress candle
would be frozen forever); an empty answer advances the cursor (no trades, no candle),
an API error does not. State in _price_state.json (per token: last closed 1h/1d
bucket, last visit; per stable pin: last pinned buckets; `seen`: last ledger
reference per token). Tokens never visited are `history_pending`; tokens the budget
did not reach this run are `deferred`; both are run-summary warnings and the ETL's
onboarding gate is history_pending == 0 (docs/ETL_HANDOFF_HYPERCORE.md).
"""
from __future__ import annotations

import json
import logging
import time as _time
from pathlib import Path

from . import prices
from . import schema as S
from .ops import atomic_write_text
from .spot_meta import _STABLE_SYMBOLS

log = logging.getLogger("blueblocx_hyperliquid.price_step")

STATE_FILE = "_price_state.json"
TABLE_1H = "l3_token_price_usd_1h"
TABLE_1D = "l3_token_price_usd_1d"
HOUR_MS = 3_600_000
DAY_MS = 86_400_000
HOT_DAYS = 7
_CANDLE_CAP = 5000


def interval_grid(start_ms: int, end_ms: int, step_ms: int) -> list[int]:
    """Aligned bucket opens t with t >= start_ms and t + step_ms <= end_ms: every bucket
    that is CLOSED by end_ms, none in progress."""
    first = -(-int(start_ms) // step_ms) * step_ms
    last = ((int(end_ms) - step_ms) // step_ms) * step_ms
    return list(range(first, last + 1, step_ms)) if last >= first else []


def load_state(ns_dir: Path) -> dict:
    p = Path(ns_dir) / STATE_FILE
    if p.exists():
        try:
            st = json.loads(p.read_text(encoding="utf-8"))
            if isinstance(st, dict):
                return {"tokens": st.get("tokens") or {}, "pins": st.get("pins") or {}, "seen": st.get("seen") or {}}
        except Exception:  # noqa: BLE001 - a corrupt state file means "first visit again", never a crash
            log.warning("%s unreadable - starting the price state afresh", p)
    return {"tokens": {}, "pins": {}, "seen": {}}


def save_state(ns_dir: Path, state: dict) -> None:
    atomic_write_text(Path(ns_dir) / STATE_FILE, json.dumps(state, indent=1, sort_keys=True))


def record_seen(ns_dir: Path, token_addrs, now_ms: int) -> None:
    """Remember that the ledger referenced these tokens now (the hot tier of the next run)."""
    addrs = {a for a in token_addrs if a}
    if not addrs:
        return
    state = load_state(ns_dir)
    for a in addrs:
        state["seen"][a] = max(int(now_ms), int(state["seen"].get(a) or 0))
    save_state(ns_dir, state)


def plan_order(universe, state: dict, now_ms: int) -> tuple[list[str], int]:
    """(token addresses in fetch order, number of hot tokens): hot tier first, within a
    tier never-visited first, then the most stale (oldest closed 1h bucket)."""
    hot_cut = now_ms - HOT_DAYS * DAY_MS
    toks = state.get("tokens") or {}
    seen = state.get("seen") or {}

    def stale_key(a):
        st = toks.get(a) or {}
        return (st.get("last_1h_ms") is not None, int(st.get("last_1h_ms") or 0), a)
    hot = sorted((a for a in universe if int(seen.get(a) or 0) >= hot_cut), key=stale_key)
    cold = sorted((a for a in universe if int(seen.get(a) or 0) < hot_cut), key=stale_key)
    return hot + cold, len(hot)


def _closed_candles(candles, now_ms: int) -> list[dict]:
    return [c for c in candles if int(c.get("T", 0) or 0) <= now_ms]


def run(client, resolver, ns_dir: Path, now_ms: int, now_iso: str, *, budget_s: float, history_days: int,
        clock=_time, token_meta: dict | None = None) -> dict:
    """Fetch what the budget allows, upsert the two staged CSVs, persist the state;
    returns the report extract.run records as recon["prices"]."""
    from .extract import _load_token_meta, upsert_csv      # lazy: extract imports this module
    t0 = clock.time()
    deadline = t0 + budget_s
    ns_dir = Path(ns_dir)
    state = load_state(ns_dir)
    meta = token_meta if token_meta is not None else _load_token_meta(ns_dir / "l1_token_metadata.csv")
    pairs = resolver.usdc_pairs()
    stables = {S.USDC_ADDRESS} | {a for a, m in meta.items()
                                  if m.get("is_stablecoin") or (m.get("symbol") or "").upper() in _STABLE_SYMBOLS}
    universe = {a: pairs[a] for a in meta if a not in stables and a in pairs}
    unpriceable = sorted((meta[a].get("symbol") or a) for a in meta if a not in stables and a not in pairs)
    order, n_hot = plan_order(universe, state, now_ms)
    history_start = int(now_ms) - int(history_days) * DAY_MS
    first_1h = (history_start // HOUR_MS) * HOUR_MS           # first visit: whole buckets from the grid
    first_1d = (history_start // DAY_MS) * DAY_MS
    last_closed_1h = ((now_ms - HOUR_MS) // HOUR_MS) * HOUR_MS
    last_closed_1d = ((now_ms - DAY_MS) // DAY_MS) * DAY_MS
    rows_1h: list[dict] = []
    rows_1d: list[dict] = []
    errors: dict[str, str] = {}
    fetched_1h = fetched_1d = 0
    visited: list[str] = []
    deferred: list[str] = []
    for i, addr in enumerate(order):
        if clock.time() >= deadline:
            deferred = order[i:]
            break
        pair = universe[addr]
        st = state["tokens"].setdefault(addr, {})
        start = (int(st["last_1h_ms"]) + HOUR_MS) if st.get("last_1h_ms") is not None else first_1h
        if start <= last_closed_1h:
            try:
                candles = client.candle_snapshot(pair.candle_coin, "1h", start, now_ms,
                                                 est_items=min(_CANDLE_CAP, (now_ms - start) // HOUR_MS + 2))
            except Exception as e:  # noqa: BLE001 - this token waits for the next run; the cursor stays
                errors[addr] = f"1h: {type(e).__name__}: {str(e)[:80]}"
                candles = None
            if candles is not None:
                closed = _closed_candles(candles, now_ms)
                rows_1h += prices.candles_to_price_1m(addr, closed, now_iso)
                st["last_1h_ms"] = max([int(c["t"]) for c in closed] + [last_closed_1h])
                fetched_1h += 1
        start_d = (int(st["last_1d_ms"]) + DAY_MS) if st.get("last_1d_ms") is not None else first_1d
        if start_d <= last_closed_1d:
            try:
                candles = client.candle_snapshot(pair.candle_coin, "1d", start_d, now_ms,
                                                 est_items=min(_CANDLE_CAP, (now_ms - start_d) // DAY_MS + 2))
            except Exception as e:  # noqa: BLE001
                errors[addr] = (errors.get(addr, "") + f" 1d: {type(e).__name__}: {str(e)[:80]}").strip()
                candles = None
            if candles is not None:
                closed = _closed_candles(candles, now_ms)
                rows_1d += prices.candles_to_price_1m(addr, closed, now_iso)
                st["last_1d_ms"] = max([int(c["t"]) for c in closed] + [last_closed_1d])
                fetched_1d += 1
        st["visited_ms"] = int(now_ms)
        visited.append(addr)
    # stable pins: no API weight, always complete
    for addr in sorted(stables):
        pst = state["pins"].setdefault(addr, {})
        for key, step, first, sink in (("last_1h_ms", HOUR_MS, first_1h, rows_1h),
                                       ("last_1d_ms", DAY_MS, first_1d, rows_1d)):
            s = (int(pst[key]) + step) if pst.get(key) is not None else first
            grid = interval_grid(s, now_ms, step)
            if grid:
                sink.extend(prices.stable_price_1m(addr, grid, now_iso))
                pst[key] = grid[-1]
    key_fn = lambda r: (str(r["token_address"]), str(r["bucket_minute"]))  # noqa: E731
    if rows_1h:
        upsert_csv(ns_dir / f"{TABLE_1H}.csv", S.L3_TOKEN_PRICE_USD_1H_COLUMNS, rows_1h, key_fn=key_fn, sort_fn=key_fn)
    if rows_1d:
        upsert_csv(ns_dir / f"{TABLE_1D}.csv", S.L3_TOKEN_PRICE_USD_1D_COLUMNS, rows_1d, key_fn=key_fn, sort_fn=key_fn)
    save_state(ns_dir, state)
    history_pending = sum(1 for a in universe if (state["tokens"].get(a) or {}).get("last_1h_ms") is None)
    report = {
        "budget_s": budget_s, "spent_s": round(clock.time() - t0, 1), "tokens": len(universe), "hot": n_hot,
        "visited": len(visited), "deferred": len(deferred), "history_pending": history_pending,
        "fetched_1h": fetched_1h, "fetched_1d": fetched_1d, "rows_1h": len(rows_1h), "rows_1d": len(rows_1d),
        "stable_pins": sorted(stables), "unpriceable": unpriceable, "errors": errors,
    }
    log.info("prices: %d token(s) (%d hot), visited %d, deferred %d, history pending %d, rows 1h=%d 1d=%d, "
             "errors %d, %.1fs of %.0fs", len(universe), n_hot, len(visited), len(deferred), history_pending,
             len(rows_1h), len(rows_1d), len(errors), report["spent_s"], budget_s)
    return report
