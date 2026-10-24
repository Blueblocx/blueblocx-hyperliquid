"""
Native candle prices for the ETL's token_price_usd_1h / token_price_usd_1d (v1.7.0):
hypercore.l3_token_price_usd_1h and l3_token_price_usd_1d, one row per CLOSED candle.
v1.10.0 adds the 1-minute feed (l3_token_price_usd_1m), a deep 1d history and the
token supply refresh that the token / market tables of the ETL contract need.

Runs FIRST in extract.run (before any wallet) under its own absolute time budget
(HL_PRICE_TIME_BUDGET_MIN, default 3 min), so the wallet phase keeps its slot and the
tables are lifted right after this step (watermarks follow with the main lift) -
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
  1d  one fetch once a new UTC day has closed; the FIRST fetch (and one more fetch
      when the configured depth grows) reaches HL_PRICE_HISTORY_DAYS_1D back (820:
      the market fear index calibrates against 730 trailing days) - v1.10.0
  1m  one gap fetch from the last stored closed minute (first visit: HL_PRICE_1M_DAYS
      back, 3; the API serves ~5000 candles per request, so a cursor older than that
      restarts at the cap and the hole is logged) - v1.10.0
  supply  one Info `tokenDetails` call per HL_SUPPLY_REFRESH_HOURS (24): total /
      circulating / max supply and the deploy time, kept in the state file and
      merged into l1_token_metadata by extract (supply_source 'hypercore:tokenDetails')
Only closed candles are emitted (the lift is insert-if-absent: an in-progress candle
would be frozen forever); an empty answer advances the cursor (no trades, no candle),
an API error does not. State in _price_state.json (per token: last closed 1h/1d/1m
bucket, 1d depth fetched, supply, last visit; per stable pin: last pinned buckets;
`seen`: last ledger reference per token). Tokens never visited are `history_pending`;
tokens the budget did not reach this run are `deferred`; both are run-summary
warnings and the ETL's onboarding gate is history_pending == 0
(docs/ETL_HANDOFF_HYPERCORE.md).
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
TABLE_1M = "l3_token_price_usd_1m"
MINUTE_MS = 60_000
HOUR_MS = 3_600_000
DAY_MS = 86_400_000
HOT_DAYS = 7
_CANDLE_CAP = 5000
SUPPLY_SOURCE = "hypercore:tokenDetails"


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


def supply_overrides(ns_dir: Path) -> dict[str, dict]:
    """token_address -> the supply the price step last fetched from tokenDetails
    ({total, circulating, max, deploy_ms, as_of_ms}); what extract merges into
    l1_token_metadata (total_supply, token_created_at, supply_*)."""
    state = load_state(ns_dir)
    return {a: dict(st["supply"]) for a, st in (state.get("tokens") or {}).items()
            if isinstance(st, dict) and isinstance(st.get("supply"), dict)}


# v1.10.5: a HyperCore token deployed without a real supply cap reports the raw
# uint64 or (less often) int64 maximum as totalSupply/maxSupply, scaled by its
# weiDecimals - not a real figure. Confirmed on-chain 2026-10-23 on WMNT (cap =
# 2**64-1 / 1e8), KHYPE, FXRP and the HIP-3 synthetic-stock tickers MUX/SPYX/QQQX/
# NVDAX/SKHYX (cap = 2**63-1, scaled); WMNT alone read a $102B market cap on a
# token trading at $0.53. A tolerance of 1 part per million distinguishes this from
# a genuinely huge supply that happens to be close to a round number (BONK's real
# ~88.8T, PUMP's real 1T) because neither is anywhere near a power of two.
_UINT64_MAX = 2 ** 64 - 1
_INT64_MAX = 2 ** 63 - 1
_SENTINEL_RELTOL = 1e-6
# Second pattern, same root cause by a different symptom: circulatingSupply ==
# totalSupply == maxSupply bit-for-bit (USDE: all three exactly 100,000,000,000) -
# no mint/burn was ever tracked for the token, so the three fields never diverge.
# A real, actively traded token's circulating supply is never bit-identical to its
# cap (UETH's circulating is 99999999.9674654007 against a 100000000.0 cap). The
# >= 1e9 floor keeps a genuinely small, quiet-week token from matching by chance.
_PLACEHOLDER_FLOOR = 1e9


def _is_sentinel_supply(total: float, circulating: float | None, max_supply: float | None,
                        decimals: int | None) -> bool:
    """True when `total` is not a usable supply figure (see the two patterns above)."""
    if decimals is not None:
        raw = total * (10 ** decimals)
        for sentinel in (_UINT64_MAX, _INT64_MAX):
            if abs(raw - sentinel) / sentinel < _SENTINEL_RELTOL:
                return True
    if total >= _PLACEHOLDER_FLOOR and circulating is not None and max_supply is not None:
        if circulating == total == max_supply:
            return True
    return False


def apply_supply(meta_rows: list[dict], supply: dict[str, dict]) -> int:
    """Fill the supply columns of l1_token_metadata rows from supply_overrides();
    returns how many rows got a supply. Rows of tokens without one are untouched (the
    columns stay NULL, never a fake 0)."""
    n = 0
    for r in meta_rows:
        s = supply.get(r.get("token_address"))
        if not s or s.get("total") is None:
            continue
        total = float(s["total"])
        # v1.10.8: the supply that prices a market cap is the CIRCULATING one (the
        # CoinGecko / CMC convention). tokenDetails' totalSupply is the minted amount,
        # which on HyperCore is often the whole cap parked in the deployer's wallet
        # (DRV: 100B minted, USDH: 100B minted; priced at total they read $38B and
        # $99.7B and dominated the platform-wide total market cap). total stays the
        # fallback when circulating is missing or not a sane fraction of it, and the
        # sentinel test below still looks at total.
        circulating = s.get("circulating")
        if circulating is not None and 0 < float(circulating) <= total:
            r["total_supply"] = float(circulating)
        else:
            r["total_supply"] = total
        r["total_supply_raw"] = None
        r["supply_as_of"] = prices._iso(int(s["as_of_ms"])) if s.get("as_of_ms") else None
        r["supply_source"] = SUPPLY_SOURCE
        # Unit-bridged assets (UETH, UBTC, USOL, ...: category "wrapped") report the
        # pre-minted pool (100M UETH) as their supply, not the bridged amount: a true
        # on-chain figure, but no basis for a market cap (UETH would read $247B). Flag
        # it "low"; the market-cap consumers skip low-confidence supplies.
        from .rollups.categories import categories_for
        wrapped = "wrapped" in categories_for(r.get("symbol"), bool(r.get("is_stablecoin")), set())
        decimals_raw = r.get("decimals")
        decimals = int(decimals_raw) if decimals_raw not in (None, "") else None
        sentinel = _is_sentinel_supply(total, s.get("circulating"), s.get("max"), decimals)
        r["supply_confidence"] = "low" if (wrapped or sentinel) else "high"
        if s.get("deploy_ms"):
            r["token_created_at"] = prices._iso(int(s["deploy_ms"]))
        n += 1
    return n


def staged_rows_with_supply(csv_path: Path, supply: dict[str, dict], exclude, now_iso: str) -> list[dict]:
    """l1_token_metadata rows of tokens that have a supply but are NOT in `exclude` (the
    tokens this run's ledger referenced), taken from the staged CSV with a fresh write
    stamp: the refresh rotates over every priceable token, while a run only rebuilds the
    rows of the tokens it saw, so a quiet token's supply would otherwise never land."""
    import csv
    if not supply or not Path(csv_path).exists():
        return []
    out = []
    with Path(csv_path).open("r", newline="", encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            addr = r.get("token_address")
            if addr in supply and addr not in exclude:
                row = dict(r)
                row["updated_at"] = now_iso
                out.append(row)
    return out


def parse_token_details(details: dict, now_ms: int) -> dict | None:
    """The supply record kept in the state from one tokenDetails answer."""
    def num(key):
        v = details.get(key)
        try:
            return float(v) if v not in (None, "") else None
        except (TypeError, ValueError):
            return None
    total = num("totalSupply")
    if total is None:
        return None
    deploy = details.get("deployTime")
    deploy_ms = None
    if deploy:
        try:
            from datetime import datetime, timezone
            s = str(deploy).replace("Z", "+00:00")
            d = datetime.fromisoformat(s)
            if d.tzinfo is None:
                d = d.replace(tzinfo=timezone.utc)
            deploy_ms = int(d.timestamp() * 1000)
        except ValueError:
            deploy_ms = None
    return {"total": total, "circulating": num("circulatingSupply"), "max": num("maxSupply"),
            "deploy_ms": deploy_ms, "as_of_ms": int(now_ms)}


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
        clock=_time, token_meta: dict | None = None, history_days_1d: int | None = None,
        minute_days: float = 0.0, supply_refresh_h: float = 0.0) -> dict:
    """Fetch what the budget allows, upsert the staged CSVs, persist the state;
    returns the report extract.run records as recon["prices"].
    `history_days_1d` (default = history_days) is the depth of the 1d history per token,
    `minute_days` > 0 turns the 1-minute feed on, `supply_refresh_h` > 0 the
    tokenDetails supply refresh (both v1.10.0; off = the v1.7 behaviour)."""
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
    history_days_1d = int(history_days if history_days_1d is None else max(history_days_1d, history_days))
    history_start = int(now_ms) - int(history_days) * DAY_MS
    first_1h = (history_start // HOUR_MS) * HOUR_MS           # first visit: whole buckets from the grid
    first_1d = ((int(now_ms) - history_days_1d * DAY_MS) // DAY_MS) * DAY_MS
    last_closed_1h = ((now_ms - HOUR_MS) // HOUR_MS) * HOUR_MS
    last_closed_1d = ((now_ms - DAY_MS) // DAY_MS) * DAY_MS
    last_closed_1m = ((now_ms - MINUTE_MS) // MINUTE_MS) * MINUTE_MS
    minute_on = minute_days and minute_days > 0
    # the API serves ~5000 candles per request: a 1m cursor further back than that
    # cannot be caught up, the fetch restarts at the cap (hole logged)
    cap_1m = ((now_ms - (_CANDLE_CAP - 10) * MINUTE_MS) // MINUTE_MS) * MINUTE_MS
    first_1m = max(((int(now_ms) - int(minute_days * DAY_MS)) // MINUTE_MS) * MINUTE_MS, cap_1m) if minute_on else None
    supply_refresh_ms = int(supply_refresh_h * HOUR_MS) if supply_refresh_h and supply_refresh_h > 0 else None
    details_fn = None
    if supply_refresh_ms:
        try:
            details_fn = getattr(client, "token_details", None)
        except Exception:  # noqa: BLE001 - a client without the call (older stubs) just means no supply
            details_fn = None
    rows_1h: list[dict] = []
    rows_1d: list[dict] = []
    rows_1m: list[dict] = []
    errors: dict[str, str] = {}
    fetched_1h = fetched_1d = fetched_1m = fetched_supply = 0
    holes_1m: list[str] = []
    visited: list[str] = []
    deferred: list[str] = []

    def err(addr: str, msg: str) -> None:
        errors[addr] = (errors.get(addr, "") + " " + msg).strip()

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
                err(addr, f"1h: {type(e).__name__}: {str(e)[:80]}")
                candles = None
            if candles is not None:
                closed = _closed_candles(candles, now_ms)
                rows_1h += prices.candles_to_price_1m(addr, closed, now_iso)
                st["last_1h_ms"] = max([int(c["t"]) for c in closed] + [last_closed_1h])
                fetched_1h += 1
        # 1d: the gap since the last closed day; the first fetch (or a deeper configured
        # history than the one on record) reaches history_days_1d back in ONE call
        deep_pending = int(st.get("hist_1d_days") or 0) < history_days_1d
        start_d = first_1d if (deep_pending or st.get("last_1d_ms") is None) else int(st["last_1d_ms"]) + DAY_MS
        if start_d <= last_closed_1d:
            try:
                candles = client.candle_snapshot(pair.candle_coin, "1d", start_d, now_ms,
                                                 est_items=min(_CANDLE_CAP, (now_ms - start_d) // DAY_MS + 2))
            except Exception as e:  # noqa: BLE001
                err(addr, f"1d: {type(e).__name__}: {str(e)[:80]}")
                candles = None
            if candles is not None:
                closed = _closed_candles(candles, now_ms)
                rows_1d += prices.candles_to_price_1m(addr, closed, now_iso)
                st["last_1d_ms"] = max([int(c["t"]) for c in closed] + [last_closed_1d])
                st["hist_1d_days"] = history_days_1d
                fetched_1d += 1
        # 1m: the gap since the last closed minute (v1.10.0)
        if minute_on:
            start_m = (int(st["last_1m_ms"]) + MINUTE_MS) if st.get("last_1m_ms") is not None else first_1m
            if start_m < cap_1m:
                holes_1m.append(f"{meta.get(addr, {}).get('symbol') or addr}:{(cap_1m - start_m) // MINUTE_MS}min")
                start_m = cap_1m
            if start_m <= last_closed_1m:
                try:
                    candles = client.candle_snapshot(pair.candle_coin, "1m", start_m, now_ms,
                                                     est_items=min(_CANDLE_CAP, (now_ms - start_m) // MINUTE_MS + 2))
                except Exception as e:  # noqa: BLE001
                    err(addr, f"1m: {type(e).__name__}: {str(e)[:80]}")
                    candles = None
                if candles is not None:
                    closed = _closed_candles(candles, now_ms)
                    rows_1m += prices.candles_to_price_1m(addr, closed, now_iso)
                    st["last_1m_ms"] = max([int(c["t"]) for c in closed] + [last_closed_1m])
                    fetched_1m += 1
        # supply: one tokenDetails call per refresh period (v1.10.0)
        if details_fn is not None and pair.base.token_id and \
                int(now_ms) - int(st.get("supply_ms") or 0) >= supply_refresh_ms and clock.time() < deadline:
            try:
                rec = parse_token_details(details_fn(pair.base.token_id) or {}, now_ms)
            except Exception as e:  # noqa: BLE001
                err(addr, f"supply: {type(e).__name__}: {str(e)[:80]}")
                rec = None
            if rec is not None:
                st["supply"] = rec
                st["supply_ms"] = int(now_ms)
                fetched_supply += 1
        st["visited_ms"] = int(now_ms)
        visited.append(addr)
    # stable pins: no API weight, always complete
    for addr in sorted(stables):
        pst = state["pins"].setdefault(addr, {})
        grids = [("last_1h_ms", HOUR_MS, first_1h, rows_1h, None),
                 ("last_1d_ms", DAY_MS, first_1d, rows_1d, "hist_1d_days")]
        if minute_on:
            grids.append(("last_1m_ms", MINUTE_MS, first_1m, rows_1m, None))
        for key, step, first, sink, depth_key in grids:
            deep = depth_key is not None and int(pst.get(depth_key) or 0) < history_days_1d
            s = (int(pst[key]) + step) if pst.get(key) is not None and not deep else first
            grid = interval_grid(s, now_ms, step)
            if grid:
                sink.extend(prices.stable_price_1m(addr, grid, now_iso))
                pst[key] = max(grid[-1], int(pst.get(key) or 0))
            if depth_key is not None:
                pst[depth_key] = history_days_1d
    key_fn = lambda r: (str(r["token_address"]), str(r["bucket_minute"]))  # noqa: E731
    if rows_1h:
        upsert_csv(ns_dir / f"{TABLE_1H}.csv", S.L3_TOKEN_PRICE_USD_1H_COLUMNS, rows_1h, key_fn=key_fn, sort_fn=key_fn)
    if rows_1d:
        upsert_csv(ns_dir / f"{TABLE_1D}.csv", S.L3_TOKEN_PRICE_USD_1D_COLUMNS, rows_1d, key_fn=key_fn, sort_fn=key_fn)
    if rows_1m:
        upsert_csv(ns_dir / f"{TABLE_1M}.csv", S.L3_TOKEN_PRICE_USD_1M_COLUMNS, rows_1m, key_fn=key_fn, sort_fn=key_fn)
    save_state(ns_dir, state)
    history_pending = sum(1 for a in universe if (state["tokens"].get(a) or {}).get("last_1h_ms") is None)
    supply_known = sum(1 for a in universe if isinstance((state["tokens"].get(a) or {}).get("supply"), dict))
    report = {
        "budget_s": budget_s, "spent_s": round(clock.time() - t0, 1), "tokens": len(universe), "hot": n_hot,
        "visited": len(visited), "deferred": len(deferred), "history_pending": history_pending,
        "fetched_1h": fetched_1h, "fetched_1d": fetched_1d, "rows_1h": len(rows_1h), "rows_1d": len(rows_1d),
        "stable_pins": sorted(stables), "unpriceable": unpriceable, "errors": errors,
    }
    if minute_on:
        report.update({"fetched_1m": fetched_1m, "rows_1m": len(rows_1m), "holes_1m": holes_1m})
    if supply_refresh_ms:
        report.update({"fetched_supply": fetched_supply, "supply_known": supply_known})
    log.info("prices: %d token(s) (%d hot), visited %d, deferred %d, history pending %d, rows 1h=%d 1d=%d 1m=%d, "
             "supply fetched %d, errors %d, %.1fs of %.0fs", len(universe), n_hot, len(visited), len(deferred),
             history_pending, len(rows_1h), len(rows_1d), len(rows_1m), fetched_supply, len(errors),
             report["spent_s"], budget_s)
    return report
