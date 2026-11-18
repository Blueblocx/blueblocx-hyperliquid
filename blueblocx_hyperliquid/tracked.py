"""
Which wallets a run pulls. The Hyperliquid Info API serves data per address (every
call we make carries `user`), and the per-IP weight cap makes "every wallet on the
chain" impossible anyway, so the scheduled run needs a list. Two inputs, merged:

  * pins - the wallets file (HL_WALLETS_FILE, /etc/hypercore/wallets.txt): wallets
    the product must always cover. Optional once a tracked source is configured.
  * tracked set (HL_TRACKED_SOURCE=leaderboard) - the same definition the TS
    platform-backend uses for its live whale / smart-money sweeps
    (src/hyperliquid/hyperliquid.constants.ts, refreshLeaderboard):
        whales = the top HL_WHALE_TOP_N leaderboard rows by accountValue
        smart  = rows with accountValue >= HL_SMART_MIN_ACCOUNT_VALUE,
                 month roi >= HL_SMART_MIN_MONTH_ROI, month volume >= HL_SMART_MIN_MONTH_VLM,
                 ranked by month pnl (never roi: dust accounts print absurd roi),
                 top HL_SMART_TOP_N
    read from https://stats-data.hyperliquid.xyz/Mainnet/leaderboard (~39 MB, ~47k
    rows, a separate stats host: no Info API weight) once per run and cached in
    HL_OUT_DIR/hypercore/_tracked_wallets.json. When the stats host is down the
    cached set is reused for HL_TRACKED_MAX_AGE_HOURS, then the run falls back to
    the pins and reports `tracked_unavailable`.

Run order: pins first, then smart, then whales. The auto part is rotated by a
cursor saved after every run, so when the soft deadline (HL_RUN_SOFT_DEADLINE_MIN,
extract.run deadline_ts) defers the tail of a too-long list, the next run starts
where this one stopped instead of starving the same wallets every hour.
"""
from __future__ import annotations

import datetime as dt
import inspect
import json
import logging
import math
import re
import time
from dataclasses import dataclass
from pathlib import Path

from . import config
from .ops import atomic_write_text

log = logging.getLogger("blueblocx_hyperliquid.tracked")

TRACKED_FILE = "_tracked_wallets.json"
# rows below this are dropped while scanning the payload (TS: LEADERBOARD_MIN_ACCOUNT_VALUE);
# the effective scan floor is min(this, HL_SMART_MIN_ACCOUNT_VALUE) so that setting always acts
LEADERBOARD_MIN_ACCOUNT_VALUE = 100_000.0
BYTE_CAP = 80 * 1024 * 1024            # the payload is ~39 MB (2026-10); a runaway body aborts
TOTAL_TIMEOUT_S = 120.0                # whole download, like the TS LEADERBOARD_TOTAL_TIMEOUT_MS
CHUNK = 64 * 1024                      # small enough that the caps are checked often
_ADDR = re.compile(r"^0x[0-9a-f]{40}$")


# the leaderboard's windowPerformances keys, in file order; `allTime` is the perf-cohort metric
WINDOWS = ("day", "week", "month", "allTime")


@dataclass(frozen=True)
class Row:
    """One leaderboard row above the scan floor. The month window drives the smart-money
    selection (as in the TS backend); v1.11.0 keeps the other three windows as well, for
    the heatmap's l4x_wallet_cohort_snap (pnl / roi / vlm per window, allTime.pnl = the
    perf cohort). `rank_account_value` (1 = largest accountValue among the parsed rows)
    is filled by parse_leaderboard once every row is known."""
    address: str
    account_value: float
    month_pnl: float
    month_roi: float
    month_vlm: float
    day_pnl: float = 0.0
    day_roi: float = 0.0
    day_vlm: float = 0.0
    week_pnl: float = 0.0
    week_roi: float = 0.0
    week_vlm: float = 0.0
    all_pnl: float = 0.0
    all_roi: float = 0.0
    all_vlm: float = 0.0
    rank_account_value: int | None = None

    def window(self, name: str) -> tuple[float, float, float]:
        """(pnl, roi, vlm) of a window: day | week | month | allTime (or all)."""
        k = {"day": "day", "week": "week", "month": "month", "allTime": "all", "all": "all"}[name]
        return (getattr(self, f"{k}_pnl"), getattr(self, f"{k}_roi"), getattr(self, f"{k}_vlm"))


def _num(v) -> float:
    """parseFloat(String(v)) with non-finite -> 0, like the TS scanner."""
    try:
        f = float(v)
    except (TypeError, ValueError):
        return 0.0
    return f if math.isfinite(f) else 0.0


# --- leaderboard payload -------------------------------------------------------------

def parse_leaderboard(text: str, *, min_account_value: float = LEADERBOARD_MIN_ACCOUNT_VALUE
                      ) -> tuple[list[Row], int]:
    """(rows above the floor, total row count) from {"leaderboardRows":[{...},...]}.

    Rows are decoded one at a time with JSONDecoder.raw_decode: json.loads of the whole
    payload would materialise ~47k nested rows (hundreds of MB) to keep ~15k numbers.
    """
    key = text.find('"leaderboardRows"')
    if key < 0:
        raise ValueError("payload has no leaderboardRows key")
    idx = text.index("[", key) + 1
    dec = json.JSONDecoder()
    n = len(text)
    rows: list[Row] = []
    total = 0
    while True:
        while idx < n and text[idx] in " \t\r\n,":
            idx += 1
        if idx >= n:
            raise ValueError("unterminated leaderboardRows array")
        if text[idx] == "]":
            break
        raw, idx = dec.raw_decode(text, idx)
        total += 1
        if not isinstance(raw, dict):
            continue
        addr = str(raw.get("ethAddress") or "").lower()
        av = _num(raw.get("accountValue"))
        if not _ADDR.match(addr) or av < min_account_value:
            continue
        win: dict[str, dict] = {}
        for item in raw.get("windowPerformances") or []:
            if isinstance(item, (list, tuple)) and len(item) == 2 and item[0] in WINDOWS:
                win[item[0]] = item[1] if isinstance(item[1], dict) else {}
        month, day, week, allt = (win.get(k) or {} for k in ("month", "day", "week", "allTime"))
        rows.append(Row(addr, av, _num(month.get("pnl")), _num(month.get("roi")), _num(month.get("vlm")),
                        _num(day.get("pnl")), _num(day.get("roi")), _num(day.get("vlm")),
                        _num(week.get("pnl")), _num(week.get("roi")), _num(week.get("vlm")),
                        _num(allt.get("pnl")), _num(allt.get("roi")), _num(allt.get("vlm"))))
    return rank_rows(rows), total


def rank_rows(rows: list[Row]) -> list[Row]:
    """The same rows (same order) with `rank_account_value` filled: 1 = the largest
    accountValue among `rows` (ties keep file order). Rows below the scan floor are not
    counted, so the rank is 'among the leaderboard rows above the floor'."""
    order = sorted(range(len(rows)), key=lambda i: -rows[i].account_value)
    rank = {i: n + 1 for n, i in enumerate(order)}
    return [Row(**{**r.__dict__, "rank_account_value": rank[i]}) for i, r in enumerate(rows)]


def fetch_leaderboard_text(url: str, *, timeout: tuple[float, float] = (10.0, 120.0),
                           byte_cap: int = BYTE_CAP, total_timeout: float = TOTAL_TIMEOUT_S,
                           chunk_size: int = CHUNK, get=None, clock=None, headers_out: dict | None = None) -> str:
    """Stream the payload into memory under a byte cap and a wall-clock cap. The
    request timeout alone bounds connect and per-read stalls, not a slow trickle; the
    wall clock starts before the request, like the TS AbortController.

    `headers_out` (v1.11.0): a dict that receives the response's `Last-Modified` header
    (as `last_modified`, the raw HTTP-date string) - evidence of the stats file's refresh
    cadence, kept in _tracked_wallets.json and l4x_wallet_cohort_snap."""
    if get is None:
        import requests
        get = requests.get
    clock = clock or time.monotonic
    t0 = clock()
    r = get(url, stream=True, timeout=timeout)
    try:
        r.raise_for_status()
        if headers_out is not None:
            try:
                headers_out["last_modified"] = (getattr(r, "headers", None) or {}).get("Last-Modified")
            except Exception:  # noqa: BLE001 - a stand-in without headers
                headers_out["last_modified"] = None
        buf = bytearray()
        for chunk in r.iter_content(chunk_size):
            buf += chunk
            if len(buf) > byte_cap:
                raise ValueError(f"leaderboard exceeded the byte cap ({len(buf)} > {byte_cap} bytes)")
            if clock() - t0 > total_timeout:
                raise ValueError(f"leaderboard download exceeded {total_timeout:g} s ({len(buf)} bytes so far)")
    finally:
        r.close()
    return buf.decode("utf-8")


def select_tracked(rows: list[Row], *, whale_top_n: int, smart_top_n: int,
                   smart_min_account_value: float, smart_min_month_roi: float,
                   smart_min_month_vlm: float) -> dict[str, list[str]]:
    """The TS refreshLeaderboard selection, address lists in rank order."""
    whales = [r.address for r in sorted(rows, key=lambda r: -r.account_value)[:max(0, whale_top_n)]]
    cands = [r for r in rows
             if r.account_value >= smart_min_account_value
             and r.month_roi >= smart_min_month_roi
             and r.month_vlm >= smart_min_month_vlm]
    smart = [r.address for r in sorted(cands, key=lambda r: -r.month_pnl)[:max(0, smart_top_n)]]
    return {"whales": whales, "smart": smart}


def settings() -> dict:
    return {
        "url": config.env("HL_LEADERBOARD_URL"),
        "whale_top_n": config.env_int("HL_WHALE_TOP_N"),
        "smart_top_n": config.env_int("HL_SMART_TOP_N"),
        "smart_min_account_value": config.env_float("HL_SMART_MIN_ACCOUNT_VALUE"),
        "smart_min_month_roi": config.env_float("HL_SMART_MIN_MONTH_ROI"),
        "smart_min_month_vlm": config.env_float("HL_SMART_MIN_MONTH_VLM"),
        "max_age_h": config.env_float("HL_TRACKED_MAX_AGE_HOURS"),
    }


# --- cache + cursor (HL_OUT_DIR/hypercore/_tracked_wallets.json) -----------------------

def load_cached(ns_dir: Path) -> dict:
    p = Path(ns_dir) / TRACKED_FILE
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {}


def save_cached(ns_dir: Path, data: dict) -> None:
    atomic_write_text(Path(ns_dir) / TRACKED_FILE, json.dumps(data, indent=2))


def load_cursor(ns_dir: Path) -> int:
    try:
        return max(0, int(load_cached(ns_dir).get("cursor", 0) or 0))
    except (TypeError, ValueError):
        return 0


def save_cursor(ns_dir: Path, cursor: int) -> None:
    data = load_cached(ns_dir)
    data["cursor"] = int(cursor)
    save_cached(ns_dir, data)


def _accepts_headers_out(fn) -> bool:
    try:
        params = inspect.signature(fn).parameters
    except (TypeError, ValueError):
        return False
    return "headers_out" in params or any(p.kind is inspect.Parameter.VAR_KEYWORD for p in params.values())


def http_date_to_iso(value: str | None) -> str | None:
    """RFC 7231 HTTP-date ('Sat, 08 Nov 2026 19:22:59 GMT') -> ISO 8601 UTC; None when absent
    or unparseable (never raises: the header is evidence, not an input)."""
    if not value:
        return None
    try:
        from email.utils import parsedate_to_datetime
        d = parsedate_to_datetime(str(value))
    except (TypeError, ValueError, IndexError):
        return None
    if d is None:
        return None
    if d.tzinfo is None:
        d = d.replace(tzinfo=dt.timezone.utc)
    return d.astimezone(dt.timezone.utc).isoformat()


def refresh_tracked(ns_dir: Path, *, now: float | None = None, fetch=None, keep_rows_for=(), **overrides
                    ) -> tuple[dict[str, list[str]], dict]:
    """Fetch + select + cache. Returns (sets, meta); sets = {"whales": [...], "smart": [...]}.

    meta.source: "fresh" (fetched now) | "cache" (stats host failed, cached set within
    HL_TRACKED_MAX_AGE_HOURS reused; meta.error says why) | "none" (nothing usable;
    sets are empty). Never raises: the caller decides what an empty set means.

    v1.11.0: on a fresh fetch meta also carries `rows_by_address` ({address: Row} for the
    selected wallets plus `keep_rows_for`, e.g. the pins - the heatmap's cohort rows read
    the four windows and the rank from it) and `last_modified` (the stats file's HTTP
    Last-Modified as ISO, also logged and cached). A cached / failed refresh has no rows
    (the heatmap then writes perf_cohort = unknown and NULL leaderboard columns).
    """
    s = settings()
    s.update(overrides)
    now = time.time() if now is None else now
    cached = load_cached(ns_dir)
    try:
        hdrs: dict = {}
        fetcher = fetch or fetch_leaderboard_text
        if _accepts_headers_out(fetcher):
            text = fetcher(s["url"], headers_out=hdrs)
        else:                                    # a stand-in fetcher without the kwarg (tests, older callers)
            text = fetcher(s["url"])
        rows, total = parse_leaderboard(
            text, min_account_value=min(LEADERBOARD_MIN_ACCOUNT_VALUE, s["smart_min_account_value"]))
        del text
        if not rows:
            # an empty array while the stats host rebuilds, or a reshaped payload that
            # yields nothing: never let it overwrite a good cache as a "fresh" empty set
            raise ValueError(f"leaderboard yielded no rows above the floor ({total} rows total)")
        sets = select_tracked(rows, whale_top_n=s["whale_top_n"], smart_top_n=s["smart_top_n"],
                              smart_min_account_value=s["smart_min_account_value"],
                              smart_min_month_roi=s["smart_min_month_roi"],
                              smart_min_month_vlm=s["smart_min_month_vlm"])
        fetched_at = dt.datetime.fromtimestamp(now, tz=dt.timezone.utc).isoformat()
        last_modified = http_date_to_iso(hdrs.get("last_modified"))
        wanted = set(sets["whales"]) | set(sets["smart"]) | {str(w).lower() for w in (keep_rows_for or ())}
        rows_by_address = {r.address: r for r in rows if r.address in wanted}
        save_cached(ns_dir, {
            "fetched_at": fetched_at, "fetched_at_ts": now, "source_url": s["url"],
            "rows_total": total, "rows_kept": len(rows), "last_modified": last_modified,
            "whales": sets["whales"], "smart": sets["smart"],
            "cursor": cached.get("cursor", 0),
        })
        log.info("tracked set refreshed: %d whales, %d smart (%d of %d leaderboard rows above the floor; "
                 "Last-Modified %s)", len(sets["whales"]), len(sets["smart"]), len(rows), total,
                 last_modified or "n/a")
        return sets, {"source": "fresh", "fetched_at": fetched_at, "rows_total": total,
                      "rows_kept": len(rows), "last_modified": last_modified,
                      "rows_by_address": rows_by_address}
    except Exception as e:  # noqa: BLE001 - the run must go on with what it has (daily.Terminated
        err = f"{type(e).__name__}: {str(e)[:200]}"   # is a BaseException and passes through)
        log.warning("leaderboard refresh failed: %s", err)
        if isinstance(cached.get("whales"), list) and isinstance(cached.get("smart"), list):
            age_h = (now - float(cached.get("fetched_at_ts") or 0)) / 3600.0
            if 0 <= age_h <= s["max_age_h"]:
                log.warning("using the cached tracked set from %s (%.1f h old)", cached.get("fetched_at"), age_h)
                return ({"whales": list(cached["whales"]), "smart": list(cached["smart"])},
                        {"source": "cache", "fetched_at": cached.get("fetched_at"),
                         "age_h": round(age_h, 1), "error": err})
            return {"whales": [], "smart": []}, {"source": "none", "fetched_at": cached.get("fetched_at"),
                                                  "age_h": round(age_h, 1), "error": err}
        return {"whales": [], "smart": []}, {"source": "none", "error": err}


# --- run order -----------------------------------------------------------------------

def _dedupe(items) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for w in items:
        w = str(w).strip().lower()
        if w and w not in seen:
            seen.add(w)
            out.append(w)
    return out


def auto_order(sets: dict[str, list[str]], pins: list[str]) -> list[str]:
    """smart first (fewer, cheaper, the product's 'smart money'), then whales; pins removed."""
    pinned = set(pins)
    return [w for w in _dedupe(list(sets.get("smart") or []) + list(sets.get("whales") or []))
            if w not in pinned]


def rotate(items: list[str], cursor: int) -> list[str]:
    if not items:
        return []
    c = cursor % len(items)
    return items[c:] + items[:c]


def plan_wallets(pins: list[str], sets: dict[str, list[str]], cursor: int = 0
                 ) -> tuple[list[str], list[str]]:
    """(run order, auto list in canonical order). Pins always lead; the auto part is
    rotated so a deferred tail is first next time."""
    pins_u = _dedupe(pins)
    auto = auto_order(sets, pins_u)
    return pins_u + rotate(auto, cursor), auto


def advance_cursor(auto: list[str], cursor: int, processed: set[str]) -> int:
    """Index (in canonical auto order) of the first wallet this run did not reach.
    extract.run processes the rotated list in order and defers a suffix, so the
    processed auto wallets are a prefix of the rotated list."""
    if not auto:
        return 0
    done = 0
    for w in rotate(auto, cursor):
        if w in processed:
            done += 1
        else:
            break
    return (cursor + done) % len(auto)
