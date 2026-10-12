"""
Minimal Hyperliquid Info API client (POST https://api.hyperliquid.xyz/info).

Read-only, public endpoint. Handles:
  - retries with backoff on 429 / 5xx / network errors
  - a PROACTIVE per-IP weight budget (sliding 60s window) so concurrent callers
    stay under the HL limit instead of burning retries on 429s
  - thread-safety (per-thread requests.Session) so independent calls can run in
    a thread pool — the real latency lever on a network-bound workload
  - time-window pagination for history endpoints

Verified limits:
  - userFillsByTime: <=2000 rows/response, only the 10000 most-recent available.
    Full history must come from S3 node_fills (hl-mainnet-node-data).
  - userFunding / userNonFundingLedgerUpdates: <=500 rows/response.
  - candleSnapshot: returns only the ~5000 MOST RECENT candles of the requested
    range; paging backward returns nothing. So 1m history ends ~3.5 days back —
    pick the interval by range length (interval_for_range).
  - IP budget 1200 weight/min. Weight model ~ 20 + items/20 per response.
"""
from __future__ import annotations

import logging
import os
import threading
import time
from collections import deque
from typing import Any

import requests

log = logging.getLogger(__name__)

DEFAULT_URL = "https://api.hyperliquid.xyz/info"
_MAX_RETRIES = 5
_PAGE_SLEEP_S = 0.15          # gentle spacing between paginated calls
_PER_RESP_FILLS = 2000
_PER_RESP_LEDGER = 500
_CANDLE_CAP = 5000            # API returns at most this many, most recent first

# Default budget below the documented 1200/min to leave headroom for other
# clients on the same IP (e.g. the TS platform-backend poller). HL_WEIGHT_BUDGET /
# HL_INFO_URL / HL_HTTP_TIMEOUT are read when a client is CONSTRUCTED (after the
# .env file has been loaded), never at import time.
DEFAULT_WEIGHT_BUDGET = 900
DEFAULT_TIMEOUT_S = 30


def interval_for_range(days: float) -> str:
    """Finest candle interval that still covers `days` within the 5000-candle cap."""
    minutes = days * 1440
    if minutes <= _CANDLE_CAP:
        return "1m"
    if minutes / 5 <= _CANDLE_CAP:
        return "5m"
    if minutes / 15 <= _CANDLE_CAP:
        return "15m"
    if minutes / 60 <= _CANDLE_CAP:
        return "1h"
    if minutes / 240 <= _CANDLE_CAP:
        return "4h"
    return "1d"


class _WeightBudget:
    """Sliding-window weight limiter shared across threads."""

    def __init__(self, budget_per_min: int):
        self.budget = budget_per_min
        self._events: deque[tuple[float, int]] = deque()
        self._lock = threading.Lock()

    def _prune(self, now: float) -> None:
        while self._events and now - self._events[0][0] > 60.0:
            self._events.popleft()

    def acquire(self, est_weight: int) -> list:
        """Reserve `est_weight` in the window; returns the slot handle to settle."""
        while True:
            with self._lock:
                now = time.monotonic()
                self._prune(now)
                used = sum(w for _, w in self._events)
                if used + est_weight <= self.budget or not self._events:
                    slot = [now, est_weight]
                    self._events.append(slot)
                    return slot
                wait = 60.0 - (now - self._events[0][0]) + 0.05
            log.debug("weight budget: %d/%d used, sleeping %.1fs", used, self.budget, wait)
            time.sleep(max(wait, 0.05))

    def settle(self, slot: list, actual_weight: int) -> None:
        """Replace THIS caller's estimate with the measured weight. Settling by
        handle (not by popping the last entry) keeps accounting correct when
        several threads interleave acquire/settle."""
        with self._lock:
            slot[1] = actual_weight


class InfoClient:
    def __init__(self, url: str | None = None, timeout: int | None = None,
                 weight_budget: int | None = None):
        self.url = url or os.environ.get("HL_INFO_URL") or DEFAULT_URL
        self.timeout = int(timeout if timeout is not None
                           else (os.environ.get("HL_HTTP_TIMEOUT") or DEFAULT_TIMEOUT_S))
        budget = (weight_budget if weight_budget is not None
                  else int(os.environ.get("HL_WEIGHT_BUDGET") or DEFAULT_WEIGHT_BUDGET))
        self._local = threading.local()
        self._budget = _WeightBudget(budget)

    def _session(self) -> requests.Session:
        s = getattr(self._local, "session", None)
        if s is None:
            s = requests.Session()
            s.headers.update({"Content-Type": "application/json"})
            self._local.session = s
        return s

    # Hyperliquid's documented per-request weights (rate-limits page):
    #   clearinghouseState / spotClearinghouseState ............ 2
    #   paginated history (userFillsByTime, userFunding,
    #     userNonFundingLedgerUpdates, candleSnapshot) .......... 20 + items/20
    #   everything else (meta, portfolio, openOrders, ...) ...... 20 flat
    # Booking 20 + items/20 for every call overstated light wallets ~2.3x and
    # halved throughput for nothing.
    _WEIGHT_STATE = {"clearinghouseState", "spotClearinghouseState"}
    _WEIGHT_PAGED = {"userFillsByTime", "userFills", "userFunding",
                     "userNonFundingLedgerUpdates", "candleSnapshot",
                     "userTwapSliceFillsByTime", "userTwapSliceFills"}

    @classmethod
    def _weight_of(cls, body: dict[str, Any], resp: Any) -> int:
        t = body.get("type")
        if t in cls._WEIGHT_STATE:
            return 2
        if t in cls._WEIGHT_PAGED:
            n = len(resp) if isinstance(resp, list) else 0
            return 20 + n // 20
        return 20

    def _post(self, body: dict[str, Any], est_items: int = 0) -> Any:
        t = body.get("type")
        if t in self._WEIGHT_STATE:
            est = 2
        elif t in self._WEIGHT_PAGED:
            est = 20 + est_items // 20
        else:
            est = 20
        backoff = 1.0
        last_err: Exception | None = None
        for attempt in range(1, _MAX_RETRIES + 1):
            # every HTTP attempt costs weight, so reserve per attempt (a 429'd
            # request still counted against the budget server-side)
            slot = self._budget.acquire(est)
            try:
                r = self._session().post(self.url, json=body, timeout=self.timeout)
                if r.status_code == 429 or r.status_code >= 500:
                    raise requests.HTTPError(f"HTTP {r.status_code}")
                r.raise_for_status()
                data = r.json()
                self._budget.settle(slot, self._weight_of(body, data))
                return data
            except Exception as e:  # noqa: BLE001 — retry everything transient
                last_err = e
                if attempt == _MAX_RETRIES:
                    break
                log.warning("info %s attempt %d failed: %s; retrying in %.1fs",
                            body.get("type"), attempt, e, backoff)
                time.sleep(backoff)
                backoff = min(backoff * 2, 16.0)
        raise RuntimeError(f"Info API {body.get('type')} failed after "
                           f"{_MAX_RETRIES} tries: {last_err}")

    # --- point-in-time snapshots ----------------------------------------------

    def clearinghouse_state(self, user: str, dex: str | None = None) -> dict:
        """Perp positions + margin summary (current). Without `dex` this is the
        MAIN perp dex only; HIP-3 builder dexes (perp_dexs()) each have their own
        margin account and must be queried with dex=<name>."""
        body = {"type": "clearinghouseState", "user": user}
        if dex:
            body["dex"] = dex
        return self._post(body)

    def meta(self, dex: str | None = None) -> dict:
        """Perp universe metadata. For a HIP-3 dex includes `collateralToken`
        (spot token index: 0 = USDC; flx/vntl/km = USDH, hyna = USDE, cash = USDT0
        as of 2026-10) — the token perp PnL/funding/accountValue are denominated in."""
        body = {"type": "meta"}
        if dex:
            body["dex"] = dex
        return self._post(body, est_items=300)

    def perp_dexs(self) -> list[str]:
        """Names of HIP-3 perp dexes (excluding the main dex, reported as null)."""
        out = self._post({"type": "perpDexs"})
        names = []
        for d in out or []:
            name = d.get("name") if isinstance(d, dict) else d
            if name:
                names.append(str(name))
        return names

    def spot_clearinghouse_state(self, user: str) -> dict:
        """Spot balances (current)."""
        return self._post({"type": "spotClearinghouseState", "user": user})

    def spot_meta(self) -> dict:
        """Spot universe + tokens (for coin -> token/decimals resolution)."""
        return self._post({"type": "spotMeta"}, est_items=800)

    def spot_meta_and_asset_ctxs(self) -> tuple[dict, list[dict]]:
        """spotMeta PLUS a live markPx/midPx for every spot pair — ONE call that
        replaces per-token candle fetches when all you need is a current price.
        Returns (spot_meta, ctxs). NOTE: ctxs are NOT index-aligned with
        spot_meta['universe'] (verified live) — match on each ctx's `coin`."""
        meta, ctxs = self._post({"type": "spotMetaAndAssetCtxs"}, est_items=1500)
        return meta, ctxs

    def meta_and_asset_ctxs(self) -> tuple[dict, list[dict]]:
        """Perp universe PLUS live markPx/funding/OI per coin — one call."""
        meta, ctxs = self._post({"type": "metaAndAssetCtxs"}, est_items=500)
        return meta, ctxs

    def delegator_summary(self, user: str) -> dict:
        """HYPE staking summary: delegated / undelegated / pending withdrawal.
        Staked HYPE is real net worth NOT reflected in spotClearinghouseState."""
        return self._post({"type": "delegatorSummary", "user": user})

    def open_orders(self, user: str) -> list[dict]:
        """Resting orders (perp + spot). Spot BUY orders lock USDC in
        spotClearinghouseState.USDC.hold; subtracting them isolates the part of
        `hold` that is perp margin reserved from spot (unified-account signal)."""
        return self._post({"type": "openOrders", "user": user}, est_items=200)

    def user_vault_equities(self, user: str) -> list[dict]:
        """Equity the user holds in vaults: [{vaultAddress, equity, lockedUntil...}].
        Vault deposits leave the perp/spot books, so this is net worth that NO
        other endpoint reports."""
        return self._post({"type": "userVaultEquities", "user": user})

    def all_mids(self) -> dict:
        return self._post({"type": "allMids"}, est_items=600)

    def portfolio(self, user: str) -> list:
        """Account value + PnL history, per period — the SAME series the HL UI
        shows. Used as the validation ORACLE. Shape: [[period, {accountValueHistory,
        pnlHistory}], ...] with periods day/week/month/allTime/perp*."""
        return self._post({"type": "portfolio", "user": user}, est_items=200)

    # --- history (time-window paginated) --------------------------------------

    def user_fills_by_time(self, user: str, start_ms: int, end_ms: int) -> list[dict]:
        """Fills in [start, end]. Pages by advancing startTime to last time
        (inclusive) and dedups on tid so boundary-ms fills are never skipped."""
        out: list[dict] = []
        seen: set[tuple] = set()
        cursor = start_ms
        while True:
            batch = self._post({
                "type": "userFillsByTime",
                "user": user,
                "startTime": cursor,
                "endTime": end_ms,
                "aggregateByTime": False,
            }, est_items=_PER_RESP_FILLS)
            if not isinstance(batch, list) or not batch:
                break
            new = 0
            for f in batch:
                key = (f.get("tid"), f.get("hash"), f.get("oid"), f.get("time"))
                if key in seen:
                    continue
                seen.add(key)
                out.append(f)
                new += 1
            last_t = batch[-1]["time"]
            if len(batch) == _PER_RESP_FILLS and batch[0]["time"] == last_t:
                log.warning("userFillsByTime: a full page shares one millisecond "
                            "(%d); rows beyond the page in that ms cannot be fetched",
                            last_t)
            if len(batch) < _PER_RESP_FILLS or new == 0 or last_t >= end_ms:
                break
            cursor = last_t
            time.sleep(_PAGE_SLEEP_S)
        return out

    def user_fills_saturated(self, fills: list[dict], start_ms: int,
                             gap_tolerance_ms: int = 3_600_000) -> bool:
        """True if the API cap truncated the requested window.

        The cap is NOT exactly 10k: live runs returned 10,773–21,596 fills for
        capped wallets. So two conditions are required: a cap-sized response
        (>= 10,000 fills) AND the oldest returned fill lying more than
        `gap_tolerance_ms` after the window start. A cap-sized response whose
        oldest fill is within minutes of the window start covered the window
        (3 of 8 flagged wallets in a 39-wallet sample were such false positives);
        a wallet that simply started trading mid-window is below the count and
        is never flagged."""
        if len(fills) < 10000:
            return False
        oldest = min((int(f.get("time", 0) or 0) for f in fills), default=start_ms)
        return oldest - start_ms > gap_tolerance_ms

    def user_twap_slice_fills_by_time(self, user: str, start_ms: int, end_ms: int) -> list[dict]:
        """TWAP slice fills in [start, end], NORMALISED to the plain fill shape.

        userFillsByTime does NOT include TWAP executions (verified live: 46 slice
        fills in one hour present in node data and at the counterparty, absent from
        userFillsByTime); they are served by userTwapSliceFillsByTime as
        {"fill": {...}, "twapId": N} with twapId null inside the fill. We flatten
        each item to the fill dict with twapId set, so the mapper sees one fill
        shape. Dedup against userFillsByTime is by tid (disjoint in practice).
        """
        out: list[dict] = []
        seen: set = set()
        cursor = start_ms
        while True:
            batch = self._post({
                "type": "userTwapSliceFillsByTime", "user": user,
                "startTime": cursor, "endTime": end_ms,
            }, est_items=_PER_RESP_FILLS)
            if not isinstance(batch, list) or not batch:
                break
            new = 0
            for item in batch:
                f = dict(item.get("fill") or item)
                if item.get("twapId") is not None:
                    f["twapId"] = item.get("twapId")
                key = (f.get("tid"), f.get("time"), f.get("oid"))
                if key in seen:
                    continue
                seen.add(key)
                out.append(f)
                new += 1
            last_t = int((batch[-1].get("fill") or batch[-1]).get("time", 0) or 0)
            if len(batch) < _PER_RESP_FILLS or new == 0 or last_t >= end_ms:
                break
            cursor = last_t
            time.sleep(_PAGE_SLEEP_S)
        return out

    def user_funding(self, user: str, start_ms: int, end_ms: int) -> list[dict]:
        return self._paged_ledger("userFunding", user, start_ms, end_ms)

    def user_non_funding_ledger_updates(self, user: str, start_ms: int,
                                        end_ms: int) -> list[dict]:
        return self._paged_ledger("userNonFundingLedgerUpdates", user,
                                  start_ms, end_ms)

    def _paged_ledger(self, typ: str, user: str, start_ms: int,
                      end_ms: int) -> list[dict]:
        """Shared pager for {time,hash,delta} envelope endpoints (<=500/resp).
        Dedup key includes the discriminating delta fields (coin/token/usdc/
        amount/destination) — per-coin funding shares (time, hash, type)."""
        out: list[dict] = []
        seen: set[tuple] = set()
        cursor = start_ms
        while True:
            batch = self._post({
                "type": typ, "user": user,
                "startTime": cursor, "endTime": end_ms,
            }, est_items=_PER_RESP_LEDGER)
            if not isinstance(batch, list) or not batch:
                break
            new = 0
            for e in batch:
                d = e.get("delta") or {}
                key = (e.get("time"), e.get("hash"), d.get("type"),
                       d.get("coin"), d.get("token"), d.get("usdc"),
                       d.get("amount"), d.get("destination"),
                       d.get("toPerp"), d.get("isDeposit"), d.get("operation"),
                       d.get("nonce"))
                if key in seen:
                    continue
                seen.add(key)
                out.append(e)
                new += 1
            last_t = batch[-1]["time"]
            if len(batch) < _PER_RESP_LEDGER or new == 0 or last_t >= end_ms:
                break
            cursor = last_t
            time.sleep(_PAGE_SLEEP_S)
        return out

    def candle_snapshot(self, coin: str, interval: str,
                        start_ms: int, end_ms: int) -> list[dict]:
        """OHLCV candles for a coin. The API returns only the ~5000 most recent
        candles in range and backward paging yields nothing, so this is a single
        request; callers choose `interval` via interval_for_range() and should
        treat a first-candle time later than start_ms as a coverage hole."""
        batch = self._post({
            "type": "candleSnapshot",
            "req": {"coin": coin, "interval": interval,
                    "startTime": start_ms, "endTime": end_ms},
        }, est_items=_CANDLE_CAP)
        if not isinstance(batch, list):
            return []
        if batch and batch[0]["t"] > start_ms + 60_000:
            log.info("candles %s/%s: coverage starts %d ms after requested start "
                     "(API retention cap)", coin, interval, batch[0]["t"] - start_ms)
        return batch
