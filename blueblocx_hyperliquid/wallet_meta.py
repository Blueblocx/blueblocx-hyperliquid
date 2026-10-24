"""
l4_wallet_metadata over a wallet's ACCUMULATED history (v1.6.0).

Before v1.6.0 the row described only the run window (count_tx = rows of the last ~1 h
pull). Now a per-wallet running aggregate lives in `_watermarks.json[wallet]["meta"]`
and is advanced under exactly the rule that advances the wallet's watermark
(`through_ms`): a run folds the rows with prev.through_ms < block_timestamp <= now_ms
only when the window is complete (not API-capped) or the cap was accepted as a hole.
Windows are therefore disjoint and contiguous, so counts are exact without storing
keys, re-pulled overlap rows are never counted twice, and a window that will be
re-pulled (watermark held) is not counted early. A wallet without state is seeded
from every row held in staging up to the covered instant (disk + this run = up to 45
days of staging, i.e. "since go-live" on the production host). Deleting a wallet's
`meta` key re-seeds it the same way on the next run.

Semantics (docs/ETL_HANDOFF_HYPERCORE.md section 7): perp cashflow rows (perp PnL,
fee, funding) and income rows (staking reward, interest) are excluded everywhere -
v1.10.9, the same set wallet_transactions drops via HL_WALLET_TX_EVENT_TYPES, so
count_tx matches the rows a consumer can see there; first_tx / latest_tx = the
first / latest token-movement instants (= first/latest_block_timestamp);
count_tx = token-movement ledger rows (spot_fill, transfer, deposit, withdraw, mint,
burn); active_days = distinct UTC days with such activity;
last_active = whole UTC days since the latest event (0 = active today);
active_on_avg = NULL (no agreed definition). A wallet with no non-funding activity
covered yet has no row.
"""
from __future__ import annotations

import datetime as dt

from . import schema as S

# v1.10.9: everything that is not a token movement (was: funding only). Keep equal to
# the complement of the wallet_transactions event filter on the ETL side.
META_EXCLUDED_EVENT_TYPES = frozenset(S.REALIZED_PNL_EVENT_TYPES)


def empty() -> dict:
    return {"first_ms": None, "latest_ms": None, "count_tx": 0, "days": [], "through_ms": None}


def _ms(value) -> int:
    if isinstance(value, (int, float)):
        return int(value)
    v = dt.datetime.fromisoformat(str(value))
    if v.tzinfo is None:
        v = v.replace(tzinfo=dt.timezone.utc)
    return int(v.timestamp() * 1000)


def _iso(ms: int) -> str:
    return dt.datetime.fromtimestamp(ms / 1000, tz=dt.timezone.utc).isoformat()


def _day(ms: int) -> str:
    return dt.datetime.fromtimestamp(ms / 1000, tz=dt.timezone.utc).strftime("%Y-%m-%d")


def update(state: dict, rows, since_ms: int | None, now_ms: int) -> dict:
    """A COPY of `state` with the rows satisfying since_ms < block_timestamp <= now_ms
    folded in (since_ms None = everything up to now_ms). Funding rows never count. A
    window already folded (state.through_ms >= now_ms) is a no-op, so a retried run
    cannot double count."""
    if state.get("through_ms") is not None and now_ms <= int(state["through_ms"]):
        return dict(state)
    first, latest = state.get("first_ms"), state.get("latest_ms")
    count = int(state.get("count_tx") or 0)
    days = set(state.get("days") or [])
    for r in rows:
        if r.get("event_type") in META_EXCLUDED_EVENT_TYPES:
            continue
        ts = _ms(r["block_timestamp"])
        if ts > now_ms or (since_ms is not None and ts <= int(since_ms)):
            continue
        count += 1
        first = ts if first is None else min(int(first), ts)
        latest = ts if latest is None else max(int(latest), ts)
        days.add(r.get("block_date") or _day(ts))
    return {"first_ms": first, "latest_ms": latest, "count_tx": count, "days": sorted(days), "through_ms": int(now_ms)}


def seed(rows, through_ms: int) -> dict:
    """The state of a wallet without one: every row held, up to the covered instant."""
    return update(empty(), rows, None, through_ms)


def advance_state(prev: dict | None, rows, prior_through_ms, now_ms: int, advance: bool) -> dict | None:
    """The wallet's state after this run, bound to the watermark rule. `advance` is the
    predicate that moves through_ms to now_ms ((not fills_saturated) or accept_hole):
    with it the window (prev.through_ms, now_ms] is folded; without it the state is kept
    as is (the window will be re-pulled). No state yet: seed from `rows` up to the
    covered instant - now_ms when advancing, else the prior watermark - or stay None
    when nothing is covered (a capped first window of a pinned wallet)."""
    if prev is None:
        bound = now_ms if advance else prior_through_ms
        return None if bound is None else seed(rows, int(bound))
    if not advance:
        return dict(prev)
    return update(prev, rows, prev.get("through_ms"), now_ms)


def row(wallet: str, state: dict | None, now_ms: int, now_iso: str) -> dict | None:
    """The l4_wallet_metadata row for a state; None when no non-funding activity is
    covered (nothing to describe)."""
    if not state or not state.get("count_tx") or state.get("first_ms") is None:
        return None
    first, latest = int(state["first_ms"]), int(state["latest_ms"])
    last_active = (dt.date.fromisoformat(_day(now_ms)) - dt.date.fromisoformat(_day(latest))).days
    return {
        "wallet_address": wallet.lower(),
        "first_block_timestamp": _iso(first),
        "latest_block_timestamp": _iso(latest),
        "count_tx": int(state["count_tx"]),
        "first_tx": _iso(first),
        "latest_tx": _iso(latest),
        "active_days": len(state.get("days") or []),
        "last_active": max(0, last_active),
        "active_on_avg": None,
        "updated_at": now_iso,
    }
