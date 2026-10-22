"""
Extractor: pull HyperCore data for one or more wallets via the Info API into
CSVs whose columns match the BlueBlocx Iceberg schema (nessie.hypercore.*).

Usage:
    python -m blueblocx_hyperliquid.extract --wallet 0xA --days 7
    python -m blueblocx_hyperliquid.extract --wallet 0xA,0xB --days 7            # multi-wallet
    python -m blueblocx_hyperliquid.extract --wallet 0xA --days 7 --with-prices  # also build L3

Design notes (see docs/HYPERCORE_LEDGER_SPEC.md):
  - The per-wallet path is NETWORK-bound. Independent calls run in a thread pool
    under a proactive weight budget (info_client); pagination stays sequential.
  - Price materialisation (L3 candles) is a per-TOKEN shared asset and lives in
    price_job.py. Here, current spot prices come from ONE spotMetaAndAssetCtxs
    call, which is all net-worth valuation needs. --with-prices runs the price
    job inline for convenience.
  - Every output is UPSERTED by its natural key, so repeated runs and multi-wallet
    runs accumulate (l4x snapshots become a daily history) and never duplicate.
  - Natural keys are wallet-scoped; this module ASSERTS chain-wide uniqueness of
    (tx_hash, event_index, direction) across all wallets in the run — the key
    downstream Rust dedups on — and fails the reconciliation if violated.

Output (nessie.hypercore.*):
    out/hypercore/l1_wallet_ledger.csv            upsert (tx_hash, event_index, direction)
    out/hypercore/l1_token_metadata.csv           upsert token_address
    out/hypercore/l4x_perp_position_snapshot.csv  upsert (as_of_date, wallet, coin)
    out/hypercore/l4_wallet_metadata.csv          upsert wallet_address
    out/hypercore/wallet_watchlist.csv            upsert wallet_address  (consumer gate)
    out/hypercore/token_whitelist.csv             upsert token_address   (consumer gate)
    out/hypercore/l3_token_price_usd_1m.csv       native closed 1m candles, last HL_PRICE_1M_DAYS (price_step,
                                                  v1.10.0; the token/market tables read them)
    out/hypercore/l3_token_price_usd_1h.csv       native closed 1h candles (price_step, FIRST in the
    out/hypercore/l3_token_price_usd_1d.csv       run under HL_PRICE_TIME_BUDGET_MIN; state _price_state.json)
    out/hypercore/wallet_analytics_contrib.json   per-wallet contribution; the Rust
                                                  wallet-analytics crate OWNS the table
    out/hypercore/_reconciliation.json            asserts + portfolio-oracle checks
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

from . import schema as S
from . import analytics, config, mappers, ops, price_step, prices, wallet_meta
from .info_client import InfoClient
from .ops import atomic_write_text
from .spot_meta import SpotMetaResolver, empty as empty_resolver

log = logging.getLogger("blueblocx_hyperliquid.extract")

OUT_ROOT = Path(__file__).resolve().parent.parent / "out"     # repo default; HL_OUT_DIR overrides (config.out_root)

# Reconciliation tolerance for the USDC cashflow identity (float rounding).
_RECON_EPSILON = 1.0  # USD
_STABLE_SYMBOLS = {"USDC", "USDT0", "USDT", "USDHL", "FEUSD"}
# ETL contract 4.4(c): a row must be readable within 2 h of its updated_at. Rows are
# stamped at run START and committed at the end; warn when the gap grows past this.
_STAMP_AGE_WARN_MIN = 60.0
# The lazy full-history ledger pull (lending resolution, phase 2) is the one unbounded
# call of a run: 541 s on an old whale when it was written, 17 min on 2026-10-21, which
# carried the run past the 50-min unit timeout and lost the hour. It is started only
# while at least this many seconds remain before the soft deadline; otherwise it is
# deferred (lending_deferred, retried next run) exactly as if the deadline had passed.
_LAZY_PULL_RESERVE_S = 12 * 60.0


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _s(v) -> str:
    """Key/compare normalisation: a typed value and its CSV read-back are equal
    (None and '' both become '', everything else str()); csv writes str(v) too."""
    return "" if v is None else str(v)


def _earlier(a, b):
    """The earlier of two ISO instants (strings or None); an unparseable pair falls back
    to the lexicographic minimum, a missing side yields the other."""
    sa, sb = _s(a), _s(b)
    if not sa:
        return b
    if not sb:
        return a
    try:
        da, db = datetime.fromisoformat(sa), datetime.fromisoformat(sb)
        da = da if da.tzinfo else da.replace(tzinfo=timezone.utc)
        db = db if db.tzinfo else db.replace(tzinfo=timezone.utc)
        return a if da <= db else b
    except ValueError:
        return a if sa <= sb else b


def content_merge(columns: list[str], *, stamp_col: str = "updated_at",
                  preserve=("created_at",), min_cols=()):
    """merge_fn for upsert_csv (dimension tables): a row's write stamp advances only
    when its CONTENT changed - the ETL's Tier-A cursor re-reads exactly the changed
    rows and a dimension re-emitted every hour with the same values is not re-pulled.
    Compared: every column except `stamp_col`, `preserve` (taken from the existing row
    when it has a value: created_at = first ingestion) and `min_cols` (the earlier
    instant wins: first_seen never moves later). Returns the EXISTING row untouched
    when nothing changed, else the new row with preserve/min applied."""
    preserve = tuple(c for c in preserve if c in columns)
    min_cols = tuple(c for c in min_cols if c in columns)
    compare = [c for c in columns if c != stamp_col and c not in preserve and c not in min_cols]

    def merge(old: dict, new: dict) -> dict:
        out = dict(new)
        for c in preserve:
            if _s(old.get(c)):
                out[c] = old[c]
        for c in min_cols:
            out[c] = _earlier(old.get(c), new.get(c))
        if all(_s(old.get(c)) == _s(out.get(c)) for c in compare + list(min_cols)):
            return old
        return out
    return merge


def upsert_csv(path: Path, columns: list[str], rows: list[dict], *, key_fn,
               sort_fn=None, replace_scope_fn=None, merge_fn=None) -> int:
    """Merge `rows` into the CSV at `path` by natural key (new rows win).

    Existing rows are read back as strings; keys are compared as str tuples so
    typed new rows and string old rows match. If `replace_scope_fn` is given,
    existing rows for which it returns True are DROPPED before merging — this
    gives replace-partition semantics (e.g. l4x: a later run on the same day for
    the same wallet must not leave stale rows for positions that closed). If
    `merge_fn(old, new)` is given it decides the row for a key present on both
    sides (content_merge: keep the old row - and its stamp - when nothing changed).
    Returns the total row count.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    merged: dict[tuple, dict] = {}
    if path.exists():
        with path.open("r", newline="", encoding="utf-8") as fh:
            for r in csv.DictReader(fh):
                if replace_scope_fn is not None and replace_scope_fn(r):
                    continue
                merged[key_fn(r)] = r
    unchanged = 0
    for r in rows:
        k = key_fn(r)
        old = merged.get(k)
        if merge_fn is not None and old is not None:
            kept = merge_fn(old, r)
            unchanged += kept is old
            merged[k] = kept
        else:
            merged[k] = r
    out = list(merged.values())
    if sort_fn is not None:
        out.sort(key=sort_fn)
    # atomic: a crash mid-write must not leave a truncated state file behind
    tmp = path.with_name(path.name + ".tmp")
    with tmp.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=columns, extrasaction="ignore")
        w.writeheader()
        for r in out:
            w.writerow({c: ("" if r.get(c) in (None, "") else r[c]) for c in columns})
    os.replace(tmp, path)
    log.info("upserted %-34s +%6d -> %6d rows%s", path.name, len(rows), len(out),
             f" ({unchanged} unchanged, stamps kept)" if unchanged else "")
    return len(out)


def prune_csv(path: Path, ts_col: str, cutoff_ms: int) -> dict:
    """Staged-CSV retention: drop rows whose `ts_col` is older than `cutoff_ms`.

    Only called after a SUCCESSFUL Iceberg lift (the lake is the durable store;
    the CSV is a staging + analytics-window buffer), so the file stays bounded at
    retention_days of history instead of growing forever — an MM wallet stages
    ~180k fills/day. Unparseable timestamps are kept, never dropped blindly.
    Atomic rewrite; returns counts."""
    if not path.exists():
        return {"kept": 0, "dropped": 0}
    kept: list[dict] = []
    dropped = 0
    with path.open("r", newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        columns = reader.fieldnames or []
        for r in reader:
            ts = r.get(ts_col)
            try:
                old = bool(ts) and _iso_to_ms(ts) < cutoff_ms
            except ValueError:
                old = False
            if old:
                dropped += 1
            else:
                kept.append(r)
    if dropped:
        tmp = path.with_name(path.name + ".tmp")
        with tmp.open("w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=columns)
            w.writeheader()
            w.writerows(kept)
        os.replace(tmp, path)
        log.info("retention %-33s -%6d rows older than %s -> %6d rows", path.name, dropped,
                 datetime.fromtimestamp(cutoff_ms / 1000, tz=timezone.utc).date(), len(kept))
    return {"kept": len(kept), "dropped": dropped}


def compute_start_ms(default_start_ms: int, watermark_through_ms: int | None,
                     overlap_ms: int = 3_600_000) -> int:
    """Incremental window start.

    First run for a wallet (no watermark): the --days default window. Afterwards
    ALWAYS resume from the watermark minus an overlap (re-pulled rows dedup on the
    natural key) — never clamped to --days, otherwise a missed day would leave a
    silent hole between the watermark and now-days while coverage stayed
    'contiguous'. A long gap simply produces a long window (10k-capped fills then
    fall back to --s3-backfill)."""
    if watermark_through_ms is None:
        return default_start_ms
    return int(watermark_through_ms) - overlap_ms


def _load_watermarks(path: Path) -> dict:
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001
            return {}
    return {}


def _load_ledger_rows_by_wallet(path: Path, wallets: set[str]) -> dict[str, list[dict]]:
    """ONE pass over the staged ledger: rows of every wallet in `wallets`, typed
    enough for analytics. The per-wallet variant below used to re-read the whole
    file once per wallet (O(wallets x file): hours per run at a few hundred
    wallets with 45 days of staging)."""
    out: dict[str, list[dict]] = {w: [] for w in wallets}
    if not path.exists() or not wallets:
        return out
    with path.open("r", newline="", encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            rows = out.get(r.get("wallet_address"))
            if rows is None:
                continue
            r["amount_signed"] = float(r["amount_signed"] or 0)
            r["event_index"] = int(r["event_index"] or 0)
            rows.append(r)
    return out


def _load_wallet_ledger_rows(path: Path, wallet: str) -> list[dict]:
    """Read a wallet's rows already on disk (typed enough for analytics)."""
    return _load_ledger_rows_by_wallet(path, {wallet})[wallet]


def _load_token_meta(path: Path) -> dict[str, dict]:
    """token_address -> {symbol, is_stablecoin} from l1_token_metadata.csv."""
    if not path.exists():
        return {}
    with path.open("r", newline="", encoding="utf-8") as fh:
        return {r["token_address"]: {"symbol": r.get("symbol"),
                                     "is_stablecoin": str(r.get("is_stablecoin")).lower() == "true"}
                for r in csv.DictReader(fh)}


def plan_s3_window(gaps: list[tuple[int, int]]) -> tuple[int, int] | None:
    """One S3 window covering every truncated wallet's gap [w_start, oldest_api):
    hour files are shared across wallets, so they are fetched ONCE per run and
    each wallet keeps only the fills inside its own gap."""
    gaps = [(s, e) for s, e in gaps if e > s]
    if not gaps:
        return None
    return min(s for s, _ in gaps), max(e for _, e in gaps)


def _load_prior_unrealized_all(path: Path, wallets: set[str], now_ms: int,
                               min_age_h: float = 20.0, max_age_h: float = 28.0) -> dict[str, tuple]:
    """ONE pass over the l4x snapshot CSV: for every wallet, Σ unrealized_pnl_usd of
    its latest snapshot taken 20-28 h ago -> {wallet: (value, age_hours)}; wallets
    without such a snapshot map to (None, None)."""
    out: dict[str, tuple] = {w: (None, None) for w in wallets}
    if not path.exists() or not wallets:
        return out
    rows_by_wallet: dict[str, list[tuple]] = {}
    with path.open("r", newline="", encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            w = r.get("wallet_address")
            if w not in out:
                continue
            ts = _iso_to_ms(r["as_of_ts"])
            age_h = (now_ms - ts) / 3_600_000
            if min_age_h <= age_h <= max_age_h:
                rows_by_wallet.setdefault(w, []).append((ts, float(r.get("unrealized_pnl_usd") or 0)))
    for w, rows in rows_by_wallet.items():
        best_ts = max(ts for ts, _ in rows)            # the latest snapshot in the band
        out[w] = (sum(v for ts, v in rows if ts == best_ts), (now_ms - best_ts) / 3_600_000)
    return out


def _load_prior_unrealized(path: Path, wallet: str, now_ms: int,
                           min_age_h: float = 20.0, max_age_h: float = 28.0):
    """Σ unrealized_pnl_usd from the wallet's l4x snapshot taken ~24h ago (the
    previous daily run). Returns (value, age_hours) or (None, None)."""
    return _load_prior_unrealized_all(path, {wallet}, now_ms, min_age_h, max_age_h)[wallet]


def merge_rows_by_key(existing: list[dict], new: list[dict]) -> list[dict]:
    """Union by the natural key; new rows win (same semantics as upsert_csv)."""
    merged = {mappers.natural_key(r): r for r in existing}
    for r in new:
        merged[mappers.natural_key(r)] = r
    return list(merged.values())


def _gather(tasks: dict, max_workers: int) -> dict:
    """Run the pulls concurrently. If a BaseException escapes result() (SIGTERM ->
    daily.Terminated) cancel the queued pulls and do NOT wait for the in-flight ones:
    `with ThreadPoolExecutor` would call shutdown(wait=True) and block until every
    queued call had finished - past systemd's stop timeout on a slow API, which
    would SIGKILL the runner before it writes its summary. The interpreter joins
    the leftover workers at exit, after the summary and the lock release."""
    pool = ThreadPoolExecutor(max_workers=max_workers)
    try:
        futs = {k: pool.submit(fn) for k, fn in tasks.items()}
        out = {k: f.result() for k, f in futs.items()}
    except BaseException:
        pool.shutdown(wait=False, cancel_futures=True)
        raise
    pool.shutdown(wait=True)
    return out


def _fetch_wallet(client: InfoClient, wallet: str, start_ms: int, now_ms: int,
                  max_workers: int, dexes: list[str] | None = None,
                  need_ledger_all: bool = True) -> dict:
    """Run the independent Info API pulls for one wallet concurrently (7 core +
    one clearinghouseState per HIP-3 dex)."""
    def safe(fn, default):
        try:
            return fn()
        except Exception as e:  # noqa: BLE001
            log.warning("%s failed for %s: %s", getattr(fn, "__name__", fn), wallet, e)
            return default

    errors: dict[str, str] = {}

    def guarded(name, fn, default):
        def _run():
            try:
                return fn()
            except Exception as e:  # noqa: BLE001
                errors[name] = f"{type(e).__name__}: {e}"
                log.warning("%s failed for %s: %s", name, wallet, e)
                return default
        return _run

    # Every pull is guarded so one wallet's failure never aborts the batch; the
    # caller skips a wallet whose CORE pulls (fills/funding/ledger/chs) failed.
    tasks = {
        "fills": guarded("fills", lambda: client.user_fills_by_time(wallet, start_ms, now_ms), None),
        # TWAP executions are NOT in userFillsByTime (verified live) — separate pull
        "twap_fills": guarded("twap_fills", lambda: client.user_twap_slice_fills_by_time(wallet, start_ms, now_ms), None),
        "funding": guarded("funding", lambda: client.user_funding(wallet, start_ms, now_ms), None),
        "ledger": guarded("ledger", lambda: client.user_non_funding_ledger_updates(wallet, start_ms, now_ms), None),
        "chs": guarded("chs", lambda: client.clearinghouse_state(wallet), None),
        "spot_state": guarded("spot_state", lambda: client.spot_clearinghouse_state(wallet), {}),
        "staking": guarded("staking", lambda: client.delegator_summary(wallet), {}),
        "portfolio": guarded("portfolio", lambda: client.portfolio(wallet), []),
        "vaults": guarded("vaults", lambda: client.user_vault_equities(wallet), []),
        "open_orders": guarded("open_orders", lambda: client.open_orders(wallet), []),
    }
    if need_ledger_all:
        # Full ledger history — the only source for the outstanding borrow/lend
        # supply (portfolio counts it; spotClearinghouseState omits it for non-PM
        # accounts). NOT cheap for old whales (one took 541 s = ~170 pages), so in
        # incremental mode it is pulled once and the position is cached in the
        # watermark; later runs apply only the window's new borrowLend deltas.
        tasks["ledger_all"] = guarded(
            "ledger_all", lambda: client.user_non_funding_ledger_updates(wallet, 0, now_ms), [])
    out = _gather(tasks, max_workers)
    # HIP-3 dexes: query a dex's clearinghouse ONLY if the wallet touched it in
    # this window (fills or funding coin prefixed "dex:"). Funding accrues hourly
    # on every open position, so any open HIP-3 position surfaces in funding for
    # windows >= 1h — this skips ~10 calls per wallet for the ~75% that never
    # trade HIP-3 without missing open positions.
    touched = set()
    for f in (out.get("fills") or []):
        c = f.get("coin", "")
        if ":" in c:
            touched.add(c.split(":", 1)[0])
    for e in (out.get("funding") or []):
        c = ((e.get("delta") or {}).get("coin") or "")
        if ":" in c:
            touched.add(c.split(":", 1)[0])
    dex_tasks = {f"chs:{dx}": guarded(f"chs:{dx}", (lambda d: lambda: client.clearinghouse_state(wallet, dex=d))(dx), {})
                 for dx in (dexes or []) if dx in touched}
    if dex_tasks:
        out.update(_gather(dex_tasks, max_workers))
    out["_dexes_queried"] = sorted(touched & set(dexes or []))
    out["_errors"] = errors
    return out


_CORE_PULLS = ("fills", "twap_fills", "funding", "ledger", "chs")


def _is_core_pull(name: str) -> bool:
    """Pulls whose failure makes the wallet's run invalid: history, main
    clearinghouse AND every HIP-3 clearinghouse (a missing dex would otherwise
    replace the day's l4x scope with an incomplete snapshot)."""
    return name in _CORE_PULLS or name.startswith("chs:")


def _iso_to_ms(iso: str) -> int:
    return int(datetime.fromisoformat(iso).timestamp() * 1000)


def _reconcile(l1_rows, snap_rows, chs, fills_saturated, fills, funding) -> dict:
    """Reconcile MAPPED rows against the RAW API inputs (mapping fidelity).

    This is a real check, not a tautology: it compares what the mapper emitted
    with what the API returned, so dropped rows, sign flips, dedup losses or
    double-emits show up as a residual. (The old "USDC partition identity" only
    re-summed the same rows and could never fail; it is kept as
    `usdc_partition_residual` for classification completeness only.)

      Σ fills.closedPnl        == Σ perp_realized_pnl rows          (USDC)
      Σ fills.fee (USDC-denominated) == -Σ fee rows in USDC
      Σ funding.delta.usdc     == Σ funding rows
      #fills with closedPnl!=0 == #perp_realized_pnl rows
      #funding entries (usdc!=0) == #funding rows
    """
    def rows_sum(pred):
        return sum(r["amount_signed"] for r in l1_rows if pred(r))

    def rows_count(pred):
        return sum(1 for r in l1_rows if pred(r))

    is_usdc = lambda r: r["token_address"] == S.USDC_ADDRESS  # noqa: E731
    mapped_pnl = rows_sum(lambda r: r["event_type"] == S.EVT_PERP_PNL)
    mapped_fee_usdc = rows_sum(lambda r: r["event_type"] == S.EVT_PERP_FEE and is_usdc(r))
    mapped_funding = rows_sum(lambda r: r["event_type"] == S.EVT_FUNDING)
    mapped_pnl_n = rows_count(lambda r: r["event_type"] == S.EVT_PERP_PNL)
    mapped_funding_n = rows_count(lambda r: r["event_type"] == S.EVT_FUNDING)

    # closedPnl is compared for PERP fills only: HL also reports a small closedPnl
    # on spot sells (vs its own cost basis) which we deliberately do NOT book —
    # spot realized PnL is the FIFO layer's job on the base/quote legs. Those are
    # surfaced separately below so nothing is silently ignored.
    perp_fills = [f for f in fills if not mappers._is_spot(f["coin"])]
    spot_pnl_fills = [f for f in fills if mappers._is_spot(f["coin"])
                      and float(f.get("closedPnl", 0) or 0) != 0.0]
    raw_pnl = sum(float(f.get("closedPnl", 0) or 0) for f in perp_fills)
    raw_fee_usdc = sum(float(f.get("fee", 0) or 0) + float(f.get("builderFee", 0) or 0)
                       for f in fills if f.get("feeToken", S.USDC_TOKEN) == S.USDC_TOKEN)
    raw_funding = sum(float((e.get("delta") or {}).get("usdc", 0) or 0) for e in funding)
    raw_pnl_n = sum(1 for f in perp_fills if float(f.get("closedPnl", 0) or 0) != 0.0)
    raw_funding_n = sum(1 for e in funding
                        if float((e.get("delta") or {}).get("usdc", 0) or 0) != 0.0)

    checks = {
        "closed_pnl": (raw_pnl, mapped_pnl),
        "fees_usdc": (-raw_fee_usdc, mapped_fee_usdc),
        "funding": (raw_funding, mapped_funding),
    }
    residuals = {k: round(raw - mapped, 6) for k, (raw, mapped) in checks.items()}
    counts_ok = (raw_pnl_n == mapped_pnl_n) and (raw_funding_n == mapped_funding_n)
    fidelity_ok = all(abs(v) <= _RECON_EPSILON for v in residuals.values()) and counts_ok

    # classification completeness: every USDC row falls into a known bucket
    known = (S.REALIZED_PNL_EVENT_TYPES | S.FLOW_EVENT_TYPES | {S.EVT_SPOT_FILL})
    unclassified = rows_count(lambda r: r["event_type"] not in known)

    unrealized = sum(r["unrealized_pnl_usd"] for r in snap_rows)
    account_value = float((chs.get("marginSummary", {}) or {}).get("accountValue", 0) or 0)
    if not fidelity_ok:
        log.warning("RECON FAIL: raw-vs-mapped residuals %s counts_ok=%s", residuals, counts_ok)
    return {
        "l1_ledger_rows": len(l1_rows),
        "raw_fills": len(fills),
        "raw_funding_entries": len(funding),
        "realized_pnl_window": round(mapped_pnl, 6),
        "funding_window": round(mapped_funding, 6),
        "fees_window_usdc": round(mapped_fee_usdc, 6),
        "fidelity_residuals": residuals,
        "fidelity_counts": {"pnl_rows": [raw_pnl_n, mapped_pnl_n],
                            "funding_rows": [raw_funding_n, mapped_funding_n]},
        "fidelity_ok": fidelity_ok,
        "spot_fills_with_closed_pnl": len(spot_pnl_fills),
        "spot_closed_pnl_raw_not_booked": round(
            sum(float(f.get("closedPnl", 0) or 0) for f in spot_pnl_fills), 6),
        "unclassified_rows": unclassified,
        "open_positions": len(snap_rows),
        "unrealized_pnl_now": round(unrealized, 6),
        "account_value_now": round(account_value, 6),
        "fills_truncated_at_10k_cap": fills_saturated,
    }


def spot_balance_rows(balances: list[dict], wallet: str, now_ms: int, now_iso: str, resolver,
                      registry=None) -> list[dict]:
    """l4x_spot_balance_snapshot rows from spotClearinghouseState.balances (v1.9.0): one
    per non-zero holding, keyed by the same token_address the ledger uses (resolver;
    unresolved symbols fall back to the ledger's hlcore:spot:<SYMBOL> convention).
    `registry` (v1.10.4): the held tokens are noted so l1_token_metadata covers every
    token the balance rollups reference, not only the tokens the ledger traded (128 held
    tokens had no metadata row on 2026-10-22)."""
    as_of_ts = datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc).isoformat()
    out: list[dict] = []
    for b in balances or []:
        coin = b.get("coin")
        try:
            total = float(b.get("total", 0) or 0)
        except (TypeError, ValueError):
            continue
        if not coin or total == 0.0:
            continue
        info = resolver.resolve_symbol(coin) if resolver is not None else None
        token_address = info.token_address if info is not None else (
            S.USDC_ADDRESS if coin == "USDC" else f"hlcore:spot:{coin}")
        if registry is not None:
            if info is not None:
                registry.note_token(info)
            else:
                registry.note(token_address, coin, 0, coin.upper() in _STABLE_SYMBOLS)

        def _f(key):
            try:
                v = b.get(key)
                return float(v) if v is not None else None
            except (TypeError, ValueError):
                return None
        out.append({"as_of_ts": as_of_ts, "as_of_date": as_of_ts[:10], "wallet_address": wallet,
                    "wallet_group": S.addr_group(wallet), "token_address": token_address, "coin": coin,
                    "total": total, "hold": _f("hold"), "entry_ntl": _f("entryNtl"), "created_at": now_iso})
    return out


def run(wallets: list[str], days: int, out_dir: Path, *, with_prices: bool = False,
        max_workers: int = 4, max_perp_price_coins: int = 30,
        s3_backfill: bool = False, iceberg: bool = False, incremental: bool = False,
        hip3: bool = True, retention_days: int = 0, deadline_ts: float | None = None,
        soft_wallets: set[str] | None = None, progress=None,
        price_budget_min: float | None = None, rollup_budget_min: float | None = None) -> dict:
    """Pull, map, snapshot, analyse and write every wallet in `wallets` (in order).

    deadline_ts (epoch seconds): after it, no further wallet is STARTED and the
    phase-2 lazy full-ledger pull is skipped (left uncached, so it is retried); the
    rest of the list comes back as recon["deferred"] with nothing of theirs touched
    (no watermark, no snapshot scope), and the run completes normally for the
    wallets it did pull. The scheduled runner sets it well inside the service
    timeout so a list that outgrows one slot degrades to "the tail waits an hour"
    instead of the whole run being killed with nothing saved.

    soft_wallets: the auto-tracked (leaderboard) wallets. They are never backfilled
    from S3 in this run, and an API-capped window is accepted as a hole: the
    watermark advances and coverage restarts at the oldest fill returned, so the
    wallet is light again next run instead of re-pulling the capped window every
    hour forever. Pinned wallets keep the repair semantics (watermark held until an
    S3 backfill completes the window).

    progress: optional callable taking keyword fields (phase, wallets_done, current,
    ...) - ops.Progress.update - called at every phase change and before every wallet.

    price_budget_min: minutes the native candle step (price_step, FIRST in the run) may
    spend; None = HL_PRICE_TIME_BUDGET_MIN, 0 = off. Its two tables are lifted right
    after the step when `iceberg` is set, so they land even if the wallet phase fails.
    """
    wallets = list(dict.fromkeys(w.lower() for w in wallets))   # dedupe: one pull and one dimension row per wallet
    soft = {w.lower() for w in (soft_wallets or ())}
    progress = progress or (lambda **kw: None)
    client = InfoClient()        # HL_INFO_URL / HL_WEIGHT_BUDGET / HL_HTTP_TIMEOUT from the env
    now_iso = _now_iso()
    now_ms = int(time.time() * 1000)
    default_start_ms = now_ms - days * 86400 * 1000
    start_ms = default_start_ms          # run-level window (prices, reporting)
    ns_dir = out_dir / S.NS_HYPERCORE
    wm_path = ns_dir / "_watermarks.json"
    watermarks = _load_watermarks(wm_path) if incremental else {}
    log.info("Extracting HyperCore for %d wallet(s) over %d days%s%s%s", len(wallets), days,
             " [incremental]" if incremental else "", " [s3-backfill]" if s3_backfill else "",
             " [iceberg]" if iceberg else "")

    # HIP-3 builder dexes: each has its own margin account; positions there are
    # invisible to clearinghouseState(main). Fetch the list once per run.
    dexes: list[str] = []
    collateral_by_prefix: dict = {}
    collateral_report: dict[str, str] = {}
    if hip3:
        try:
            dexes = client.perp_dexs()
        except Exception as e:  # noqa: BLE001
            log.warning("perpDexs failed — HIP-3 positions will not be snapshotted: %s", e)
    price_start_ms = now_ms          # narrowed to the earliest processed window

    # --- shared: spotMeta + live spot prices in ONE call ------------------------
    try:
        spot_meta, spot_ctxs = client.spot_meta_and_asset_ctxs()
        resolver = SpotMetaResolver(spot_meta)
    except Exception as e:  # noqa: BLE001
        log.warning("spotMetaAndAssetCtxs failed — decimals=0 fallback, no spot prices: %s", e)
        spot_meta, spot_ctxs, resolver = {}, [], empty_resolver()

    # Each HIP-3 dex denominates PnL/funding/accountValue in ITS collateral token
    # (xyz=USDC, flx/vntl/km=USDH, hyna=USDE, cash=USDT0 as of 2026-10). Needs the
    # spotMeta resolver, so it runs after it.
    for dx in dexes:
        try:
            idx = (client.meta(dx) or {}).get("collateralToken")
            ti = resolver.token(int(idx)) if idx is not None else None
        except Exception as e:  # noqa: BLE001
            log.warning("meta(dex=%s) failed — assuming USDC collateral: %s", dx, e)
            ti = None
        if ti is not None:
            collateral_by_prefix[f"{dx}:"] = ti
            collateral_report[dx] = ti.symbol
        else:
            collateral_report[dx] = "USDC(assumed)"

    # ---- phase 0: native 1h/1d candles for the ETL's price tables (price_step) ----------
    # FIRST, under its own absolute budget, so the wallet phase keeps its slot; the two
    # tables are lifted at once (their watermarks come with the main lift) so they land
    # even when the wallet phase defers or fails. Never fatal for the run.
    native_price_report = None
    budget_min = config.env_float("HL_PRICE_TIME_BUDGET_MIN") if price_budget_min is None else price_budget_min
    if budget_min > 0:
        progress(phase="prices")
        try:
            native_price_report = price_step.run(client, resolver, ns_dir, now_ms, now_iso, budget_s=budget_min * 60,
                                                 history_days=config.env_int("HL_PRICE_HISTORY_DAYS"), clock=time,
                                                 history_days_1d=min(config.env_int("HL_PRICE_HISTORY_DAYS_1D"), 4000),
                                                 minute_days=config.env_float("HL_PRICE_1M_DAYS"),
                                                 supply_refresh_h=config.env_float("HL_SUPPLY_REFRESH_HOURS"))
        except Exception as e:  # noqa: BLE001 - prices never block the wallet phase
            log.warning("price step failed: %s: %s", type(e).__name__, e)
            native_price_report = {"error": f"{type(e).__name__}: {str(e)[:120]}"}
        if iceberg and not native_price_report.get("error") and (native_price_report.get("rows_1h")
                                                                 or native_price_report.get("rows_1d")
                                                                 or native_price_report.get("rows_1m")):
            from .iceberg_load import lift_directory, lift_since, read_lift_state
            config.load_env()
            prev_lift = read_lift_state(ns_dir).get("last_success_run_iso") if incremental else None
            price_tables = [price_step.TABLE_1H, price_step.TABLE_1D]
            if native_price_report.get("rows_1m"):
                price_tables.append(price_step.TABLE_1M)
            try:
                early = lift_directory(ns_dir, tables=price_tables,
                                       since_iso=lift_since(prev_lift, now_iso), watermarks=False)
                native_price_report["early_lift"] = {t: early.get(t) for t in price_tables}
            except Exception as e:  # noqa: BLE001 - the main lift retries these tables at the end of the run
                log.warning("early lift of the price tables failed (the main lift retries): %s: %s",
                            type(e).__name__, e)
                native_price_report["early_lift"] = {"error": f"{type(e).__name__}: {str(e)[:120]}"}

    registry = mappers.TokenRegistry()
    all_l1: list[dict] = []
    all_snap: list[dict] = []
    all_spot: list[dict] = []                 # v1.9.0: l4x_spot_balance_snapshot rows
    wallet_meta_rows: list[dict] = []
    watchlist_rows: list[dict] = []
    contrib: dict[str, dict] = {}
    recon_by_wallet: dict[str, dict] = {}
    unknown_types_all: dict[str, int] = {}
    held_symbols: set[str] = set()
    perp_coins_open: set[str] = set()

    # ---- phase 1: pull every wallet's API data -----------------------------------
    from .s3_backfill import merge_fills as _merge
    pending: list[dict] = []
    deferred: list[str] = []
    progress(phase="pull", wallets_total=len(wallets), wallets_done=0, current=None)
    for i, wallet in enumerate(wallets):
        if deadline_ts is not None and time.time() >= deadline_ts:
            deferred = wallets[i:]
            log.warning("soft deadline reached after %d of %d wallets - %d deferred to the next run",
                        i, len(wallets), len(deferred))
            break
        progress(current=wallet, wallets_done=i)
        t_wallet = time.monotonic()
        prior = watermarks.get(wallet) or {}
        w_start_ms = compute_start_ms(default_start_ms, prior.get("through_ms"))
        # The full ledger history (lending position) is pulled LAZILY in phase 2:
        # only if the net worth without it does not already match the oracle (then
        # lending is the only unexplained component). Cached afterwards.
        need_ledger_all = not (incremental and prior.get("lending_through_ms") is not None)
        d = _fetch_wallet(client, wallet, w_start_ms, now_ms, max_workers, dexes,
                          need_ledger_all=False)
        core_errors = {k: v for k, v in d["_errors"].items() if _is_core_pull(k)}
        if core_errors:
            log.error("[%s] SKIPPED — core pull(s) failed: %s", wallet[:10], core_errors)
            recon_by_wallet[wallet] = {"skipped": True, "errors": d["_errors"]}
            continue
        fills = d["fills"]
        # truncation is judged on the regular fills pull BEFORE merging TWAP slices
        fills_saturated = client.user_fills_saturated(fills, w_start_ms)
        oldest_api = min((int(f.get("time", 0) or 0) for f in fills), default=now_ms)
        twap_fills = d.get("twap_fills") or []
        if twap_fills:
            fills = _merge(fills, twap_fills)
            log.info("[%s] +%d TWAP slice fills merged", wallet[:10], len(twap_fills))
        pending.append(dict(wallet=wallet, prior=prior, w_start_ms=w_start_ms, d=d, fills=fills,
                            fills_saturated=fills_saturated, oldest_api=oldest_api,
                            twap_fills=twap_fills, need_ledger_all=need_ledger_all,
                            s3_report=None))
        # one line per wallet while pulling: the only sign of life in this phase
        log.info("[%d/%d] %s pulled in %.1fs: %d fills, %d funding, %d ledger%s",
                 i + 1, len(wallets), wallet[:10], time.monotonic() - t_wallet, len(fills),
                 len(d["funding"] or []), len(d["ledger"] or []), " [API cap]" if fills_saturated else "")
    processed = wallets[:len(wallets) - len(deferred)]
    progress(wallets_done=len(processed), wallets_deferred=len(deferred), current=None)

    # ---- phase 1b: ONE shared S3 pass for every truncated wallet -------------------
    # Hour files are per chain, not per wallet: N truncated wallets used to download
    # the same ~20 files N times. Now one window covers all gaps and each wallet
    # keeps the fills inside its own [w_start, oldest_api).
    # auto-tracked (soft) wallets are never backfilled from S3 in a scheduled run: a
    # capped 2-day market-maker window is ~46 hour files (~70 min of Requester-Pays
    # downloads), far past any deadline; their cap is accepted as a hole below
    trunc = [p for p in pending if p["fills_saturated"] and p["wallet"] not in soft]
    for p in pending:
        if p["fills_saturated"] and p["wallet"] in soft:
            log.warning("[%s] fills truncated at the API cap - auto-tracked wallet: hole accepted, "
                        "coverage restarts at the oldest fill returned", p["wallet"][:10])
    if trunc and s3_backfill:
        from .s3_backfill import fetch_fills_s3
        progress(phase="s3-backfill")
        config.load_env()
        window = plan_s3_window([(p["w_start_ms"], p["oldest_api"]) for p in trunc])
        if window:
            res = fetch_fills_s3([p["wallet"] for p in trunc], window[0], window[1],
                                 on_file=lambda done, total: progress(detail=f"s3 hour file {done}/{total}"))
            for p in trunc:
                mine = [f for f in res.fills.get(p["wallet"], [])
                        if p["w_start_ms"] <= int(f.get("time", 0) or 0) < p["oldest_api"]]
                p["fills"] = _merge(p["fills"], mine)
                p["fills_saturated"] = not res.complete
                p["s3_report"] = {"gap_start_ms": p["w_start_ms"], "gap_end_ms": p["oldest_api"],
                                  "s3_fills": len(mine), "merged_fills": len(p["fills"]),
                                  "shared_window": list(window), **res.summary()}
                log.info("[%s] s3 backfill: +%d fills for the truncated gap (complete=%s)",
                         p["wallet"][:10], len(mine), res.complete)
    elif trunc:
        for p in trunc:
            log.warning("[%s] fills truncated at the API cap — pass --s3-backfill to complete "
                        "the window (watermark NOT advanced)", p["wallet"][:10])

    # ---- phase 2: map, snapshot, analyse ---------------------------------------
    # The staged CSVs are read ONCE per run for all wallets (not once per wallet).
    progress(phase="analyse")
    run_wallets = {p["wallet"] for p in pending}
    disk_rows_by_wallet = (_load_ledger_rows_by_wallet(ns_dir / "l1_wallet_ledger.csv", run_wallets)
                           if incremental else {})
    prior_unreal_by_wallet = _load_prior_unrealized_all(ns_dir / "l4x_perp_position_snapshot.csv",
                                                        run_wallets, now_ms)
    for k, p in enumerate(pending):
        wallet, prior_wm, w_start_ms, d = p["wallet"], p["prior"], p["w_start_ms"], p["d"]
        progress(current=wallet, analysed=k, analysed_total=len(pending))
        fills, fills_saturated, twap_fills, s3_report = (p["fills"], p["fills_saturated"],
                                                        p["twap_fills"], p["s3_report"])
        funding, ledger_updates = d["funding"], d["ledger"]
        chs, spot_state, staking, portfolio = d["chs"], d["spot_state"], d["staking"], d["portfolio"]
        vaults = d["vaults"]
        chs_by_dex = {dx: d.get(f"chs:{dx}") or {} for dx in d.get("_dexes_queried", [])}
        price_start_ms = min(price_start_ms, w_start_ms)
        log.info("[%s] fetched: %d fills, %d funding, %d ledger, %d positions%s",
                 wallet[:10], len(fills), len(funding), len(ledger_updates),
                 len(chs.get("assetPositions", [])),
                 "  [FILLS TRUNCATED @10k cap]" if fills_saturated else "")

        # --- L1 --------------------------------------------------------------
        l1_rows = mappers.map_fills_to_ledger(fills, wallet, now_iso, resolver, registry,
                                              collateral_by_prefix=collateral_by_prefix)
        l1_rows += mappers.map_funding_to_ledger(funding, wallet, now_iso, registry,
                                                 collateral_by_prefix=collateral_by_prefix)
        ledger_rows, unknown = mappers.map_non_funding_ledger(
            ledger_updates, wallet, now_iso, resolver, registry)
        l1_rows += ledger_rows
        for k, v in unknown.items():
            unknown_types_all[k] = unknown_types_all.get(k, 0) + v

        # --- L4x snapshot (main dex + every HIP-3 dex) ---------------------------
        snap_rows = prices.clearinghouse_to_snapshot(chs, wallet, now_ms, now_iso, dex="main")
        account_value_by_dex: dict[str, float] = {}
        for dx, dchs in chs_by_dex.items():
            if not dchs:
                continue
            av = float((dchs.get("marginSummary", {}) or {}).get("accountValue", 0) or 0)
            dex_rows = prices.clearinghouse_to_snapshot(dchs, wallet, now_ms, now_iso, dex=dx)
            if av or dex_rows:
                account_value_by_dex[dx] = av
                snap_rows += dex_rows
        perp_coins_open.update(r["coin"] for r in snap_rows)

        # --- current spot prices for valuation (no candles) -------------------
        balances = spot_state.get("balances", []) if spot_state else []
        syms = {b.get("coin") for b in balances if float(b.get("total", 0) or 0) != 0.0}
        if float(staking.get("delegated", 0) or 0) or float(staking.get("undelegated", 0) or 0):
            syms.add("HYPE")
        held_symbols |= {s for s in syms if s and s.upper() not in _STABLE_SYMBOLS}
        # --- lending position: cached + window deltas, else resolved lazily below ---
        lending_resolved = not p["need_ledger_all"]
        if lending_resolved:
            since = int(prior_wm.get("lending_through_ms") or 0)
            new_deltas = [e for e in ledger_updates if int(e.get("time", 0) or 0) > since]
            lending_by_token = analytics.merge_lending(prior_wm.get("lending_by_token") or {},
                                                      analytics.lending_position(new_deltas))
        else:
            lending_by_token = {}          # provisional; see lazy pull after the oracle

        # Analytics run over the wallet's ACCUMULATED ledger (disk + this run), not
        # just this pull: in incremental mode the pull window is ~1h, but
        # realized_7d/30d must be computed over everything we hold. Coverage
        # starts at the earliest window ever pulled for the wallet (watermark).
        coverage_start_ms = min(w_start_ms, int(prior_wm.get("coverage_start_ms") or w_start_ms))
        if incremental:
            disk_rows = disk_rows_by_wallet.get(wallet, [])
            if disk_rows and not prior_wm.get("coverage_start_ms"):
                # pre-existing data without a coverage watermark: coverage begins at
                # the earliest row we actually hold, not at this (short) window
                coverage_start_ms = min(coverage_start_ms,
                                        min(_iso_to_ms(r["block_timestamp"]) for r in disk_rows))
            analysis_rows = merge_rows_by_key(disk_rows, l1_rows)
        else:
            analysis_rows = l1_rows
        # Auto-tracked wallet at the API cap with no repair path: the fills before the
        # oldest one returned are a hole we live with. Contiguous coverage therefore
        # starts at that fill - the 7d/30d windows stay incomplete until genuinely
        # covered - and the watermark advances below, so the next run pulls ~1h
        # instead of the same capped window again.
        accept_hole = fills_saturated and wallet in soft
        if accept_hole:
            coverage_start_ms = max(coverage_start_ms, int(p["oldest_api"]))
        # price every symbol the ACCUMULATED ledger touched (fee/quote legs), not
        # just holdings; historical tokens come from l1_token_metadata.csv
        token_meta_all = dict(_load_token_meta(ns_dir / "l1_token_metadata.csv"))
        token_meta_all.update(registry.items())
        ledger_syms = {m.get("symbol") for m in token_meta_all.values()}
        spot_px = prices.spot_symbol_prices(resolver, spot_meta, spot_ctxs, syms | ledger_syms)
        token_usd_price = {addr: (1.0 if m.get("is_stablecoin") else spot_px.get(m.get("symbol")))
                           for addr, m in token_meta_all.items()}
        token_usd_price = {a: p for a, p in token_usd_price.items() if p is not None}

        # --- analytics contribution + oracle ---------------------------------
        wa = analytics.compute_analytics(analysis_rows, snap_rows, chs, spot_state, now_ms,
                                         now_iso, spot_price_by_symbol=spot_px,
                                         staking=staking, token_usd_price=token_usd_price,
                                         vault_equities=vaults,
                                         window_start_ms=coverage_start_ms,
                                         fills_truncated=(fills_saturated
                                                          or bool(prior_wm.get("fills_truncated"))),
                                         account_value_by_dex=account_value_by_dex,
                                         lending_by_token=lending_by_token,
                                         open_orders=d.get("open_orders") or [])
        # PnL oracle: HL's own 24h pnlHistory (perpDay) vs our realized perp cashflow
        # + change in unrealized since the previous daily snapshot (~24h ago)
        prior_unreal, prior_age_h = prior_unreal_by_wallet.get(wallet, (None, None))
        oracle = analytics.validate_against_oracle(
            wa, portfolio, now_ms, prior_unrealized=prior_unreal, prior_age_h=prior_age_h)
        lending_lazy_pull = lending_deferred = lending_pulled_ok = False
        lending_mismatch = ((not lending_resolved) and bool(oracle.get("checked"))
                            and oracle.get("networth_matches") is False)
        if lending_mismatch and deadline_ts is not None and time.time() >= deadline_ts - _LAZY_PULL_RESERVE_S:
            # the full-history pull is the one unbounded call of phase 2 (541 s on an
            # old whale): past the deadline it waits for the next run, uncached
            lending_deferred = True
            log.warning("[%s] lazy ledger_all pull skipped (soft deadline) - lending unresolved, "
                        "retried next run", wallet[:10])
        elif lending_mismatch:
            # Net worth without lending does not match HL: the borrow/lend supply is
            # the one component only the full ledger history reveals — pull it now
            # (expensive for old whales, hence lazy) and recompute.
            lending_lazy_pull = True
            progress(current=wallet, detail="ledger_all pull (full history; up to ~10 min on an old whale)")
            log.info("[%s] lazy ledger_all pull started (full history; up to ~10 min on an old whale)", wallet[:10])
            try:
                ledger_all = client.user_non_funding_ledger_updates(wallet, 0, now_ms)
                lending_pulled_ok = True
            except Exception as e:  # noqa: BLE001
                log.warning("[%s] ledger_all failed: %s", wallet[:10], e)
                ledger_all = []
            lending_by_token = analytics.lending_position(ledger_all)
            wa = analytics.compute_analytics(analysis_rows, snap_rows, chs, spot_state, now_ms,
                                             now_iso, spot_price_by_symbol=spot_px,
                                             staking=staking, token_usd_price=token_usd_price,
                                             vault_equities=vaults,
                                             window_start_ms=coverage_start_ms,
                                             fills_truncated=(fills_saturated
                                                              or bool(prior_wm.get("fills_truncated"))),
                                             account_value_by_dex=account_value_by_dex,
                                             lending_by_token=lending_by_token,
                                             open_orders=d.get("open_orders") or [])
            oracle = analytics.validate_against_oracle(
                wa, portfolio, now_ms, prior_unrealized=prior_unreal, prior_age_h=prior_age_h)
            log.info("[%s] lazy ledger_all pull: lending=%s -> networth_matches=%s",
                     wallet[:10], lending_by_token, oracle.get("networth_matches"))
            progress(detail=None)
        # the lending position is cached only when it is actually known: from the
        # cache, from a successful full-history pull, or implied by an oracle match
        # (or an unchecked oracle) without it - never after a skipped or failed pull
        lending_known = lending_resolved or lending_pulled_ok or not lending_mismatch
        recon = _reconcile(l1_rows, snap_rows, chs, fills_saturated, fills, funding)
        recon["unmapped_ledger_types"] = unknown
        recon["fetch_errors"] = d["_errors"]
        recon["window_start_ms"] = w_start_ms
        recon["twap_slice_fills"] = len(twap_fills)
        recon["lending_lazy_pull"] = lending_lazy_pull
        recon["lending_deferred"] = lending_deferred
        recon["hole_accepted"] = accept_hole
        recon["coverage_start_ms"] = coverage_start_ms
        recon["analysis_rows"] = len(analysis_rows)
        recon["s3_backfill"] = s3_report
        recon["hip3_dexes_with_equity"] = sorted(account_value_by_dex)
        recon["hip3_dexes_queried"] = d.get("_dexes_queried", [])
        # Watermark rules:
        #  * a window that is still truncated (no/incomplete S3 backfill) does NOT
        #    advance through_ms, so a later --s3-backfill run re-pulls the same
        #    window from S3 and genuinely repairs the hole - except for an
        #    auto-tracked wallet, whose hole was accepted above (coverage_start_ms
        #    already moved past it) and whose watermark therefore advances;
        #  * fills_truncated is sticky until a run that is complete AND contiguous
        #    with the prior watermark (w_start_ms <= prior through_ms) clears it.
        prior_through = prior_wm.get("through_ms")
        repaired = (not fills_saturated) and (prior_through is None or w_start_ms <= int(prior_through))
        sticky = fills_saturated or (bool(prior_wm.get("fills_truncated")) and not repaired)
        advance = (not fills_saturated) or accept_hole
        # l4_wallet_metadata is a running aggregate carried next to the watermark and advanced
        # under the SAME rule (wallet_meta): disjoint windows, exact counts, a capped window
        # that will be re-pulled is never counted early
        meta_state = wallet_meta.advance_state(prior_wm.get("meta"), analysis_rows, prior_through, now_ms, advance)
        watermarks[wallet] = {
            "through_ms": (now_ms if advance else prior_through),
            "coverage_start_ms": coverage_start_ms,
            "fills_truncated": sticky,
            "lending_by_token": (lending_by_token if lending_known
                                 else (prior_wm.get("lending_by_token") or {})),
            "lending_through_ms": (now_ms if lending_known else prior_wm.get("lending_through_ms")),
            "meta": meta_state,
            "updated_at": now_iso,
        }
        if accept_hole:
            log.warning("[%s] window capped - hole accepted for an auto-tracked wallet, coverage now starts at %s",
                        wallet[:10], datetime.fromtimestamp(coverage_start_ms / 1000, tz=timezone.utc).isoformat())
        elif fills_saturated:
            log.warning("[%s] window incomplete — watermark kept at %s", wallet[:10], prior_through)
        recon["spot_price_source"] = "spotMetaAndAssetCtxs"
        recon["unpriced_held_symbols"] = sorted(s for s in syms
                                               if s and s.upper() not in _STABLE_SYMBOLS
                                               and s not in spot_px)
        recon["unvalued_ledger_rows"] = wa.get("unvalued_ledger_rows", 0)
        recon["oracle"] = oracle
        recon_by_wallet[wallet] = recon
        contrib[wallet] = wa

        all_l1 += l1_rows
        all_snap += snap_rows
        all_spot += spot_balance_rows(balances, wallet, now_ms, now_iso, resolver, registry=registry)
        meta_row = wallet_meta.row(wallet, meta_state, now_ms, now_iso)
        if meta_row is not None:                  # no non-funding activity covered yet: no row
            wallet_meta_rows.append(meta_row)
        watchlist_rows.append({
            "wallet_address": wallet, "wallet_group": S.addr_group(wallet),
            "is_active": True, "whitelist_reason": "hypercore_ingest",
            "created_at": now_iso, "updated_at": now_iso,
        })
        log.info("[%s] fidelity_ok=%s perp_oracle=%s networth_oracle=%s realized=%.2f unrealized=%.2f",
                 wallet[:10], recon["fidelity_ok"],
                 oracle.get("account_value_matches"), oracle.get("networth_matches"),
                 recon["realized_pnl_window"], recon["unrealized_pnl_now"])

    progress(current=None, detail=None)
    if unknown_types_all:
        log.warning("UNMAPPED ledger delta.type(s) — extend _LEDGER_EVENT_TYPE: %s",
                    unknown_types_all)

    def abort_run(reason: str, **evidence) -> None:
        """HARD FAIL before the write phase: persist the evidence (this run's wallets
        only; the merged history is not touched) and raise. Nothing is staged, so the
        lake and the ETL never see the bad batch."""
        log.error("%s - nothing written", reason)
        ns_dir.mkdir(parents=True, exist_ok=True)
        atomic_write_text(ns_dir / "_reconciliation.json", json.dumps({
            "run_at": now_iso, "wallets": processed, "deferred": deferred,
            "natural_key_ok": not collisions and dup_within == 0,
            "natural_key_cross_wallet_collisions": collisions[:200], "natural_key_duplicates_within_run": dup_within,
            # None = not reached (the natural-key check aborted first)
            "referential_ok": None if orphans is None else (not orphans and bool(dims_distinct)),
            "ledger_tokens_without_metadata": (orphans or [])[:200], "dimension_keys_distinct": dims_distinct,
            **evidence, "by_wallet": recon_by_wallet,
        }, indent=2))
        raise RuntimeError(f"{reason} - nothing written (see _reconciliation.json)")

    # --- chain-wide natural-key uniqueness (the Rust dedup key, NO wallet) -----
    # v1.10.6: dup_within now counts only same-wallet repeats of a key (a cross-wallet
    # repeat goes to `collisions` instead, as before); `dup_examples` pins down WHICH
    # rows and how the two differ, instead of a bare count - added after three
    # consecutive production aborts (2026-10-22/23) with no way to tell which wallet
    # or event was responsible, pre-dating this release (first seen on v1.10.4).
    _DUP_FIELDS = ("event_type", "direction", "amount", "amount_signed", "token_address",
                  "block_timestamp", "counterparty_address")
    seen: dict[tuple, dict] = {}
    collisions: list[dict] = []
    dup_within = 0
    dup_examples: list[dict] = []
    for r in all_l1:
        k = mappers.natural_key(r)
        prior = seen.get(k)
        if prior is None:
            seen[k] = r
            continue
        if prior["wallet_address"] != r["wallet_address"]:
            collisions.append({"key": list(k), "wallets": [prior["wallet_address"], r["wallet_address"]]})
            continue
        dup_within += 1
        if len(dup_examples) < 20:
            dup_examples.append({"key": list(k), "wallet_address": r["wallet_address"],
                                 "first": {f: prior.get(f) for f in _DUP_FIELDS},
                                 "second": {f: r.get(f) for f in _DUP_FIELDS}})
    orphans: list[str] | None = None            # set by the referential check below
    dims_distinct: bool | None = None
    if collisions or dup_within:
        # writing would let upsert_csv silently collapse the colliding rows
        if dup_examples:
            log.error("natural key duplicate sample: %s", json.dumps(dup_examples, default=str))
        abort_run(f"natural key violation: {len(collisions)} cross-wallet collisions, {dup_within} duplicates",
                 natural_key_duplicate_examples=dup_examples)

    all_l1.sort(key=lambda r: (r["block_timestamp"], r["tx_hash"], r["event_index"]))
    reg_items = registry.items()
    meta_rows = prices.token_metadata_rows(reg_items, now_iso)
    # v1.10.0: total supply / deploy time from the price step's tokenDetails refresh
    # (the only supply source HyperCore has; NULL for tokens it has not reached yet)
    supply = price_step.supply_overrides(ns_dir)
    supplied = price_step.apply_supply(meta_rows, supply)
    # a run rebuilds only the rows of the tokens its ledger referenced; the tokens the
    # refresh reached that stayed quiet this run get their supply from their staged row
    # (fresh stamp, so the ETL's Tier-A cursor sees it) - v1.10.1
    extra = price_step.staged_rows_with_supply(ns_dir / "l1_token_metadata.csv", supply,
                                               {r["token_address"] for r in meta_rows}, now_iso)
    supplied += price_step.apply_supply(extra, supply)
    meta_rows += extra
    if supplied:
        log.info("token metadata: supply known for %d of %d token(s)", supplied, len(meta_rows))
    whitelist_rows = [{
        "token_address": addr, "token_group": S.addr_group(addr),
        "is_active": True, "whitelist_reason": "hypercore_token",
        "created_at": now_iso, "updated_at": now_iso,
    } for addr in reg_items]

    # --- referential closure + dimension keys (ETL contract 4.2 / 4.3) -----------
    # Every token_address on a ledger row must have its l1_token_metadata row in the
    # SAME batch (the destination has an FK on it), and every dimension CSV must be
    # keyed uniquely. Both hold by construction (_ledger_row registers every token;
    # one row per registry key / wallet) - this is the regression guard, failing the
    # run like the natural-key check so a bad batch is never staged or lifted.
    ledger_tokens = {r["token_address"] for r in all_l1}
    orphans = sorted(ledger_tokens - {r["token_address"] for r in meta_rows})
    dims_distinct = all(len(rows) == len({_s(r[k]) for r in rows}) for rows, k in (
        (meta_rows, "token_address"), (whitelist_rows, "token_address"),
        (watchlist_rows, "wallet_address"), (wallet_meta_rows, "wallet_address")))
    if orphans or not dims_distinct:
        abort_run(f"referential violation: {len(orphans)} ledger token(s) without metadata, "
                  f"dimension keys distinct={dims_distinct}")

    # --- write (upsert by natural key) -----------------------------------------
    progress(phase="write")
    s = _s
    # rows carry the run-start stamp; the ETL re-reads a Tier-A row only within 2 h of it
    stamp_age_min = (time.time() * 1000 - _iso_to_ms(now_iso)) / 60_000
    if stamp_age_min > _STAMP_AGE_WARN_MIN:
        log.warning("rows carry updated_at %.0f min older than their commit - the ETL's 2 h Tier-A "
                    "visibility margin is at risk (long run: keep hand runs under 2 h or announce them)",
                    stamp_age_min)

    as_of_date = datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
    # only wallets that produced a snapshot this run: a deferred or skipped wallet keeps
    # its earlier rows for today instead of having them wiped by an empty replace
    l4x_scope = {(as_of_date, w) for w in run_wallets}
    upsert_csv(ns_dir / "l1_wallet_ledger.csv", S.L1_WALLET_LEDGER_COLUMNS, all_l1,
               key_fn=lambda r: (s(r["tx_hash"]), s(r["event_index"]), s(r["direction"])),
               sort_fn=lambda r: (s(r["block_timestamp"]), s(r["tx_hash"]), int(r["event_index"])))
    # dimension rows: the stamp advances only on a content change (ETL Tier-A), created_at
    # is the first ingestion, first_seen never moves later
    upsert_csv(ns_dir / "l1_token_metadata.csv", S.L1_TOKEN_METADATA_COLUMNS, meta_rows,
               key_fn=lambda r: (s(r["token_address"]),), sort_fn=lambda r: s(r["token_address"]),
               merge_fn=content_merge(S.L1_TOKEN_METADATA_COLUMNS, min_cols=("first_seen",)))
    # l4x: replace-partition semantics per (as_of_date, wallet) so a position that
    # closed between two same-day runs does not survive as a stale row.
    upsert_csv(ns_dir / "l4x_perp_position_snapshot.csv", S.L4X_PERP_POSITION_SNAPSHOT_COLUMNS,
               all_snap,
               key_fn=lambda r: (s(r["as_of_date"]), s(r["wallet_address"]), s(r["coin"])),
               sort_fn=lambda r: (s(r["as_of_date"]), s(r["wallet_address"]), s(r["coin"])),
               replace_scope_fn=lambda r: (s(r["as_of_date"]), s(r["wallet_address"])) in l4x_scope)
    upsert_csv(ns_dir / "l4x_spot_balance_snapshot.csv", S.L4X_SPOT_BALANCE_SNAPSHOT_COLUMNS,
               all_spot,
               key_fn=lambda r: (s(r["as_of_date"]), s(r["wallet_address"]), s(r["token_address"])),
               sort_fn=lambda r: (s(r["as_of_date"]), s(r["wallet_address"]), s(r["token_address"])),
               replace_scope_fn=lambda r: (s(r["as_of_date"]), s(r["wallet_address"])) in l4x_scope)
    upsert_csv(ns_dir / "l4_wallet_metadata.csv", S.L4_WALLET_METADATA_COLUMNS, wallet_meta_rows,
               key_fn=lambda r: (s(r["wallet_address"]),), sort_fn=lambda r: s(r["wallet_address"]),
               merge_fn=content_merge(S.L4_WALLET_METADATA_COLUMNS))
    upsert_csv(ns_dir / "wallet_watchlist.csv", S.WALLET_WATCHLIST_COLUMNS, watchlist_rows,
               key_fn=lambda r: (s(r["wallet_address"]),), sort_fn=lambda r: s(r["wallet_address"]),
               merge_fn=content_merge(S.WALLET_WATCHLIST_COLUMNS))
    upsert_csv(ns_dir / "token_whitelist.csv", S.TOKEN_WHITELIST_COLUMNS, whitelist_rows,
               key_fn=lambda r: (s(r["token_address"]),), sort_fn=lambda r: s(r["token_address"]),
               merge_fn=content_merge(S.TOKEN_WHITELIST_COLUMNS))
    # tokens the ledger referenced this run form the hot tier of the next run's price step
    price_step.record_seen(ns_dir, {r["token_address"] for r in all_l1}, now_ms)

    price_report = None
    if with_prices:
        from .price_job import build_price_rows
        perp_coins = sorted(perp_coins_open)
        if len(perp_coins) > max_perp_price_coins:
            log.warning("perp price coins capped %d -> %d (reference-only)",
                        len(perp_coins), max_perp_price_coins)
            perp_coins = perp_coins[:max_perp_price_coins]
        stable_addrs = {a for a, m in reg_items.items() if m.get("is_stablecoin")}
        price_rows, price_report = build_price_rows(
            client, resolver, spot_symbols=held_symbols, perp_coins=perp_coins,
            start_ms=(price_start_ms if price_start_ms < now_ms else default_start_ms),
            end_ms=now_ms, now_iso=now_iso, stable_addrs=stable_addrs)
        upsert_csv(ns_dir / "l3_token_price_usd_1m.csv", S.L3_TOKEN_PRICE_USD_1M_COLUMNS,
                   price_rows,
                   key_fn=lambda r: (s(r["token_address"]), s(r["bucket_minute"])),
                   sort_fn=lambda r: (s(r["token_address"]), s(r["bucket_minute"])))

    # --- optional: lift straight into Iceberg (nessie.hypercore.* or local) ----
    iceberg_report = None
    retention_report = None
    rollups_report = None
    if iceberg:
        from .iceberg_load import lift_directory, lift_since, read_lift_state, write_lift_state
        config.load_env()
        # Bounded lift: only rows written since the previous successful lift (minus a
        # 1h margin) are read from the staged CSVs and checked against the lake, so
        # a daily run costs O(new rows), not O(45 days of staging) over the network.
        # The cut-off never passes THIS run's own stamp (a long hand run that overlaps
        # a scheduled lift must still lift its rows).
        lift_state = read_lift_state(ns_dir) if incremental else {}
        prev_lift = lift_state.get("last_success_run_iso")
        progress(phase="lift")
        iceberg_report = lift_directory(ns_dir, since_iso=lift_since(prev_lift, now_iso))  # raises on failure
        # Staged-CSV retention: the lake now holds these rows durably, so the
        # staging files only need to keep the analytics windows (30d) + margin.
        # Pruning rewrites the files whole, so it runs at most once a day (hourly
        # cadence would otherwise rewrite gigabytes every hour for nothing).
        extra_state: dict = {}
        if retention_days and retention_days > 0:
            last_prune = lift_state.get("last_prune_iso")
            try:
                prune_due = (last_prune is None
                             or now_ms - _iso_to_ms(last_prune) >= 20 * 3_600_000)
            except ValueError:
                prune_due = True
            if prune_due:
                cutoff_ms = now_ms - retention_days * 86_400_000
                retention_report = {
                    "retention_days": retention_days,
                    "cutoff": datetime.fromtimestamp(cutoff_ms / 1000, tz=timezone.utc).isoformat(),
                    "l1_wallet_ledger": prune_csv(ns_dir / "l1_wallet_ledger.csv", "block_timestamp", cutoff_ms),
                    # the 1m feed stages ~50k rows/day (v1.10.0); the lake is insert-if-absent, so
                    # the CSV only has to span the lift margin: 7 days, never the full retention
                    "l3_token_price_usd_1m": prune_csv(ns_dir / "l3_token_price_usd_1m.csv", "bucket_minute",
                                                       max(cutoff_ms, now_ms - 7 * 86_400_000)),
                    "l3_token_price_usd_1h": prune_csv(ns_dir / "l3_token_price_usd_1h.csv", "bucket_minute", cutoff_ms),
                    "l3_token_price_usd_1d": prune_csv(ns_dir / "l3_token_price_usd_1d.csv", "bucket_minute", cutoff_ms),
                    "l4x_perp_position_snapshot": prune_csv(ns_dir / "l4x_perp_position_snapshot.csv", "as_of_ts", cutoff_ms),
                    "l4x_spot_balance_snapshot": prune_csv(ns_dir / "l4x_spot_balance_snapshot.csv", "as_of_ts", cutoff_ms),
                }
                extra_state["last_prune_iso"] = now_iso
            else:
                retention_report = {"retention_days": retention_days, "skipped": f"last prune {last_prune}"}
        write_lift_state(ns_dir, now_iso, iceberg_report, extra=extra_state)
        # --- rollups: the wallet-level L4 tables, from the lake just committed (v1.9.0) ---
        # Never fails the run: a rollup error or a skipped table is a run-summary warning.
        if config.env("HL_ROLLUPS").strip().lower() not in ("0", "false", "no", "off", ""):
            progress(phase="rollups")
            try:
                from . import rollups
                from .iceberg_load import dispose_catalog, load_catalog_from_env
                catalog, _ = load_catalog_from_env()
                try:
                    rollups_report = rollups.run(ns_dir, catalog, budget_min=rollup_budget_min)
                finally:
                    dispose_catalog(catalog)
            except Exception as e:  # noqa: BLE001 - reported, never fatal
                log.exception("rollups failed")
                rollups_report = {"error": f"{type(e).__name__}: {str(e)[:300]}"}
    elif retention_days:
        log.warning("retention_days=%d ignored: staged CSVs are the only store without --iceberg",
                    retention_days)

    # --- incremental watermarks (per wallet) -----------------------------------
    progress(phase="finalise")
    if incremental:
        atomic_write_text(wm_path, json.dumps(watermarks, indent=2))

    # Per-wallet contribution to all_chain.wallet_analytics. NOT the sink itself:
    # the Rust wallet-analytics crate owns that table; this is input/validation.
    contrib_path = ns_dir / "wallet_analytics_contrib.json"
    existing = {}
    if contrib_path.exists():
        try:
            existing = json.loads(contrib_path.read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001
            existing = {}
    existing.update(contrib)
    atomic_write_text(contrib_path, json.dumps(existing, indent=2))

    # Reconciliation is merged per wallet across runs (like the contrib file), so
    # earlier wallets' audit evidence is not overwritten by a later single-wallet run.
    recon_path = ns_dir / "_reconciliation.json"
    prev_by_wallet = {}
    if recon_path.exists():
        try:
            prev_by_wallet = json.loads(recon_path.read_text(encoding="utf-8")).get("by_wallet", {}) or {}
        except Exception:  # noqa: BLE001
            prev_by_wallet = {}
    prev_by_wallet.update(recon_by_wallet)
    recon = {
        "run_at": now_iso,
        "wallets": processed,
        "deferred": deferred,
        "natural_key_cross_wallet_collisions": collisions,
        "natural_key_duplicates_within_run": dup_within,
        "natural_key_ok": not collisions and dup_within == 0,
        "referential_ok": True,
        "ledger_tokens_without_metadata": [],
        "dimension_keys_distinct": True,
        "stamp_age_min": round(stamp_age_min, 2),
        "unmapped_ledger_types": unknown_types_all,
        "price_job": price_report,
        "prices": native_price_report,
        "iceberg": iceberg_report,
        "retention": retention_report,
        "rollups": rollups_report,
        "hip3_dexes_checked": dexes,
        "hip3_collateral_by_dex": collateral_report,
        "by_wallet": prev_by_wallet,
    }
    atomic_write_text(recon_path, json.dumps(recon, indent=2))
    done = [r for w, r in recon_by_wallet.items() if not r.get("skipped")]
    log.info("done: natural_key_ok=%s fidelity_ok=%s perp_oracle_ok=%s networth_oracle_ok=%s skipped=%d",
             recon["natural_key_ok"],
             all(r["fidelity_ok"] for r in done),
             all(r["oracle"].get("account_value_matches") for r in done),
             all(r["oracle"].get("networth_matches") for r in done),
             len(recon_by_wallet) - len(done))
    return recon


def main(argv=None) -> int:
    """Exit codes: 0 done, 1 the run raised (traceback), 3 another run holds the state
    dir's lock (the hourly tick, a lift or maintenance): nothing was pulled - retry when
    it is free. While a long hand run holds the lock, scheduled ticks exit 3 themselves."""
    config.load_env()
    config.setup_logging()
    ap = argparse.ArgumentParser(description="Hyperliquid HyperCore -> staged CSVs -> Iceberg")
    ap.add_argument("--wallet", required=True, help="one address or comma-separated list")
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--with-prices", action="store_true",
                    help="also build l3_token_price_usd_1m for held/open tokens (price_job)")
    ap.add_argument("--max-price-coins", type=int, default=30,
                    help="cap on perp reference-price coins when --with-prices")
    ap.add_argument("--workers", type=int, default=4, help="concurrent Info API calls")
    ap.add_argument("--s3-backfill", action="store_true",
                    help="when the API truncates at 10k fills, pull the rest from S3 node data")
    ap.add_argument("--iceberg", action="store_true",
                    help="lift outputs into Iceberg (NESSIE_URI or local catalog under out/iceberg)")
    ap.add_argument("--incremental", action="store_true",
                    help="resume each wallet from its watermark (out/hypercore/_watermarks.json)")
    ap.add_argument("--no-hip3", action="store_true",
                    help="skip per-dex clearinghouseState for HIP-3 builder dexes")
    ap.add_argument("--retention-days", type=int, default=config.env_int("HL_RETENTION_DAYS"),
                    help="with --iceberg: prune staged l1/l3 CSV rows older than this (0 = never)")
    ap.add_argument("--out", default=str(config.out_root()))
    args = ap.parse_args(argv)
    wallets = [w.strip() for w in args.wallet.split(",") if w.strip()]
    ns_dir = Path(args.out) / S.NS_HYPERCORE
    try:
        with ops.RunLock(ns_dir / ".daily.lock"):
            run(wallets, args.days, Path(args.out), with_prices=args.with_prices,
                max_workers=args.workers, max_perp_price_coins=args.max_price_coins,
                s3_backfill=args.s3_backfill, iceberg=args.iceberg, incremental=args.incremental,
                hip3=not args.no_hip3, retention_days=args.retention_days)
    except ops.LockHeld as e:
        log.error("%s - another run owns the state dir; retry when it has finished", e)
        return 3
    return 0


if __name__ == "__main__":
    sys.exit(main())
