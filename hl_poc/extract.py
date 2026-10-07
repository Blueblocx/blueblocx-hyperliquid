"""
POC extractor: pull HyperCore data for one or more wallets via the Info API into
CSVs whose columns match the BlueBlocx Iceberg schema (nessie.hypercore.*).

Usage:
    python -m hl_poc.extract --wallet 0xA --days 7
    python -m hl_poc.extract --wallet 0xA,0xB --days 7            # multi-wallet
    python -m hl_poc.extract --wallet 0xA --days 7 --with-prices  # also build L3

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
    out/hypercore/l3_token_price_usd_1m.csv       (only with --with-prices)
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
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

from . import schema as S
from . import analytics, mappers, prices
from .info_client import InfoClient
from .spot_meta import SpotMetaResolver, empty as empty_resolver

log = logging.getLogger("hl_poc.extract")

OUT_ROOT = Path(__file__).resolve().parent.parent / "out"

# Reconciliation tolerance for the USDC cashflow identity (float rounding).
_RECON_EPSILON = 1.0  # USD
_STABLE_SYMBOLS = {"USDC", "USDT0", "USDT", "USDHL", "FEUSD"}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def upsert_csv(path: Path, columns: list[str], rows: list[dict], *, key_fn,
               sort_fn=None, replace_scope_fn=None) -> int:
    """Merge `rows` into the CSV at `path` by natural key (new rows win).

    Existing rows are read back as strings; keys are compared as str tuples so
    typed new rows and string old rows match. If `replace_scope_fn` is given,
    existing rows for which it returns True are DROPPED before merging — this
    gives replace-partition semantics (e.g. l4x: a later run on the same day for
    the same wallet must not leave stale rows for positions that closed).
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
    for r in rows:
        merged[key_fn(r)] = r
    out = list(merged.values())
    if sort_fn is not None:
        out.sort(key=sort_fn)
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=columns, extrasaction="ignore")
        w.writeheader()
        for r in out:
            w.writerow({c: ("" if r.get(c) in (None, "") else r[c]) for c in columns})
    log.info("upserted %-34s +%6d -> %6d rows", path.name, len(rows), len(out))
    return len(out)


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


def _load_wallet_ledger_rows(path: Path, wallet: str) -> list[dict]:
    """Read a wallet's rows already on disk (typed enough for analytics)."""
    if not path.exists():
        return []
    out = []
    with path.open("r", newline="", encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            if r.get("wallet_address") != wallet:
                continue
            r["amount_signed"] = float(r["amount_signed"] or 0)
            r["event_index"] = int(r["event_index"] or 0)
            out.append(r)
    return out


def _load_token_meta(path: Path) -> dict[str, dict]:
    """token_address -> {symbol, is_stablecoin} from l1_token_metadata.csv."""
    if not path.exists():
        return {}
    with path.open("r", newline="", encoding="utf-8") as fh:
        return {r["token_address"]: {"symbol": r.get("symbol"),
                                     "is_stablecoin": str(r.get("is_stablecoin")).lower() == "true"}
                for r in csv.DictReader(fh)}


def merge_rows_by_key(existing: list[dict], new: list[dict]) -> list[dict]:
    """Union by the natural key; new rows win (same semantics as upsert_csv)."""
    merged = {mappers.natural_key(r): r for r in existing}
    for r in new:
        merged[mappers.natural_key(r)] = r
    return list(merged.values())


def _fetch_wallet(client: InfoClient, wallet: str, start_ms: int, now_ms: int,
                  max_workers: int, dexes: list[str] | None = None) -> dict:
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
        "funding": guarded("funding", lambda: client.user_funding(wallet, start_ms, now_ms), None),
        "ledger": guarded("ledger", lambda: client.user_non_funding_ledger_updates(wallet, start_ms, now_ms), None),
        "chs": guarded("chs", lambda: client.clearinghouse_state(wallet), None),
        "spot_state": guarded("spot_state", lambda: client.spot_clearinghouse_state(wallet), {}),
        "staking": guarded("staking", lambda: client.delegator_summary(wallet), {}),
        "portfolio": guarded("portfolio", lambda: client.portfolio(wallet), []),
        "vaults": guarded("vaults", lambda: client.user_vault_equities(wallet), []),
        # full ledger history (cheap: few hundred entries even for whales) — the
        # only source for the outstanding borrow/lend supply, which portfolio
        # counts but spotClearinghouseState omits for non-PM accounts
        "ledger_all": guarded("ledger_all", lambda: client.user_non_funding_ledger_updates(wallet, 0, now_ms), []),
    }
    for dex in (dexes or []):
        tasks[f"chs:{dex}"] = guarded(
            f"chs:{dex}", (lambda dx: lambda: client.clearinghouse_state(wallet, dex=dx))(dex), {})
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futs = {k: pool.submit(fn) for k, fn in tasks.items()}
        out = {k: f.result() for k, f in futs.items()}
    out["_errors"] = errors
    return out


_CORE_PULLS = ("fills", "funding", "ledger", "chs")


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


def run(wallets: list[str], days: int, out_dir: Path, *, with_prices: bool = False,
        max_workers: int = 4, max_perp_price_coins: int = 30,
        s3_backfill: bool = False, iceberg: bool = False, incremental: bool = False,
        hip3: bool = True) -> dict:
    wallets = [w.lower() for w in wallets]
    client = InfoClient(url=os.environ.get("HL_INFO_URL", InfoClient().url))
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

    registry = mappers.TokenRegistry()
    all_l1: list[dict] = []
    all_snap: list[dict] = []
    wallet_meta_rows: list[dict] = []
    watchlist_rows: list[dict] = []
    contrib: dict[str, dict] = {}
    recon_by_wallet: dict[str, dict] = {}
    unknown_types_all: dict[str, int] = {}
    held_symbols: set[str] = set()
    perp_coins_open: set[str] = set()

    for wallet in wallets:
        w_start_ms = compute_start_ms(default_start_ms,
                                      (watermarks.get(wallet) or {}).get("through_ms"))
        d = _fetch_wallet(client, wallet, w_start_ms, now_ms, max_workers, dexes)
        core_errors = {k: v for k, v in d["_errors"].items() if _is_core_pull(k)}
        if core_errors:
            log.error("[%s] SKIPPED — core pull(s) failed: %s", wallet[:10], core_errors)
            recon_by_wallet[wallet] = {"skipped": True, "errors": d["_errors"]}
            continue
        fills, funding, ledger_updates = d["fills"], d["funding"], d["ledger"]
        chs, spot_state, staking, portfolio = d["chs"], d["spot_state"], d["staking"], d["portfolio"]
        vaults = d["vaults"]
        chs_by_dex = {dx: d.get(f"chs:{dx}") or {} for dx in dexes}
        fills_saturated = client.user_fills_saturated(fills, w_start_ms)
        s3_report = None
        if fills_saturated and s3_backfill:
            # The API served only its ~10k most recent fills: fetch the rest of the
            # window from S3 node data. Same key scheme -> overlap dedups itself.
            # The window counts as complete ONLY if every hour file was read.
            from .s3_backfill import fetch_fills_s3, merge_fills, load_dotenv
            load_dotenv()
            oldest_api = min(int(f.get("time", 0) or 0) for f in fills)
            res = fetch_fills_s3([wallet], w_start_ms, oldest_api)
            s3_fills = res.fills.get(wallet, [])
            fills = merge_fills(fills, s3_fills)
            fills_saturated = not res.complete
            s3_report = {"gap_start_ms": w_start_ms, "gap_end_ms": oldest_api,
                         "s3_fills": len(s3_fills), "merged_fills": len(fills), **res.summary()}
            log.info("[%s] s3 backfill: +%d fills for the truncated gap (complete=%s)",
                     wallet[:10], len(s3_fills), res.complete)
        elif fills_saturated:
            log.warning("[%s] fills truncated at the 10k API cap — pass --s3-backfill to "
                        "complete the window (watermark NOT advanced)", wallet[:10])
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
        # Analytics run over the wallet's ACCUMULATED ledger (disk + this run), not
        # just this pull: in incremental mode the pull window is ~1h, but
        # realized_7d/30d must be computed over everything we hold. Coverage
        # starts at the earliest window ever pulled for the wallet (watermark).
        prior_wm = watermarks.get(wallet) or {}
        coverage_start_ms = min(w_start_ms, int(prior_wm.get("coverage_start_ms") or w_start_ms))
        if incremental:
            disk_rows = _load_wallet_ledger_rows(ns_dir / "l1_wallet_ledger.csv", wallet)
            if disk_rows and not prior_wm.get("coverage_start_ms"):
                # pre-existing data without a coverage watermark: coverage begins at
                # the earliest row we actually hold, not at this (short) window
                coverage_start_ms = min(coverage_start_ms,
                                        min(_iso_to_ms(r["block_timestamp"]) for r in disk_rows))
            analysis_rows = merge_rows_by_key(disk_rows, l1_rows)
        else:
            analysis_rows = l1_rows
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
                                         lending_by_token=analytics.lending_position(d.get("ledger_all") or []))
        oracle = analytics.validate_against_oracle(wa, portfolio, now_ms)
        recon = _reconcile(l1_rows, snap_rows, chs, fills_saturated, fills, funding)
        recon["unmapped_ledger_types"] = unknown
        recon["fetch_errors"] = d["_errors"]
        recon["window_start_ms"] = w_start_ms
        recon["coverage_start_ms"] = coverage_start_ms
        recon["analysis_rows"] = len(analysis_rows)
        recon["s3_backfill"] = s3_report
        recon["hip3_dexes_with_equity"] = sorted(account_value_by_dex)
        # Watermark rules:
        #  * a window that is still truncated (no/incomplete S3 backfill) does NOT
        #    advance through_ms, so a later --s3-backfill run re-pulls the same
        #    window from S3 and genuinely repairs the hole;
        #  * fills_truncated is sticky until a run that is complete AND contiguous
        #    with the prior watermark (w_start_ms <= prior through_ms) clears it.
        prior_through = prior_wm.get("through_ms")
        repaired = (not fills_saturated) and (prior_through is None or w_start_ms <= int(prior_through))
        sticky = fills_saturated or (bool(prior_wm.get("fills_truncated")) and not repaired)
        watermarks[wallet] = {
            "through_ms": (prior_through if fills_saturated else now_ms),
            "coverage_start_ms": coverage_start_ms,
            "fills_truncated": sticky,
            "updated_at": now_iso,
        }
        if fills_saturated:
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
        wallet_meta_rows += prices.wallet_metadata_row(l1_rows, wallet, now_iso)
        watchlist_rows.append({
            "wallet_address": wallet, "wallet_group": S.addr_group(wallet),
            "is_active": True, "whitelist_reason": "hypercore_ingest",
            "created_at": now_iso, "updated_at": now_iso,
        })
        log.info("[%s] fidelity_ok=%s perp_oracle=%s networth_oracle=%s realized=%.2f unrealized=%.2f",
                 wallet[:10], recon["fidelity_ok"],
                 oracle.get("account_value_matches"), oracle.get("networth_matches"),
                 recon["realized_pnl_window"], recon["unrealized_pnl_now"])

    if unknown_types_all:
        log.warning("UNMAPPED ledger delta.type(s) — extend _LEDGER_EVENT_TYPE: %s",
                    unknown_types_all)

    # --- chain-wide natural-key uniqueness (the Rust dedup key, NO wallet) -----
    seen: dict[tuple, str] = {}
    collisions: list[dict] = []
    for r in all_l1:
        k = mappers.natural_key(r)
        if k in seen and seen[k] != r["wallet_address"]:
            collisions.append({"key": list(k), "wallets": [seen[k], r["wallet_address"]]})
        seen.setdefault(k, r["wallet_address"])
    dup_within = len(all_l1) - len(seen)
    if collisions or dup_within:
        # HARD FAIL: writing would let upsert_csv silently collapse the colliding
        # rows. Persist the evidence, then abort with a non-zero exit.
        log.error("NATURAL KEY VIOLATION: %d cross-wallet collisions, %d duplicate keys",
                  len(collisions), dup_within)
        ns_dir.mkdir(parents=True, exist_ok=True)
        (ns_dir / "_reconciliation.json").write_text(json.dumps({
            "run_at": now_iso, "wallets": wallets, "natural_key_ok": False,
            "natural_key_cross_wallet_collisions": collisions[:200],
            "natural_key_duplicates_within_run": dup_within,
            "by_wallet": recon_by_wallet,
        }, indent=2), encoding="utf-8")
        raise RuntimeError(
            f"natural key violation: {len(collisions)} cross-wallet collisions, "
            f"{dup_within} duplicates — nothing written (see _reconciliation.json)")

    all_l1.sort(key=lambda r: (r["block_timestamp"], r["tx_hash"], r["event_index"]))
    reg_items = registry.items()
    meta_rows = prices.token_metadata_rows(reg_items, now_iso)
    whitelist_rows = [{
        "token_address": addr, "token_group": S.addr_group(addr),
        "is_active": True, "whitelist_reason": "hypercore_token",
        "created_at": now_iso, "updated_at": now_iso,
    } for addr in reg_items]

    # --- write (upsert by natural key) -----------------------------------------
    def s(v):  # key component -> str; None and "" (CSV read-back) compare equal
        return "" if v is None else str(v)

    as_of_date = datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
    l4x_scope = {(as_of_date, w) for w in wallets}
    upsert_csv(ns_dir / "l1_wallet_ledger.csv", S.L1_WALLET_LEDGER_COLUMNS, all_l1,
               key_fn=lambda r: (s(r["tx_hash"]), s(r["event_index"]), s(r["direction"])),
               sort_fn=lambda r: (s(r["block_timestamp"]), s(r["tx_hash"]), int(r["event_index"])))
    upsert_csv(ns_dir / "l1_token_metadata.csv", S.L1_TOKEN_METADATA_COLUMNS, meta_rows,
               key_fn=lambda r: (s(r["token_address"]),), sort_fn=lambda r: s(r["token_address"]))
    # l4x: replace-partition semantics per (as_of_date, wallet) so a position that
    # closed between two same-day runs does not survive as a stale row.
    upsert_csv(ns_dir / "l4x_perp_position_snapshot.csv", S.L4X_PERP_POSITION_SNAPSHOT_COLUMNS,
               all_snap,
               key_fn=lambda r: (s(r["as_of_date"]), s(r["wallet_address"]), s(r["coin"])),
               sort_fn=lambda r: (s(r["as_of_date"]), s(r["wallet_address"]), s(r["coin"])),
               replace_scope_fn=lambda r: (s(r["as_of_date"]), s(r["wallet_address"])) in l4x_scope)
    upsert_csv(ns_dir / "l4_wallet_metadata.csv", S.L4_WALLET_METADATA_COLUMNS, wallet_meta_rows,
               key_fn=lambda r: (s(r["wallet_address"]),), sort_fn=lambda r: s(r["wallet_address"]))
    upsert_csv(ns_dir / "wallet_watchlist.csv", S.WALLET_WATCHLIST_COLUMNS, watchlist_rows,
               key_fn=lambda r: (s(r["wallet_address"]),), sort_fn=lambda r: s(r["wallet_address"]))
    upsert_csv(ns_dir / "token_whitelist.csv", S.TOKEN_WHITELIST_COLUMNS, whitelist_rows,
               key_fn=lambda r: (s(r["token_address"]),), sort_fn=lambda r: s(r["token_address"]))

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
    if iceberg:
        from .iceberg_load import lift_directory
        from .s3_backfill import load_dotenv
        load_dotenv()
        iceberg_report = lift_directory(ns_dir)

    # --- incremental watermarks (per wallet) -----------------------------------
    if incremental:
        wm_path.write_text(json.dumps(watermarks, indent=2), encoding="utf-8")

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
    contrib_path.write_text(json.dumps(existing, indent=2), encoding="utf-8")

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
        "wallets": wallets,
        "natural_key_cross_wallet_collisions": collisions,
        "natural_key_duplicates_within_run": dup_within,
        "natural_key_ok": not collisions and dup_within == 0,
        "unmapped_ledger_types": unknown_types_all,
        "price_job": price_report,
        "iceberg": iceberg_report,
        "hip3_dexes_checked": dexes,
        "hip3_collateral_by_dex": collateral_report,
        "by_wallet": prev_by_wallet,
    }
    recon_path.write_text(json.dumps(recon, indent=2), encoding="utf-8")
    done = [r for w, r in recon_by_wallet.items() if not r.get("skipped")]
    log.info("done: natural_key_ok=%s fidelity_ok=%s perp_oracle_ok=%s networth_oracle_ok=%s skipped=%d",
             recon["natural_key_ok"],
             all(r["fidelity_ok"] for r in done),
             all(r["oracle"].get("account_value_matches") for r in done),
             all(r["oracle"].get("networth_matches") for r in done),
             len(recon_by_wallet) - len(done))
    return recon


def main() -> None:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    ap = argparse.ArgumentParser(description="Hyperliquid HyperCore -> CSV POC")
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
    ap.add_argument("--out", default=str(OUT_ROOT))
    args = ap.parse_args()
    from .s3_backfill import load_dotenv
    load_dotenv()
    wallets = [w.strip() for w in args.wallet.split(",") if w.strip()]
    run(wallets, args.days, Path(args.out), with_prices=args.with_prices,
        max_workers=args.workers, max_perp_price_coins=args.max_price_coins,
        s3_backfill=args.s3_backfill, iceberg=args.iceberg, incremental=args.incremental,
        hip3=not args.no_hip3)


if __name__ == "__main__":
    main()
