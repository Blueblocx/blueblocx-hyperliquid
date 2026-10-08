"""
L4 -> all_chain.wallet_analytics rollup for HyperCore, plus the portfolio oracle
validation.

The stock L4 layer (FIFO/balance/networth) runs unchanged on the spot legs, but
PERP realized PnL lives as USDC-pinned-$1 ledger rows that FIFO nets to ~$0. So we
compute the perp-aware analytics here and reconcile against Hyperliquid's own
`portfolio` series (the numbers the HL UI shows) as ground truth.

Metric definitions (documented once, here — see MH-12/13/14/15):
  realized_pnl_Nd = Σ amount_signed over REALIZED_PNL_EVENT_TYPES in [now-N, now]
                    (perp closedPnl + funding − fees + staking rewards), plus FIFO
                    spot realized PnL (computed by the stock L4 layer, added there).
  unrealized_pnl  = Σ l4x.unrealized_pnl_usd  (open perp mark-to-market)
  net_worth       = perp accountValue (clearinghouseState, ground truth)
                      minus its overlap with spot USDC.spotHold (portfolio margin)
                    + spot balances × price (spotClearinghouseState; stables $1)
                    + staked HYPE × price (delegatorSummary)
                    + vault equity (userVaultEquities)
  net_flow_Nd     = Σ USD over FLOW_EVENT_TYPES only (deposit/withdraw/external
                    transfer) — NEVER perp pnl/fee/funding.
"""
from __future__ import annotations

from datetime import datetime, timezone


def _to_ms(iso: str) -> int:
    return int(datetime.fromisoformat(iso).timestamp() * 1000)


def _window_sum(rows, event_types, now_ms, days, price_of=None):
    """Σ amount_signed (or USD via price_of) over rows whose event_type is in
    `event_types` and whose timestamp is within the last `days`."""
    start = now_ms - days * 86400 * 1000
    total = 0.0
    for r in rows:
        if r["event_type"] not in event_types:
            continue
        if _to_ms(r["block_timestamp"]) < start:
            continue
        if price_of is None:
            total += r["amount_signed"]
        else:
            px = price_of(r)
            total += r["amount_signed"] * (px if px is not None else 0.0)
    return total


def lending_position(ledger_updates) -> dict[str, float]:
    """Outstanding supply to Hyperliquid's borrow/lend market per token, derived
    from the FULL ledger history: Σ supply − Σ withdraw + Σ realised interest.

    There is no Info endpoint for lending state (all candidates return 422), and
    for non-portfolio-margin accounts the supplied USDC is NOT in
    spotClearinghouseState.balances — yet portfolio.allTime counts it. Verified:
    0x4fb2… 5,599.40 + 6.60 interest = 5,606.00 == oracle gap exactly; 0xf481…
    722,867 vs 724,023 (0.13% = accrued, not yet realised, interest).
    """
    pos: dict[str, float] = {}
    for e in ledger_updates or []:
        d = e.get("delta") or {}
        if d.get("type") != "borrowLend":
            continue
        tok = d.get("token") or "USDC"
        amt = float(d.get("amount", 0) or 0)
        interest = float(d.get("interestAmount", 0) or 0)
        op = (d.get("operation") or "").lower()
        if op == "supply":
            pos[tok] = pos.get(tok, 0.0) + amt
        elif op in ("withdraw", "unsupply", "withdrawsupply"):
            pos[tok] = pos.get(tok, 0.0) - amt
        elif op in ("borrow",):
            pos[tok] = pos.get(tok, 0.0) - amt          # a borrow is a liability
        elif op in ("repay",):
            pos[tok] = pos.get(tok, 0.0) + amt
        pos[tok] = pos.get(tok, 0.0) + interest
    return {k: v for k, v in pos.items() if abs(v) > 1e-9}


def merge_lending(stored: dict, delta: dict) -> dict[str, float]:
    """Cached lending position + position change from new borrowLend deltas."""
    out = {k: float(v) for k, v in (stored or {}).items()}
    for k, v in (delta or {}).items():
        out[k] = out.get(k, 0.0) + float(v)
    return {k: v for k, v in out.items() if abs(v) > 1e-9}


def compute_analytics(l1_rows, snap_rows, chs, spot_state, now_ms, now_iso,
                      windows=(1, 7, 30), spot_price_by_symbol=None, staking=None,
                      token_usd_price=None, vault_equities=None,
                      window_start_ms=None, fills_truncated=False,
                      account_value_by_dex=None, lending_by_token=None, open_orders=None):
    """Build the HyperCore contribution to all_chain.wallet_analytics.

    Every ledger row is valued in USD via token_usd_price (stables = $1, spot
    tokens from spotMetaAndAssetCtxs); rows without a price are counted in
    unvalued_ledger_rows rather than summed. Spot realized PnL (FIFO) is the stock
    L4 layer's job and is added there. Returns a dict shaped like the
    wallet_analytics columns we can populate from HyperCore alone.
    """
    from . import schema as S

    # --- USD valuation of ledger rows -----------------------------------------
    # Ledger legs are NOT all USDC: spot-buy fee legs are in the BASE token, spot
    # quote legs may be a non-USDC quote token. Summing amount_signed as if USD
    # would be wrong, so every row is valued via token_usd_price (USDC/stables=1,
    # spot tokens from spotMetaAndAssetCtxs). Rows with no price are COUNTED and
    # surfaced (unvalued_rows), never silently summed.
    px_by_addr = dict(token_usd_price or {})
    px_by_addr.setdefault(S.USDC_ADDRESS, 1.0)
    unvalued = {"count": 0, "token_addresses": set()}

    def usd_price(r):
        px = px_by_addr.get(r["token_address"])
        if px is None:
            unvalued["count"] += 1
            unvalued["token_addresses"].add(r["token_address"])
        return px

    # --- window coverage: a metric labelled Nd is only valid if we ingested >= N
    # days AND the fills were not truncated by the 10k API cap. Incomplete windows
    # are emitted as None with an explicit *_complete=False flag, never as a
    # smaller number wearing the wrong label.
    coverage_days = ((now_ms - window_start_ms) / 86_400_000
                     if window_start_ms is not None else float("inf"))

    def complete(d):
        return coverage_days + 1e-9 >= d and not fills_truncated

    # --- realized PnL (perp side): closedPnl + funding - fees + staking ----------
    realized = {d: (_window_sum(l1_rows, S.REALIZED_PNL_EVENT_TYPES, now_ms, d,
                                price_of=usd_price) if complete(d) else None)
                for d in windows}

    # --- net_flow: ONLY capital in/out, never PnL --------------------------------
    net_flow = {d: (_window_sum(l1_rows, S.FLOW_EVENT_TYPES, now_ms, d,
                                price_of=usd_price) if complete(d) else None)
                for d in windows}
    # perp-book cashflow over the last 24h (closedPnl + funding − fees), for the
    # PnL oracle: HL perpDay.pnlHistory Δ == this + Δ unrealized
    realized_perp_24h = _window_sum(l1_rows, S.PERP_CASHFLOW_TYPES, now_ms, 1, price_of=usd_price)

    def _r(v):
        return None if v is None else round(v, 6)

    # --- unrealized PnL + perp equity (mark-to-market) -------------------------
    unrealized = sum(r["unrealized_pnl_usd"] for r in snap_rows)
    account_value = float((chs.get("marginSummary", {}) or {}).get("accountValue", 0) or 0)

    # --- spot net worth: absolute balances valued at L3 close (stables at $1) --
    prices_by_sym = spot_price_by_symbol or {}
    stables = {"USDC", "USDT0", "USDT", "USDHL", "FEUSD"}
    spot_value = 0.0          # total spot book value (stables + priced tokens)
    stable_value = 0.0        # stable-only, for stable_coin_pct
    spot_balances = []
    for b in (spot_state.get("balances", []) if spot_state else []):
        total = float(b.get("total", 0) or 0)
        if total == 0.0:
            continue
        coin = b.get("coin")
        if coin and coin.upper() in stables:
            usd = total
            stable_value += usd
        else:
            px = prices_by_sym.get(coin)
            usd = total * px if px is not None else None
        spot_balances.append({
            "coin": coin, "total": total,
            "usd_value": (round(usd, 6) if usd is not None else None),
            "priced": usd is not None,
        })
        if usd is not None:
            spot_value += usd

    # --- staked HYPE: real net worth not in spotClearinghouseState -------------
    stk = staking or {}
    staked_hype = (float(stk.get("delegated", 0) or 0)
                   + float(stk.get("undelegated", 0) or 0)
                   + float(stk.get("totalPendingWithdrawal", 0) or 0))
    hype_px = prices_by_sym.get("HYPE")
    staked_value = staked_hype * hype_px if (staked_hype and hype_px) else 0.0

    # --- vault equity: capital deposited into HL vaults (not in perp/spot/staking)
    vault_value = 0.0
    for v in (vault_equities or []):
        try:
            vault_value += float(v.get("equity", 0) or 0)
        except (TypeError, ValueError):
            continue

    # --- perp/spot overlap (portfolio margin) ----------------------------------
    # In portfolio-margin (unified) accounts the perp book is collateralised by
    # spot USDC: spotClearinghouseState reports that collateral as USDC.spotHold
    # and it is ALREADY inside USDC.total. Adding clearinghouseState.accountValue
    # on top double counts it. Data-driven rule: the overlap is min(accountValue,
    # spotHold); non-unified accounts have no spotHold -> overlap 0 -> full
    # accountValue is added (verified: MM wallet net_worth == accountValue == oracle;
    # unified wallet residual == accountValue before this correction).
    # HIP-3 builder dexes (xyz, flx, ...) each have their OWN margin account that
    # clearinghouseState(main) does not report (verified live: perpAllTime ==
    # main + Σ dex). Their equity is part of TOTAL perp equity.
    other_dex_value = sum(float(v or 0) for k, v in (account_value_by_dex or {}).items() if k)
    total_perp = account_value + other_dex_value

    # Overlap rule — is this a UNIFIED account (perp margin drawn from spot USDC,
    # so perp equity is already inside spot USDC.total) or a SEPARATE one (perp
    # book funded on its own)? Verified on 6 live wallets:
    #   * unified: USDC.spotHold present (portfolio-margin flag), OR the part of
    #     USDC.hold not explained by resting spot BUY orders ≈ perp totalMarginUsed
    #     (48M whale: hold 4.63M − bids 0.06M = 4.57M vs margin 4.44M; oracle ==
    #     spot + staked, perp not additive)
    #   * separate: no USDC row (MM) or hold == 0 while perp margin > 0 (0x16c9…:
    #     oracle == perp + spot, no overlap)
    # For unified accounts overlap = min(total_perp, USDC.total); else 0.
    usdc_total = 0.0
    spot_hold = 0.0
    hold = 0.0
    for b in (spot_state.get("balances", []) if spot_state else []):
        if b.get("coin") == "USDC":
            try:
                usdc_total = float(b.get("total", 0) or 0)
                spot_hold = float(b.get("spotHold", 0) or 0)
                hold = float(b.get("hold", 0) or 0)
            except (TypeError, ValueError):
                usdc_total, spot_hold, hold = 0.0, 0.0, 0.0
    spot_bid_lock = 0.0
    for o in (open_orders or []):
        c = o.get("coin", "")
        if ("/" in c or c.startswith("@")) and o.get("side") == "B":
            try:
                spot_bid_lock += float(o.get("sz", 0) or 0) * float(o.get("limitPx", 0) or 0)
            except (TypeError, ValueError):
                pass
    # USDC.hold not explained by resting spot BUY orders is USDC reserved for the
    # perp account: ≈ totalMarginUsed while positions are open (whale 4.57M vs
    # 4.44M), == perp accountValue when none are (0xfe35…: 34,618.95 both). Any
    # material unexplained hold therefore marks a unified account; margin used
    # alone cannot be the test because it is 0 for a flat unified account.
    hold_perp = max(0.0, hold - spot_bid_lock)
    if usdc_total <= 0 or total_perp <= 0:
        unified, overlap_rule = False, "none"
    elif spot_hold > 0:
        unified, overlap_rule = True, "spotHold"
    elif hold_perp >= max(1.0, 0.02 * total_perp):
        unified, overlap_rule = True, "hold_reserved"
    else:
        unified, overlap_rule = False, "separate"
    perp_overlap = min(total_perp, usdc_total) if unified else 0.0
    perp_contrib = total_perp - perp_overlap

    # --- lending market supply (not in spot balances for non-PM accounts) --------
    # For portfolio-margin accounts the USDC row carries `supplied` and USDC.total
    # already includes it (light wallet: oracle matched without this term), so the
    # ledger-derived position is added only when the balance row does NOT expose it.
    lending_value = 0.0
    lending_detail: dict[str, float] = {}
    exposes_supplied = any("supplied" in b for b in (spot_state.get("balances", []) if spot_state else []))
    if not exposes_supplied:
        for tok, qty in (lending_by_token or {}).items():
            px = 1.0 if tok.upper() in stables else prices_by_sym.get(tok)
            if px is None:
                unvalued["count"] += 1
                unvalued["token_addresses"].add(f"lending:{tok}")
                continue
            lending_detail[tok] = round(qty * px, 6)
            lending_value += qty * px

    net_worth = perp_contrib + spot_value + staked_value + vault_value + lending_value

    # --- stable_coin_pct: stable holdings (spot stables + perp USDC) / net worth
    # perp accountValue is USDC-denominated collateral, so it counts as stable.
    stable_total = stable_value + perp_contrib + vault_value + sum(
        v for t, v in lending_detail.items() if t.upper() in stables)
    stable_pct = (stable_total / net_worth) if net_worth else 0.0
    all_priced = all(b["priced"] for b in spot_balances)

    return {
        "as_of_date": now_iso[:10],
        "wallet_address": (l1_rows[0]["wallet_address"] if l1_rows else None),
        "chains": ["hypercore"],
        "net_worth": round(net_worth, 6),
        "realized_pnl_24h": _r(realized.get(1)),
        "realized_pnl_7d": _r(realized.get(7)),
        "realized_pnl_30d": _r(realized.get(30)),
        "realized_pnl_24h_complete": complete(1),
        "realized_pnl_7d_complete": complete(7),
        "realized_pnl_30d_complete": complete(30),
        "unrealized_pnl": round(unrealized, 6),
        "realized_perp_24h": round(realized_perp_24h, 6),
        "net_flow_24h": _r(net_flow.get(1)),
        "net_flow_7d": _r(net_flow.get(7)),
        "net_flow_30d": _r(net_flow.get(30)),
        "coverage_days": (None if coverage_days == float("inf") else round(coverage_days, 3)),
        "fills_truncated": bool(fills_truncated),
        "stable_coin_pct": round(stable_pct, 6),
        "account_value_usd": round(account_value, 6),
        "spot_value_usd": round(spot_value, 6),
        "staked_hype": round(staked_hype, 6),
        "staked_value_usd": round(staked_value, 6),
        "vault_value_usd": round(vault_value, 6),
        "lending_value_usd": round(lending_value, 6),
        "lending_by_token_usd": lending_detail,
        "perp_spot_overlap_usd": round(perp_overlap, 6),
        "perp_spot_overlap_rule": overlap_rule,
        "account_value_other_dex_usd": round(other_dex_value, 6),
        "account_value_by_dex": {k: round(float(v or 0), 6)
                                 for k, v in (account_value_by_dex or {}).items()},
        "spot_all_priced": all_priced,
        "spot_balances": spot_balances,
        "unvalued_ledger_rows": unvalued["count"],
        "unvalued_token_addresses": sorted(unvalued["token_addresses"]),
    }


def pnl_oracle(portfolio_map: dict, realized_perp_24h: float, unrealized_now: float,
               prior_unrealized, prior_age_h, tol_abs: float = 5.0, tol_pct: float = 0.02) -> dict:
    """EXTERNAL PnL check (the headline number). HL's perpDay.pnlHistory is the
    cumulative perp PnL series the UI shows (net of deposits/withdrawals); its
    change over the last 24h must equal our realized perp cashflow (closedPnl +
    funding − fees) plus the change in unrealized PnL since the previous daily
    snapshot. Needs a prior l4x snapshot 20–28h old, so it activates from the
    second daily run; until then it is reported as pending, never as passed."""
    hist = ((portfolio_map.get("perpDay") or {}).get("pnlHistory") or [])
    if len(hist) < 2:
        return {"checked": False, "reason": "no perpDay pnlHistory"}
    if prior_unrealized is None:
        return {"checked": False, "reason": "no prior snapshot (~24h) yet"}
    oracle_delta = float(hist[-1][1]) - float(hist[0][1])
    ours = realized_perp_24h + (unrealized_now - prior_unrealized)
    diff = abs(oracle_delta - ours)
    tol = max(tol_abs, tol_pct * abs(oracle_delta))
    return {"checked": True, "oracle_pnl_24h": round(oracle_delta, 6), "computed_pnl_24h": round(ours, 6),
            "realized_perp_24h": round(realized_perp_24h, 6),
            "unrealized_delta_24h": round(unrealized_now - prior_unrealized, 6),
            "prior_snapshot_age_h": round(prior_age_h or 0, 2), "abs_diff": round(diff, 6),
            "pnl_matches": diff <= tol}


def validate_against_oracle(analytics, portfolio, now_ms, tol_pct=0.005,
                            networth_tol_pct=0.01, prior_unrealized=None, prior_age_h=None):
    """Compare computed values to Hyperliquid's own portfolio series (the HL UI
    numbers). Two ASSERTED checks:

      - PERP: computed account_value_usd (clearinghouseState) must match
        `perpAllTime` accountValue (same engine figure) within tol_pct.
      - FULL: computed net_worth (perp - spot overlap + spot + staked + vault)
        must match `allTime` accountValue within networth_tol_pct. Verified on a
        portfolio-margin wallet (gap -0.001%) and a plain perp wallet (gap 0).

    portfolio is [[period, {accountValueHistory: [[ms, "val"], ...], pnlHistory}]].
    """
    result = {"checked": False}
    if not portfolio:
        return result
    d = {p[0]: p[1] for p in portfolio if isinstance(p, list) and len(p) == 2}

    def latest_av(period):
        h = (d.get(period, {}) or {}).get("accountValueHistory", []) or []
        return float(h[-1][1]) if h else None

    perp_oracle = latest_av("perpAllTime")
    full_oracle = latest_av("allTime")
    result["checked"] = True

    if perp_oracle is not None:
        # perpAllTime includes HIP-3 dex equity (verified live: == main + Σ dex)
        computed_perp = (analytics.get("account_value_usd", 0.0)
                         + analytics.get("account_value_other_dex_usd", 0.0))
        diff = abs(computed_perp - perp_oracle)
        denom = abs(perp_oracle) or 1.0
        result.update({
            "perp_oracle_account_value": round(perp_oracle, 6),
            "computed_perp_account_value": round(computed_perp, 6),
            "perp_pct_diff": round(diff / denom, 6),
            "account_value_matches": (diff / denom) <= tol_pct,
        })
    if full_oracle is not None:
        computed_full = analytics.get("net_worth", 0.0)
        gap = full_oracle - computed_full
        denom = abs(full_oracle) or 1.0
        result.update({
            "full_oracle_networth": round(full_oracle, 6),
            "computed_networth": round(computed_full, 6),
            "networth_gap": round(gap, 6),
            "networth_pct_diff": round(abs(gap) / denom, 6),
            "networth_matches": (abs(gap) / denom) <= networth_tol_pct,
        })
    result["pnl"] = pnl_oracle(d, analytics.get("realized_perp_24h", 0.0),
                               analytics.get("unrealized_pnl", 0.0), prior_unrealized, prior_age_h)
    return result
