"""FIFO PnL: l4_wallet_pnl_state_fifo and l4_wallet_pnl_realized_1d (atlas crate
fifo-pnl; formulas section 4).

The priced statement = every ledger row that has an ASOF price (1h candle close
at or before the row; stables 1.0). amount_signed > 0 is a buy at that price,
< 0 a sell; two-pointer FIFO per (wallet, token) with QTY_EPSILON 1e-12; a sell
with no lots realizes nothing. Rows dated today are left out (the atlas never
folds the partial day), so state and the daily realized rows agree.

The per-pair realized total and the priced rows are kept on the context for the
positions engine (wallet_token_stats / wallet_trade_summary).
"""
from __future__ import annotations

import datetime as dt

import pyarrow as pa

from .common import (QTY_EPSILON, as_date, asof, ddl_columns, ensure_inputs, group_rows, hourly_prices,
                     sort_key, stable_tokens, table_from_rows, utc)


def price_rows(rows: list[dict], prices: dict[str, list], stables: set[str], cutoff: dt.date) -> list[dict]:
    """The priced statement for one wallet: ledger rows before `cutoff` with an
    ASOF price (stables 1.0), `amount_signed != 0`, deduped on (tx_hash, event_index,
    direction), in ledger order."""
    out: list[dict] = []
    seen: set[tuple] = set()
    for r in sorted(rows, key=sort_key):
        day = as_date(r["block_date"]) or utc(r["block_timestamp"]).date()
        if day >= cutoff:
            continue
        signed = r["amount_signed"]
        if signed is None or float(signed) == 0.0:
            continue
        key = (r["tx_hash"], r["event_index"], r["direction"])
        if key in seen:
            continue
        seen.add(key)
        tok = r["token_address"]
        if tok in stables:
            px = 1.0
        else:
            series = prices.get(tok)
            px = asof(series, utc(r["block_timestamp"])) if series else None
        if px is None or px != px or px in (float("inf"), float("-inf")):
            continue
        out.append({**r, "block_date": day, "block_timestamp": utc(r["block_timestamp"]),
                    "amount_signed": float(signed), "amount": float(r["amount"] or abs(float(signed))),
                    "price_usd": float(px)})
    return out


def fifo_pair(rows: list[dict]) -> tuple[dict, list[dict]]:
    """FIFO over one (wallet, token) priced statement (ledger order). Returns the state
    row and the realized sell rows (matched portion only)."""
    lots: list[list[float]] = []            # [remaining_qty, buy_price]
    sells: list[dict] = []
    last = rows[-1]
    for r in rows:
        q = r["amount_signed"]
        if q > 0:
            lots.append([q, r["price_usd"]])
            continue
        sell_qty = -q
        remaining, matched, cost = sell_qty, 0.0, 0.0
        while lots and remaining > QTY_EPSILON:
            lot = lots[0]
            take = min(lot[0], remaining)
            matched += take
            cost += take * lot[1]
            lot[0] -= take
            remaining -= take
            if lot[0] <= QTY_EPSILON:
                lots.pop(0)
        if matched > QTY_EPSILON:
            proceeds = matched * r["price_usd"]
            sells.append({"wallet_address": r["wallet_address"], "token_address": r["token_address"],
                          "block_date": r["block_date"], "block_timestamp": r["block_timestamp"],
                          "tx_hash": r["tx_hash"], "event_index": r["event_index"], "sell_qty": sell_qty,
                          "sell_price_usd": r["price_usd"], "matched_qty": matched, "proceeds_usd": proceeds,
                          "cost_usd": cost, "realized_pnl_usd": proceeds - cost})
    state = {"wallet_address": last["wallet_address"], "token_address": last["token_address"],
             "open_qty": sum(l[0] for l in lots), "open_cost_usd": sum(l[0] * l[1] for l in lots),
             "last_block_timestamp": last["block_timestamp"], "last_tx_hash": last["tx_hash"],
             "last_event_index": int(last["event_index"] or 0)}
    return state, sells


def opening_lots(ctx, prices: dict[str, list], stables: set[str]) -> dict[tuple[str, str], dict]:
    """(wallet, token) -> a synthetic BUY row for the holding the balance anchor shows
    the wallet already had before its first ledger row (balances.compute registers
    `balance_openings`), priced ASOF at that first instant. Without it every later
    sale of a pre-history holding would be an unbacked sell (no PnL at all); with it
    the cost basis is the market price at the start of our history - documented
    HyperCore deviation, the atlas has the chain's whole history instead."""
    if "balance_openings" not in ctx._loaded:
        return {}
    out: dict[tuple[str, str], dict] = {}
    arrow = ctx.sql("SELECT wallet_address, token_address, opening_qty, first_block_timestamp FROM balance_openings")
    for w, t, qty, ts in zip(*(arrow.column(c).to_pylist() for c in
                               ("wallet_address", "token_address", "opening_qty", "first_block_timestamp"))):
        ts = utc(ts)
        if t in stables:
            px = 1.0
        else:
            series = prices.get(t)
            px = asof(series, ts) if series else None
        if px is None or qty is None or qty <= 0:
            continue
        out[(w, t)] = {"wallet_address": w, "token_address": t, "counterparty_address": None,
                       "block_timestamp": ts - dt.timedelta(microseconds=1), "block_date": ts.date(),
                       "tx_hash": f"hlcore:{w}:opening:{t}", "event_index": 0, "direction": "in",
                       "amount_raw": None, "amount": float(qty), "amount_signed": float(qty), "price_usd": float(px)}
    return out


def compute(ctx) -> dict[str, pa.Table]:
    ensure_inputs(ctx, ledger=True, meta=True, px1h=True)
    prices = hourly_prices(ctx)
    stables = stable_tokens(ctx)
    openings = opening_lots(ctx, prices, stables)
    ledger = ctx.sql("SELECT wallet_address, token_address, counterparty_address, block_timestamp, block_date, "
                     "tx_hash, event_index, direction, amount_raw, amount, amount_signed FROM ledger")
    cutoff = ctx.today                         # rows dated today are not folded
    states: list[dict] = []
    sells: list[dict] = []
    priced: list[dict] = []
    for (wallet,), rows in group_rows(ledger, ("wallet_address",)).items():
        pr = price_rows(rows, prices, stables, cutoff)
        priced.extend(pr)
        by_token: dict[str, list[dict]] = {}
        for r in pr:
            by_token.setdefault(r["token_address"], []).append(r)
        for _tok, trows in by_token.items():
            opening = openings.get((wallet, _tok))
            if opening is not None and opening["block_date"] < cutoff:
                trows = [opening] + trows      # the pre-history holding is the first lot
            st, sl = fifo_pair(trows)
            states.append(st)
            sells.extend(sl)
    # daily realized per wallet
    daily: dict[tuple, dict] = {}
    for s in sells:
        k = (s["wallet_address"], s["block_date"])
        d = daily.setdefault(k, {"wallet_address": k[0], "snapshot_date": k[1], "realized_pnl_usd": 0.0,
                                 "proceeds_usd": 0.0, "cost_usd": 0.0, "num_sells": 0})
        d["realized_pnl_usd"] += s["realized_pnl_usd"]
        d["proceeds_usd"] += s["proceeds_usd"]
        d["cost_usd"] += s["cost_usd"]
        d["num_sells"] += 1
    state_t = table_from_rows(states, ddl_columns("l4_wallet_pnl_state_fifo"))
    realized_t = table_from_rows(list(daily.values()), ddl_columns("l4_wallet_pnl_realized_1d"))
    # side tables for the positions engine
    pair_cols = [("wallet_address", "STRING"), ("token_address", "STRING"), ("realized_pnl_usd", "DOUBLE")]
    pair_tot: dict[tuple, float] = {}
    for s in sells:
        k = (s["wallet_address"], s["token_address"])
        pair_tot[k] = pair_tot.get(k, 0.0) + s["realized_pnl_usd"]
    ctx.register("fifo_pair_realized", table_from_rows(
        [{"wallet_address": w, "token_address": t, "realized_pnl_usd": v} for (w, t), v in pair_tot.items()], pair_cols))
    priced_cols = [("wallet_address", "STRING"), ("token_address", "STRING"), ("block_date", "DATE"),
                   ("direction", "STRING"), ("amount", "DOUBLE"), ("amount_signed", "DOUBLE"), ("price_usd", "DOUBLE")]
    ctx.register("priced_statement", table_from_rows(priced, priced_cols))
    ctx.register("fifo_state_new", state_t)
    return {"l4_wallet_pnl_state_fifo": state_t, "l4_wallet_pnl_realized_1d": realized_t}
