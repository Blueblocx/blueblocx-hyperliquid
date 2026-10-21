"""l4_wallet_balance_per_tx and l4_wallet_balance_1d_dense (atlas crates wallet-balance
and wallet-balance-1d; docs/ATLAS_WALLET_TABLE_FORMULAS.md sections 1 and 2).

Per wallet, over its WHOLE ledger (every row is a token movement of that wallet's
balance, including the USDC cashflow of perps): a clamped running balance per
token in ledger order, then one dense row per (token, UTC day) from the pair's
first day to today, carrying the closing balance forward.

HyperCore anchor (the one deviation from the atlas). On an EVM chain the ledger
holds the token's whole history, so a running sum IS the balance. A tracked
HyperCore wallet's ledger starts at its first pull (and the Info API caps fills at
10k), so the running sum alone is a balance "since go-live". The extractor
therefore stages the exchange's own balances every run (l4x_spot_balance_snapshot);
the running sum of a pair is shifted so that it equals that balance at the
snapshot instant (offset = snapshot.total - Σ movements up to it), then clamped
at zero like the atlas. The pre-history holding the offset reveals is handed to
the FIFO engine as an opening lot (fifo.py).
"""
from __future__ import annotations

import datetime as dt

import pyarrow as pa

from .common import EPOCH, as_date, days_between, ddl_columns, ensure_inputs, group_rows, sort_key, table_from_rows, utc


def running_balances(rows: list[dict], anchor: tuple[dt.datetime, float] | None = None) -> tuple[list[dict], float]:
    """balance_per_tx rows for one (wallet, token): rows in ledger order, the
    self-transfer rule applied, `balance_amount` = max(0, offset + running) where
    `offset` pins the running sum to `anchor` = (as_of_ts, exchange balance) when
    given (0.0 without an anchor). Returns (rows, offset)."""
    ordered: list[dict] = []
    for r in sorted(rows, key=sort_key):
        wallet, cp, direction = r["wallet_address"], r["counterparty_address"], r["direction"]
        signed = float(r["amount_signed"] or 0.0)
        if cp is not None and cp == wallet:
            if direction == "in":
                continue                      # the self-transfer "in" leg is dropped ...
            signed = 0.0                      # ... and the "out" leg carries no movement
        ordered.append((r, signed))
    offset = 0.0
    if anchor is not None:
        as_of_ts, total = anchor
        cum_at_anchor = sum(signed for r, signed in ordered if utc(r["block_timestamp"]) <= as_of_ts)
        offset = float(total) - cum_at_anchor
    out: list[dict] = []
    running = offset
    for r, signed in ordered:
        running = max(0.0, running + signed)
        out.append({
            "wallet_address": r["wallet_address"], "token_address": r["token_address"],
            "block_timestamp": utc(r["block_timestamp"]), "block_date": as_date(r["block_date"]) or utc(r["block_timestamp"]).date(),
            "tx_hash": r["tx_hash"], "event_index": int(r["event_index"] or 0), "direction": r["direction"],
            "amount_raw": float(r["amount_raw"] or 0.0), "amount": float(r["amount"] or 0.0),
            "amount_signed": signed, "balance_amount": running,
        })
    return out, offset


def dense_days(per_tx: list[dict], today: dt.date, opening: float = 0.0,
               quiet_pair: tuple[str, str, dt.date, float] | None = None) -> list[dict]:
    """balance_1d_dense rows for one (wallet, token) from its per_tx rows (ledger order):
    opening = previous close (the anchor-implied holding before the first row for the
    first day), closing = balance of the day's last row else carried, in/out = sums of
    positive / negative movements, stop after the pair is flat. `quiet_pair` =
    (wallet, token, as_of_date, total) for a holding with no ledger rows at all:
    carried from its snapshot day."""
    if not per_tx:
        if quiet_pair is None:
            return []
        wallet, token, first, closing = quiet_pair
        return [{"wallet_address": wallet, "token_address": token, "snapshot_date": day,
                 "opening_balance_amount": closing, "closing_balance_amount": closing,
                 "net_flow_amount": 0.0, "in_amount": 0.0, "out_amount": 0.0, "closing_block_timestamp": EPOCH}
                for day in days_between(first, today)] if closing > 0 else []
    by_day: dict[dt.date, list[dict]] = {}
    for r in per_tx:
        by_day.setdefault(r["block_date"], []).append(r)
    first = min(by_day)
    out: list[dict] = []
    prev_close, prev_ts = max(0.0, opening), EPOCH
    wallet, token = per_tx[0]["wallet_address"], per_tx[0]["token_address"]
    for day in days_between(first, today):
        rows = by_day.get(day)
        if rows:
            last = rows[-1]                   # per_tx is already in ledger order
            closing, closing_ts = last["balance_amount"], last["block_timestamp"]
            in_amt = sum(r["amount_signed"] for r in rows if r["amount_signed"] > 0)
            out_amt = sum(-r["amount_signed"] for r in rows if r["amount_signed"] < 0)
        else:
            if prev_close == 0.0:
                # flat and quiet: the atlas stops carrying the pair at the next chunk
                # boundary; we stop at the first quiet day so the table does not grow
                # for dead pairs. A later row reopens the pair from that day on.
                continue
            closing, closing_ts, in_amt, out_amt = prev_close, prev_ts, 0.0, 0.0
        out.append({
            "wallet_address": wallet, "token_address": token, "snapshot_date": day,
            "opening_balance_amount": prev_close, "closing_balance_amount": closing,
            "net_flow_amount": in_amt - out_amt, "in_amount": in_amt, "out_amount": out_amt,
            "closing_block_timestamp": closing_ts,
        })
        prev_close, prev_ts = closing, closing_ts
    return out


def load_anchors(ctx) -> dict[tuple[str, str], tuple[dt.datetime, float]]:
    """(wallet, token) -> (as_of_ts, total) from the latest spot balance snapshot per pair
    (empty when the table does not exist yet: pre-v1.9.0 lake)."""
    if not ctx.has_table("l4x_spot_balance_snapshot"):
        return {}
    ctx.load("l4x_spot_balance_snapshot", date_col="as_of_date", alias="spot_snap",
             since=ctx.today - dt.timedelta(days=45))
    arrow = ctx.sql("SELECT wallet_address, token_address, arg_max(total, as_of_ts) AS total, max(as_of_ts) AS as_of_ts "
                    "FROM spot_snap GROUP BY 1, 2")
    return {(w, t): (utc(ts), float(total or 0.0)) for w, t, total, ts in zip(
        *(arrow.column(c).to_pylist() for c in ("wallet_address", "token_address", "total", "as_of_ts")))}


def compute(ctx) -> dict[str, pa.Table]:
    ensure_inputs(ctx, ledger=True, meta=False)
    anchors = load_anchors(ctx)
    ledger = ctx.sql("SELECT wallet_address, token_address, counterparty_address, block_timestamp, block_date, "
                     "tx_hash, event_index, direction, amount_raw, amount, amount_signed FROM ledger")
    per_tx_rows: list[dict] = []
    dense_rows: list[dict] = []
    openings: list[dict] = []
    groups = group_rows(ledger, ("wallet_address", "token_address"))
    for key, rows in groups.items():
        pt, offset = running_balances(rows, anchors.get(key))
        per_tx_rows.extend(pt)
        dense_rows.extend(dense_days(pt, ctx.today, opening=offset))
        if offset > 0 and pt:
            openings.append({"wallet_address": key[0], "token_address": key[1], "opening_qty": offset,
                             "first_block_timestamp": pt[0]["block_timestamp"]})
    # holdings the exchange reports for which the lake has no ledger row at all
    wallets = set(ctx.wallets) if ctx.wallets is not None else None
    for key, (as_of_ts, total) in anchors.items():
        if key in groups or total <= 0 or (wallets is not None and key[0] not in wallets):
            continue
        dense_rows.extend(dense_days([], ctx.today, quiet_pair=(key[0], key[1], as_of_ts.date(), total)))
        openings.append({"wallet_address": key[0], "token_address": key[1], "opening_qty": total,
                         "first_block_timestamp": as_of_ts})
    per_tx = table_from_rows(per_tx_rows, ddl_columns("l4_wallet_balance_per_tx"))
    dense = table_from_rows(dense_rows, ddl_columns("l4_wallet_balance_1d_dense"))
    ctx.register("balance_per_tx_new", per_tx)
    ctx.register("balance_1d_new", dense)
    ctx.register("balance_openings", table_from_rows(openings, [
        ("wallet_address", "STRING"), ("token_address", "STRING"), ("opening_qty", "DOUBLE"),
        ("first_block_timestamp", "TIMESTAMP")]))
    return {"l4_wallet_balance_per_tx": per_tx, "l4_wallet_balance_1d_dense": dense}
