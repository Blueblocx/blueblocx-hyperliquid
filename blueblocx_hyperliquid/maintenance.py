"""
Table maintenance for the lake this pipeline writes (run nightly, under the same
lock as the scheduled run):

    python -m blueblocx_hyperliquid.maintenance --expire-days 7 --compact [--namespace hypercore]
    python -m blueblocx_hyperliquid.maintenance --fix-first-seen      # one-time, v1.6.0 (see below)

What it does per table (every hypercore table in iceberg_load.TABLE_PLAN + control.table_watermarks):
  * expire_snapshots older than --expire-days: pyiceberg 0.12 removes the
    snapshot entries from metadata.json (the thing that grows on every commit and
    is re-read on every commit) — it does NOT delete the data/manifest files those
    snapshots referenced. Those orphans are small for this pipeline; a sweeper that
    deletes files unreferenced by any live snapshot is a documented follow-up.
  * ensure the metadata-retention table properties (TABLE_PROPERTIES) are set.

--compact (v1.8.0, nightly with the expiry): pyiceberg has no rewrite_data_files and
hourly lifts leave one small Parquet per (partition[, event_type]) per hour, which a
DuckDB reader has to open one by one. Compaction rewrites every CLOSED partition that
has >= COMPACT_MIN_FILES data files (closed = its UTC day is at least 2 days old; 1 day
for l4x, which is replaced hourly only on its current day) into one file:
`tx.delete(partition predicate); tx.append(rows)` per partition inside ONE
transaction per table, so the catalog pointer moves once per table per night and a
reader never sees a partition missing. Bounded by an absolute deadline - the next
hourly data tick (HH:10 UTC) minus a minute - because maintenance holds the shared run
lock and a tick that finds it held exits 3 (an hour skipped); what the deadline cuts
off is done the next night. Partitions above COMPACT_MAX_ROWS are skipped (reported).
Old snapshots keep the pre-compaction files alive for readers until the 7-day expiry.

--fix-first-seen (one-time repair after deploying v1.6.0, between ticks): before
v1.6.0 l1_token_metadata.first_seen moved to the last run that saw the token. Set it
to the token's earliest staged ledger event when that is earlier and re-stamp the row
(a content change, so the next lift replaces it in the lake). Exact while the staged
ledger still holds the full history since go-live (45-day retention: run before
2026-11-26). Idempotent; needs no catalog.
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import logging
import os
import sys
import time as _time
from pathlib import Path

from . import config, ops
from . import schema as S

log = logging.getLogger("blueblocx_hyperliquid.maintenance")

COMPACT_MIN_FILES = 8            # fewer data files in a partition: left alone
COMPACT_MAX_ROWS = 3_000_000     # a partition this large is skipped (memory, time) and reported
COMPACT_CLOSED_DAYS = {"l4x_perp_position_snapshot": 1, "l4x_spot_balance_snapshot": 1}   # default 2: UTC days before a partition is closed
_DEFAULT_CLOSED_DAYS = 2
TICK_MINUTE = 10                 # the hourly data timer fires at HH:10 UTC


def next_tick_deadline(now: dt.datetime, tick_minute: int = TICK_MINUTE, margin_s: float = 60.0) -> float:
    """Epoch seconds of the next hourly data tick minus a margin: compaction must have
    released the shared run lock by then, or the tick exits 3 and an hour is skipped."""
    base = now.replace(minute=tick_minute, second=0, microsecond=0)
    if base <= now:
        base += dt.timedelta(hours=1)
    return base.timestamp() - margin_s


def partition_date(partition: dict, date_col: str) -> dt.date | None:
    v = (partition or {}).get(date_col)
    if isinstance(v, dt.datetime):
        return v.date()
    if isinstance(v, dt.date):
        return v
    if isinstance(v, str):
        try:
            return dt.date.fromisoformat(v[:10])
        except ValueError:
            return None
    return None


def compact(namespace: str, catalog, *, now: dt.datetime | None = None, deadline_ts: float | None = None,
            clock=_time, min_files: int = COMPACT_MIN_FILES, max_rows: int = COMPACT_MAX_ROWS) -> dict:
    """Rewrite closed, fragmented partitions of every partitioned table in TABLE_PLAN into
    one file each: one transaction (one catalog commit) per table per call, bounded by
    `deadline_ts` (default: the next hourly tick minus a minute). Never raises for a
    table: an error aborts that table's transaction (nothing committed) and is reported."""
    from pyiceberg.expressions import EqualTo
    from .iceberg_load import TABLE_PLAN, _and, parse_ddl
    now = now or dt.datetime.fromtimestamp(clock.time(), tz=dt.timezone.utc)
    deadline = next_tick_deadline(now) if deadline_ts is None else float(deadline_ts)
    ddl = parse_ddl()
    report: dict = {"deadline": dt.datetime.fromtimestamp(deadline, tz=dt.timezone.utc).isoformat(),
                    "min_files": min_files, "tables": {}}
    for name in TABLE_PLAN:
        part_cols = (ddl.get(name) or {}).get("partition") or []
        if not part_cols:
            continue                                        # dimension tables are rewritten whole by their lifts
        try:
            table = catalog.load_table(f"{namespace}.{name}")
        except Exception:  # noqa: BLE001 - not created yet
            continue
        date_col = part_cols[0]
        closed_before = now.date() - dt.timedelta(days=COMPACT_CLOSED_DAYS.get(name, _DEFAULT_CLOSED_DAYS))
        entry: dict = {"candidates": 0, "compacted": [], "skipped_large": [], "deadline_hit": False,
                       "files_before": 0, "files_after": 0, "rows": 0, "commits": 0}
        report["tables"][f"{namespace}.{name}"] = entry
        try:
            parts = table.inspect.partitions().to_pylist()
        except Exception as e:  # noqa: BLE001
            entry["error"] = f"inspect: {type(e).__name__}: {str(e)[:120]}"
            continue
        cands = [p for p in parts
                 if int(p.get("file_count") or 0) >= min_files
                 and (partition_date(p.get("partition"), date_col) or now.date()) <= closed_before]
        cands.sort(key=lambda p: (partition_date(p["partition"], date_col), str(p["partition"])))
        entry["candidates"] = len(cands)
        if not cands:
            continue
        done: list[dict] = []
        try:
            with table.transaction() as tx:
                for p in cands:
                    if clock.time() >= deadline:
                        entry["deadline_hit"] = True
                        break
                    if int(p.get("record_count") or 0) > max_rows:
                        entry["skipped_large"].append({"partition": p["partition"], "rows": p["record_count"]})
                        continue
                    pred = _and(*[EqualTo(c, p["partition"][c]) for c in part_cols])
                    df = table.scan(row_filter=pred).to_arrow()
                    if df.num_rows != int(p.get("record_count") or 0):
                        entry.setdefault("mismatch", []).append({"partition": p["partition"], "metadata_rows": p["record_count"],
                                                                 "scanned_rows": df.num_rows})
                        continue                            # metadata and data disagree: never rewrite blind
                    tx.delete(pred)
                    tx.append(df)
                    done.append({"partition": {k: (v.isoformat() if hasattr(v, "isoformat") else v)
                                               for k, v in p["partition"].items()},
                                 "files": int(p["file_count"]), "rows": df.num_rows})
        except Exception as e:  # noqa: BLE001 - the transaction is abandoned: nothing was committed
            entry["error"] = f"{type(e).__name__}: {str(e)[:160]}"
            log.warning("compact %s.%s failed, nothing committed: %s", namespace, name, entry["error"])
            continue
        if done:
            table.refresh()
            entry["commits"] = 1
            entry["compacted"] = done
            entry["files_before"] = sum(d["files"] for d in done)
            entry["rows"] = sum(d["rows"] for d in done)
            after = {str(p["partition"]): int(p["file_count"]) for p in table.inspect.partitions().to_pylist()}
            keys = {str({k: p["partition"][k] for k in part_cols}) for p in cands
                    if any(d["rows"] == p["record_count"] for d in done)}
            entry["files_after"] = sum(n for k, n in after.items() if k in keys) or len(done)
            log.info("compact %-34s %d partition(s), %d -> %d file(s), %d rows, one commit", f"{namespace}.{name}",
                     len(done), entry["files_before"], entry["files_after"], entry["rows"])
    return report


def fix_first_seen(ns_dir: Path, now_iso: str) -> dict:
    """Rewrite the staged l1_token_metadata.csv: first_seen := the earliest
    block_timestamp of the token in the staged l1_wallet_ledger.csv when that is
    earlier than the current value; changed rows get updated_at = now_iso."""
    from .extract import _earlier
    ledger, meta = ns_dir / "l1_wallet_ledger.csv", ns_dir / "l1_token_metadata.csv"
    if not meta.exists():
        return {"skipped": "no l1_token_metadata.csv"}
    earliest: dict[str, str] = {}
    if ledger.exists():
        with ledger.open("r", newline="", encoding="utf-8") as fh:
            for r in csv.DictReader(fh):
                addr, ts = r.get("token_address"), r.get("block_timestamp")
                if addr and ts:
                    earliest[addr] = ts if addr not in earliest else _earlier(earliest[addr], ts)
    rows: list[dict] = []
    changed = 0
    with meta.open("r", newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        columns = list(reader.fieldnames or S.L1_TOKEN_METADATA_COLUMNS)
        for r in reader:
            e = earliest.get(r.get("token_address") or "")
            cur = r.get("first_seen") or ""
            if e and _earlier(cur, e) == e and e != cur:
                r["first_seen"], r["updated_at"] = e, now_iso
                changed += 1
            rows.append(r)
    if changed:
        tmp = meta.with_name(meta.name + ".tmp")
        with tmp.open("w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=columns, extrasaction="ignore")
            w.writeheader()
            w.writerows(rows)
        os.replace(tmp, meta)
    log.info("fix-first-seen: %d token row(s), %d moved earlier (re-stamped)", len(rows), changed)
    return {"tokens": len(rows), "changed": changed, "ledger_tokens": len(earliest)}


def expire_snapshots(table, older_than: dt.datetime) -> dict:
    """Expire snapshots older than `older_than` via pyiceberg's maintenance API.
    Returns counts; never raises for a table that has nothing to expire."""
    before = len(list(table.snapshots()))
    if before <= 1:
        return {"snapshots_before": before, "snapshots_after": before, "expired": 0}
    maint = getattr(table, "maintenance", None)
    if maint is None:
        return {"snapshots_before": before, "error": "pyiceberg build has no table.maintenance"}
    exp = maint.expire_snapshots()
    exp = exp.older_than(older_than) if hasattr(exp, "older_than") else exp
    exp.commit()
    table.refresh()
    after = len(list(table.snapshots()))
    return {"snapshots_before": before, "snapshots_after": after, "expired": before - after}


def run(namespace: str, expire_days: float, catalog=None) -> dict:
    """Expire old snapshots and set the retention properties on every table; a catalog
    this function opens itself is released afterwards (SQLite keeps the file open)."""
    from .iceberg_load import dispose_catalog, load_catalog_from_env
    own = catalog is None
    if own:
        catalog, _ = load_catalog_from_env()
    try:
        return _run(namespace, expire_days, catalog)
    finally:
        if own:
            dispose_catalog(catalog)


def _run(namespace: str, expire_days: float, catalog) -> dict:
    from .iceberg_load import CONTROL_NS, OPTIONAL_TABLES, TABLE_PLAN, TABLE_PROPERTIES
    cutoff = dt.datetime.now(tz=dt.timezone.utc) - dt.timedelta(days=expire_days)
    targets = [(namespace, t) for t in TABLE_PLAN] + [(CONTROL_NS, "table_watermarks")]
    report: dict = {"cutoff": cutoff.isoformat(), "tables": {}}
    for ns, name in targets:
        ident = f"{ns}.{name}"
        try:
            table = catalog.load_table(ident)
        except Exception as e:  # noqa: BLE001 — a table that does not exist yet is not an error
            report["tables"][ident] = {"skipped": ("optional table not created (development-only output)"
                                                   if name in OPTIONAL_TABLES else f"{type(e).__name__}")}
            continue
        entry: dict = {}
        missing = {k: v for k, v in TABLE_PROPERTIES.items() if table.properties.get(k) != v}
        if missing:
            try:
                with table.transaction() as tx:
                    tx.set_properties(**missing)
                table.refresh()
                entry["properties_set"] = sorted(missing)
            except Exception as e:  # noqa: BLE001
                entry["properties_error"] = f"{type(e).__name__}: {str(e)[:120]}"
        try:
            entry.update(expire_snapshots(table, cutoff))
        except Exception as e:  # noqa: BLE001
            entry["expire_error"] = f"{type(e).__name__}: {str(e)[:120]}"
        report["tables"][ident] = entry
        log.info("maintenance %-34s %s", ident, entry)
    return report


def main(argv=None) -> int:
    config.load_env()
    config.setup_logging()
    ap = argparse.ArgumentParser(description="Expire old Iceberg snapshots / set retention properties")
    ap.add_argument("--namespace", default=S.NS_HYPERCORE)
    ap.add_argument("--expire-days", type=float, default=7.0)
    ap.add_argument("--out", default=str(config.out_root()), help="state dir (for the run lock)")
    ap.add_argument("--fix-first-seen", action="store_true",
                    help="one-time (v1.6.0): move l1_token_metadata.first_seen to the earliest staged ledger "
                         "event of each token and re-stamp the changed rows; no snapshot expiry in this mode")
    ap.add_argument("--compact", action="store_true",
                    help="rewrite closed, fragmented partitions into one file each (one commit per table), "
                         "bounded by the next hourly tick minus a minute; runs before the snapshot expiry")
    args = ap.parse_args(argv)
    ns_dir = Path(args.out) / S.NS_HYPERCORE
    try:
        with ops.RunLock(ns_dir / ".daily.lock"):
            if args.fix_first_seen:
                report = {"fix_first_seen": fix_first_seen(ns_dir, dt.datetime.now(tz=dt.timezone.utc).isoformat()),
                          "tables": {}}
            else:
                compact_report = None
                catalog = None
                if args.compact:
                    from .iceberg_load import dispose_catalog, load_catalog_from_env
                    catalog, _ = load_catalog_from_env()
                    try:
                        compact_report = compact(args.namespace, catalog)
                    finally:
                        dispose_catalog(catalog)
                report = run(args.namespace, args.expire_days)
                if compact_report is not None:
                    report["compact"] = compact_report
    except ops.LockHeld as e:
        # a data run holds the lock: yield (exit 3 = SuccessExitStatus in the unit, not a
        # failure); the next nightly tick will do the work
        log.warning("%s - yielding to the running data run", e)
        return 3
    print(json.dumps(report, indent=2, default=str))
    errors = [t for t, e in report["tables"].items() if "expire_error" in e or "properties_error" in e]
    errors += [t for t, e in (report.get("compact") or {}).get("tables", {}).items() if "error" in e]
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
