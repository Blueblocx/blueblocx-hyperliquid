"""
Table maintenance for the lake this pipeline writes (run nightly, under the same
lock as the scheduled run):

    python -m blueblocx_hyperliquid.maintenance --expire-days 7 [--namespace hypercore]
    python -m blueblocx_hyperliquid.maintenance --fix-first-seen      # one-time, v1.6.0 (see below)

What it does per table (the 7 hypercore tables + control.table_watermarks):
  * expire_snapshots older than --expire-days: pyiceberg 0.12 removes the
    snapshot entries from metadata.json (the thing that grows on every commit and
    is re-read on every commit) — it does NOT delete the data/manifest files those
    snapshots referenced. Those orphans are small for this pipeline; a sweeper that
    deletes files unreferenced by any live snapshot is a documented follow-up.
  * ensure the metadata-retention table properties (TABLE_PROPERTIES) are set.

pyiceberg has no rewrite_data_files; hourly lifts leave one small Parquet per
(partition, event_type) per hour. Compaction = re-write a closed partition with
`table.overwrite(df, overwrite_filter=EqualTo(partition_col, day))`; not done
here yet (the files are tens of KB to a few MB; readers on this host are fine).

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
from pathlib import Path

from . import config, ops
from . import schema as S

log = logging.getLogger("blueblocx_hyperliquid.maintenance")


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
    from .iceberg_load import CONTROL_NS, OPTIONAL_TABLES, TABLE_PLAN, TABLE_PROPERTIES, load_catalog_from_env
    if catalog is None:
        catalog, _ = load_catalog_from_env()
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
    args = ap.parse_args(argv)
    ns_dir = Path(args.out) / S.NS_HYPERCORE
    try:
        with ops.RunLock(ns_dir / ".daily.lock"):
            if args.fix_first_seen:
                report = {"fix_first_seen": fix_first_seen(ns_dir, dt.datetime.now(tz=dt.timezone.utc).isoformat()),
                          "tables": {}}
            else:
                report = run(args.namespace, args.expire_days)
    except ops.LockHeld as e:
        # a data run holds the lock: yield (exit 3 = SuccessExitStatus in the unit, not a
        # failure); the next nightly tick will do the work
        log.warning("%s - yielding to the running data run", e)
        return 3
    print(json.dumps(report, indent=2, default=str))
    errors = [t for t, e in report["tables"].items() if "expire_error" in e or "properties_error" in e]
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
