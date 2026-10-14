"""
Lake schema migrations - the only way a production table's SCHEMA changes.

    python -m blueblocx_hyperliquid.migrate                   # dry run: print the plan; exit 0 up to date, 2 pending
    python -m blueblocx_hyperliquid.migrate --apply           # apply what is pending (idempotent)
    python -m blueblocx_hyperliquid.migrate --revert --apply  # undo (run BEFORE moving the tag back)
    python -m blueblocx_hyperliquid.migrate --json

Why a module and not an ALTER by hand: iceberg_load.conform() casts every batch to the
existing table's Arrow schema, and pyarrow casts timestamptz -> string SILENTLY, so a
DDL change that is not mirrored in the lake would quietly write ISO strings into a
string column (the ETL would then see the wrong type, not a failure). The lift
therefore refuses to write a table whose schema drifts from the DDL
(iceberg_load.schema_drift -> SchemaDriftError; preflight reports FAIL) and this
module is what fixes the drift: schema evolution with pyiceberg's update_schema (one
commit), a data backfill in ONE overwrite commit, and a rewrite of the staged CSV so
the next lift parses.

Each migration is idempotent (its plan() is empty once applied), runs under the shared
.daily.lock (exit 3 when held: migrations never run alongside a lift) and documents
its revert. deploy/bootstrap.sh runs `--apply` between the systemd step and preflight;
rolling back a release that carries a migration is `--revert --apply` first, then
bootstrap with the previous tag (docs/DEPLOY.md section 8). Before applying on the
production host, tar HL_OUT_DIR (the staged CSV is rewritten).

Registry (append only; a shipped migration is never edited):
    v1.5.0  l4_wallet_metadata_first_tx_timestamptz
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import sys
from pathlib import Path

from . import config, ops
from . import schema as S

log = logging.getLogger("blueblocx_hyperliquid.migrate")


class Migration:
    id = ""
    release = ""
    description = ""

    def plan(self, catalog, namespace: str, ns_dir: Path) -> dict:
        """What is still to do; `pending` False once applied."""
        raise NotImplementedError

    def apply(self, catalog, namespace: str, ns_dir: Path, plan: dict) -> dict:
        raise NotImplementedError

    def revert(self, catalog, namespace: str, ns_dir: Path) -> dict:
        raise NotImplementedError


def _load_table(catalog, ident: str):
    try:
        return catalog.load_table(ident)
    except Exception:  # noqa: BLE001 - NoSuchTableError and friends: the DDL creates it on the first lift
        return None


def _type_name(field) -> str:
    return type(field.field_type).__name__


def _retype_columns(table, cols: list[str], new_type, doc: str, after: dict[str, str]) -> None:
    """Drop + re-add `cols` with `new_type` in ONE schema commit (readers always see the
    columns; existing rows read NULL because the field ids are new), then restore the DDL
    order in a second, metadata-only commit. The reorder cannot share the first commit:
    pyiceberg's move_after resolves a name against the pre-commit schema, i.e. to the
    field id that is being deleted (verified on 0.12.0)."""
    present = {f.name for f in table.schema().fields}
    with table.update_schema() as upd:
        for col in cols:
            if col in present:
                upd.delete_column(col)
            upd.add_column(col, new_type(), doc=doc.format(col=col))
    table.refresh()
    with table.update_schema() as upd:
        for col in cols:
            upd.move_after(col, after[col])
    table.refresh()


class FirstTxTimestamptz(Migration):
    """v1.5.0: l4_wallet_metadata.first_tx / latest_tx were tx-hash STRINGs; the ETL
    contract wants the first/latest transaction INSTANTS (TIMESTAMPTZ).
      lake  drop + re-add the two columns as timestamptz (new field ids: existing rows
            read NULL) in one schema commit, restore the DDL column order in a second
            metadata-only commit, then ONE overwrite commit backfilling them from
            first/latest_block_timestamp (which hold the same instants).
      csv   staged l4_wallet_metadata.csv: tx-hash values replaced by the block
            timestamps so the next lift parses (rows keep their updated_at).
      revert  the two columns back to STRING in one schema commit (values NULL until the
            next pre-v1.5.0 run); the CSV is left as is (ISO strings are valid STRINGs)."""
    id = "l4_wallet_metadata_first_tx_timestamptz"
    release = "v1.5.0"
    description = "l4_wallet_metadata.first_tx/latest_tx: tx-hash STRING -> TIMESTAMPTZ instant (+ backfill, CSV rewrite)"
    TABLE = "l4_wallet_metadata"
    SOURCE = {"first_tx": "first_block_timestamp", "latest_tx": "latest_block_timestamp"}
    AFTER = {"first_tx": "count_tx", "latest_tx": "first_tx"}       # DDL column order
    TARGET_TYPE = "TimestamptzType"

    def _csv_path(self, ns_dir: Path) -> Path:
        return ns_dir / f"{self.TABLE}.csv"

    def _csv_rows_to_rewrite(self, ns_dir: Path) -> int:
        from .iceberg_load import _parse_iso
        p = self._csv_path(ns_dir)
        if not p.exists():
            return 0
        n = 0
        with p.open("r", newline="", encoding="utf-8") as fh:
            for r in csv.DictReader(fh):
                if any(r.get(c) and _parse_iso(r.get(c)) is None for c in self.SOURCE):
                    n += 1
        return n

    def plan(self, catalog, namespace: str, ns_dir: Path) -> dict:
        from .iceberg_load import _table_row_count
        out: dict = {"id": self.id, "release": self.release, "description": self.description}
        table = _load_table(catalog, f"{namespace}.{self.TABLE}")
        if table is None:
            out["lake"] = {"table": "absent (created from the DDL on the first lift)", "pending": False}
        else:
            fields = {f.name: f for f in table.schema().fields}
            types = {c: (_type_name(fields[c]) if c in fields else "missing") for c in self.SOURCE}
            retype = [c for c, t in types.items() if t != self.TARGET_TYPE]
            out["lake"] = {"types": types, "retype": retype, "rows": _table_row_count(table), "pending": bool(retype)}
        n = self._csv_rows_to_rewrite(ns_dir)
        out["csv"] = {"path": str(self._csv_path(ns_dir)), "rows_to_rewrite": n, "pending": n > 0}
        out["pending"] = bool(out["lake"]["pending"] or out["csv"]["pending"])
        return out

    def apply(self, catalog, namespace: str, ns_dir: Path, plan: dict) -> dict:
        import pyarrow.compute as pc
        from pyiceberg.types import TimestamptzType
        from .iceberg_load import _table_row_count
        result: dict = {"lake": {}, "csv": {}}
        if plan["lake"].get("pending"):
            table = catalog.load_table(f"{namespace}.{self.TABLE}")
            before = table.current_snapshot()
            result["lake"]["snapshot_before"] = before.snapshot_id if before else None
            _retype_columns(table, list(plan["lake"]["retype"]), TimestamptzType,
                            "instant of the wallet's {col} ledger event = the matching *_block_timestamp (v1.5.0)",
                            self.AFTER)
            result["lake"]["retyped"] = list(plan["lake"]["retype"])
            if (_table_row_count(table) or 0) > 0:
                df = table.scan().to_arrow()
                for col, src in self.SOURCE.items():
                    i = df.schema.get_field_index(col)
                    df = df.set_column(i, df.schema.field(col), pc.coalesce(df.column(col), df.column(src)))
                table.overwrite(df)                                   # ONE data commit: every row carries the instants
                table.refresh()
                result["lake"]["backfilled_rows"] = df.num_rows
            else:
                result["lake"]["backfilled_rows"] = 0
            snap = table.current_snapshot()
            result["lake"]["snapshot_after"] = snap.snapshot_id if snap else None
            log.info("migrate %s: lake %s", self.id, result["lake"])
        if plan["csv"].get("pending"):
            result["csv"] = self._rewrite_csv(ns_dir)
            log.info("migrate %s: csv %s", self.id, result["csv"])
        return result

    def _rewrite_csv(self, ns_dir: Path) -> dict:
        from .iceberg_load import _parse_iso
        p = self._csv_path(ns_dir)
        rows: list[dict] = []
        changed = 0
        with p.open("r", newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            columns = list(reader.fieldnames or S.L4_WALLET_METADATA_COLUMNS)
            for r in reader:
                touched = False
                for col, src in self.SOURCE.items():
                    v = r.get(col)
                    if v and _parse_iso(v) is None:
                        r[col] = r.get(src) or ""
                        touched = True
                changed += touched
                rows.append(r)
        tmp = p.with_name(p.name + ".tmp")                          # atomic, like upsert_csv
        with tmp.open("w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=columns, extrasaction="ignore")
            w.writeheader()
            w.writerows(rows)
        os.replace(tmp, p)
        return {"path": str(p), "rows": len(rows), "rewritten": changed}

    def revert(self, catalog, namespace: str, ns_dir: Path) -> dict:
        from pyiceberg.types import StringType
        table = _load_table(catalog, f"{namespace}.{self.TABLE}")
        if table is None:
            return {"lake": {"table": "absent"}, "csv": {"note": "left as is"}}
        fields = {f.name: f for f in table.schema().fields}
        todo = [c for c in self.SOURCE if c in fields and _type_name(fields[c]) != "StringType"]
        if not todo:
            return {"lake": {"already": "STRING"}, "csv": {"note": "left as is"}}
        _retype_columns(table, todo, StringType, "{col}: tx hash (pre-v1.5.0 shape)", self.AFTER)
        return {"lake": {"retyped_to_string": todo, "note": "values NULL until the next pre-v1.5.0 run"},
                "csv": {"note": "left as is: ISO strings are valid STRING values for the old code"}}


MIGRATIONS: list[Migration] = [FirstTxTimestamptz()]


def run(catalog, namespace: str, ns_dir: Path, apply: bool = False, revert: bool = False) -> dict:
    """Plan (default), apply, or revert every registered migration. `pending` in the
    report = migrations still to apply after this call (forward direction)."""
    mode = "revert" if revert else ("apply" if apply else "plan")
    report: dict = {"namespace": namespace, "state_dir": str(ns_dir), "mode": mode, "migrations": []}
    for m in (reversed(MIGRATIONS) if revert else MIGRATIONS):
        entry: dict = {"id": m.id, "release": m.release, "description": m.description}
        if revert:
            entry["revert"] = m.revert(catalog, namespace, ns_dir) if apply else "dry run: add --apply to execute"
        else:
            plan = m.plan(catalog, namespace, ns_dir)
            entry["plan"] = plan
            if plan["pending"] and apply:
                entry["applied"] = m.apply(catalog, namespace, ns_dir, plan)
        entry["pending_after"] = m.plan(catalog, namespace, ns_dir)["pending"]
        report["migrations"].append(entry)
    report["pending"] = sum(1 for e in report["migrations"] if e["pending_after"])
    return report


def _render(report: dict) -> str:
    lines = [f"migrations ({report['mode']}) namespace {report['namespace']}, state {report['state_dir']}"]
    for e in report["migrations"]:
        lines.append(f"  {e['release']:<8} {e['id']}: {e['description']}")
        if "plan" in e:
            p = e["plan"]
            lines.append(f"           lake: {p['lake']}")
            lines.append(f"           csv:  {p['csv']}")
        if "applied" in e:
            lines.append(f"           applied: {e['applied']}")
        if "revert" in e:
            lines.append(f"           revert: {e['revert']}")
        lines.append(f"           {'PENDING' if e['pending_after'] else 'up to date'}")
    return "\n".join(lines)


def main(argv=None) -> int:
    """Exit codes: 0 nothing pending (or applied / reverted), 1 failed, 2 pending (dry run),
    3 another run holds the state dir's lock."""
    config.load_env()
    config.setup_logging()
    ap = argparse.ArgumentParser(description="Plan / apply / revert lake schema migrations")
    ap.add_argument("--namespace", default=S.NS_HYPERCORE)
    ap.add_argument("--out", default=str(config.out_root()), help="state dir (staged CSVs, run lock)")
    ap.add_argument("--apply", action="store_true", help="execute (default: dry run)")
    ap.add_argument("--revert", action="store_true", help="undo the migrations (with --apply)")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args(argv)
    from .iceberg_load import dispose_catalog, load_catalog_from_env
    ns_dir = Path(args.out) / args.namespace
    try:
        with ops.RunLock(ns_dir / ".daily.lock"):
            catalog, _ = load_catalog_from_env(Path(args.out) / "iceberg")
            try:
                report = run(catalog, args.namespace, ns_dir, apply=args.apply, revert=args.revert)
            finally:
                dispose_catalog(catalog)
    except ops.LockHeld as e:
        log.error("%s - migrations never run alongside a lift; retry when it has finished", e)
        return 3
    except Exception as e:  # noqa: BLE001
        log.error("migrate failed: %s: %s", type(e).__name__, e)
        return 1
    print(json.dumps(report, indent=2, default=str) if args.json else _render(report))
    if args.revert:
        return 0
    if args.apply:
        return 0 if report["pending"] == 0 else 1
    return 2 if report["pending"] else 0


if __name__ == "__main__":
    sys.exit(main())
