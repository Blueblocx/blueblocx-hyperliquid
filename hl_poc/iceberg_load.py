"""
Lift the HyperCore CSV outputs into Iceberg tables (nessie.hypercore.*).

Table schemas are PARSED from docs/PROPOSED_SCHEMA.sql so the loader can never
drift from the DDL. Two catalog modes:

  * REST (production): set NESSIE_URI (+ optional NESSIE_BRANCH, WAREHOUSE,
    S3_ENDPOINT / S3_ACCESS_KEY / S3_SECRET_KEY / S3_REGION) — the same variable
    names the Rust services and python-ingestion read.
  * LOCAL (default when NESSIE_URI is unset): a pyiceberg SQLite catalog with a
    file warehouse under out/iceberg/. Lets the whole lift be tested end to end
    on a laptop; the table layout is the same.

Idempotency (pyiceberg has filtered delete/overwrite, which iceberg-rust 0.9
does not):
  * fact tables (l1_wallet_ledger, l3_token_price_usd_1m): INSERT-IF-ABSENT —
    existing natural keys are read for the touched partitions and only new rows
    are appended. Facts are immutable, so this is exactly right and avoids huge
    IN(...) deletes.
  * snapshot / dimension tables (l4x, metadata, gates): REPLACE-BY-KEY — delete
    rows matching the incoming keys (or the (as_of_date, wallet) scope for l4x),
    then append.
  * every load upserts a control.table_watermarks row.

Usage:
    python -m hl_poc.iceberg_load --from out/hypercore            # local catalog
    NESSIE_URI=http://nessie:19120/iceberg python -m hl_poc.iceberg_load --from out/hypercore
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import logging
import os
import re
import uuid
from pathlib import Path

import pyarrow as pa

log = logging.getLogger("hl_poc.iceberg_load")

ROOT = Path(__file__).resolve().parent.parent
DDL_PATH = ROOT / "docs" / "PROPOSED_SCHEMA.sql"
NAMESPACE = "hypercore"
CONTROL_NS = "control"

# table -> (natural key columns, mode, partition-scope columns for replace)
TABLE_PLAN = {
    "l1_wallet_ledger": (["tx_hash", "event_index", "direction"], "insert_if_absent", ["block_date"]),
    "l3_token_price_usd_1m": (["token_address", "bucket_minute"], "insert_if_absent", ["bucket_date"]),
    "l4x_perp_position_snapshot": (["as_of_date", "wallet_address", "coin"], "replace_scope", ["as_of_date", "wallet_address"]),
    "l1_token_metadata": (["token_address"], "replace_keys", []),
    "l4_wallet_metadata": (["wallet_address"], "replace_keys", []),
    "wallet_watchlist": (["wallet_address"], "replace_keys", []),
    "token_whitelist": (["token_address"], "replace_keys", []),
}

_SQL_TO_ARROW = {
    "BIGINT": pa.int64(), "INT": pa.int32(), "DOUBLE": pa.float64(), "STRING": pa.string(),
    "DATE": pa.date32(), "TIMESTAMP": pa.timestamp("us"), "BOOLEAN": pa.bool_(),
}


# --- DDL parsing -----------------------------------------------------------------

def parse_ddl(path: Path = DDL_PATH, namespace: str = NAMESPACE) -> dict[str, dict]:
    """{table: {"columns": [(name, SQLTYPE), ...], "partition": [cols]}} from the DDL."""
    text = path.read_text(encoding="utf-8")
    out: dict[str, dict] = {}
    pat = re.compile(
        rf"CREATE TABLE IF NOT EXISTS nessie\.{namespace}\.(\w+)\s*\((.*?)\)\s*USING iceberg"
        r"(?:\s*PARTITIONED BY \(([^)]*)\))?", re.S)
    for m in pat.finditer(text):
        name, body, part = m.group(1), m.group(2), m.group(3)
        cols = []
        for line in body.splitlines():
            line = line.split("--")[0].strip().rstrip(",")
            if not line:
                continue
            parts = line.split()
            if len(parts) >= 2:
                cols.append((parts[0], parts[1].upper()))
        partition = [p.strip() for p in part.split(",")] if part else []
        out[name] = {"columns": cols, "partition": partition}
    return out


def arrow_schema(cols: list[tuple[str, str]]) -> pa.Schema:
    return pa.schema([pa.field(n, _SQL_TO_ARROW[t], nullable=True) for n, t in cols])


def _coerce(value, sql_type: str):
    if value is None or value == "":
        return None
    if sql_type in ("BIGINT", "INT"):
        return int(float(value))
    if sql_type == "DOUBLE":
        return float(value)
    if sql_type == "BOOLEAN":
        return str(value).strip().lower() in ("true", "1", "t", "yes")
    if sql_type == "DATE":
        return dt.date.fromisoformat(str(value)[:10])
    if sql_type == "TIMESTAMP":
        v = dt.datetime.fromisoformat(str(value))
        if v.tzinfo is not None:
            v = v.astimezone(dt.timezone.utc).replace(tzinfo=None)   # Iceberg TIMESTAMP (no tz), UTC
        return v
    return str(value)


def rows_to_arrow(rows: list[dict], cols: list[tuple[str, str]]) -> pa.Table:
    data = {n: [_coerce(r.get(n), t) for r in rows] for n, t in cols}
    return pa.Table.from_pydict(data, schema=arrow_schema(cols))


def read_csv_rows(path: Path) -> list[dict]:
    with path.open("r", newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


# --- catalog ---------------------------------------------------------------------

def load_catalog_from_env(local_dir: Path | None = None):
    from pyiceberg.catalog import load_catalog
    uri = os.environ.get("NESSIE_URI")
    if uri:
        branch = os.environ.get("NESSIE_BRANCH", "main")
        props = {
            "type": "rest",
            "uri": uri.rstrip("/") + ("" if uri.rstrip("/").endswith(branch) else ""),
            "warehouse": os.environ.get("WAREHOUSE", "s3://warehouse"),
        }
        token = os.environ.get("NESSIE_TOKEN") or os.environ.get("ICEBERG_REST_TOKEN")
        if token:
            props["token"] = token
        ep = os.environ.get("S3_ENDPOINT") or os.environ.get("MINIO_ENDPOINT") or os.environ.get("AWS_ENDPOINT_URL")
        if ep:
            props["s3.endpoint"] = ep
        ak = os.environ.get("S3_ACCESS_KEY") or os.environ.get("AWS_ACCESS_KEY_ID")
        sk = os.environ.get("S3_SECRET_KEY") or os.environ.get("AWS_SECRET_ACCESS_KEY")
        if ak and sk:
            props["s3.access-key-id"] = ak
            props["s3.secret-access-key"] = sk
        region = os.environ.get("S3_REGION") or os.environ.get("AWS_REGION")
        if region:
            props["s3.region"] = region
        log.info("iceberg catalog: REST %s (branch %s)", uri, branch)
        return load_catalog("nessie", **props), "rest"
    local = Path(local_dir or (ROOT / "out" / "iceberg")).resolve()
    local.mkdir(parents=True, exist_ok=True)
    wh = local.as_posix()
    log.info("iceberg catalog: LOCAL sqlite at %s", wh)
    return load_catalog("local", **{"type": "sql", "uri": f"sqlite:///{wh}/catalog.db",
                                   "warehouse": f"file://{wh}/warehouse"}), "local"


def _iceberg_schema(cols: list[tuple[str, str]]):
    from pyiceberg.schema import Schema
    from pyiceberg.types import (BooleanType, DateType, DoubleType, IntegerType, LongType,
                                 NestedField, StringType, TimestampType)
    m = {"BIGINT": LongType(), "INT": IntegerType(), "DOUBLE": DoubleType(), "STRING": StringType(),
         "DATE": DateType(), "TIMESTAMP": TimestampType(), "BOOLEAN": BooleanType()}
    return Schema(*[NestedField(i + 1, n, m[t], required=False) for i, (n, t) in enumerate(cols)])


def _partition_spec(schema, cols: list[str]):
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.transforms import IdentityTransform
    fields = []
    for i, c in enumerate(cols):
        sf = schema.find_field(c)
        fields.append(PartitionField(source_id=sf.field_id, field_id=1000 + i,
                                     transform=IdentityTransform(), name=c))
    return PartitionSpec(*fields)


def ensure_table(catalog, namespace: str, name: str, spec: dict):
    catalog.create_namespace_if_not_exists(namespace)
    ident = f"{namespace}.{name}"
    schema = _iceberg_schema(spec["columns"])
    if spec["partition"]:
        return catalog.create_table_if_not_exists(
            ident, schema=schema, partition_spec=_partition_spec(schema, spec["partition"]))
    return catalog.create_table_if_not_exists(ident, schema=schema)


# --- upsert strategies -------------------------------------------------------------

def _in_filter(col: str, values: list):
    from pyiceberg.expressions import In, AlwaysTrue
    values = [v for v in values if v is not None]
    return In(col, values) if values else AlwaysTrue()


def _and(*exprs):
    from pyiceberg.expressions import And, AlwaysTrue
    exprs = [e for e in exprs if e is not None]
    if not exprs:
        return AlwaysTrue()
    out = exprs[0]
    for e in exprs[1:]:
        out = And(out, e)
    return out


def load_table(table, arrow: pa.Table, key_cols: list[str], mode: str, scope_cols: list[str]) -> dict:
    """Apply one of the idempotent strategies; returns counts."""
    if arrow.num_rows == 0:
        return {"incoming": 0, "written": 0, "skipped_existing": 0}
    if mode == "insert_if_absent":
        # read existing keys only in the touched partitions
        part_col = scope_cols[0]
        touched = sorted(set(arrow.column(part_col).to_pylist()))
        existing = table.scan(row_filter=_in_filter(part_col, touched),
                              selected_fields=tuple(key_cols)).to_arrow()
        have = set(zip(*[existing.column(c).to_pylist() for c in key_cols])) if existing.num_rows else set()
        incoming = list(zip(*[arrow.column(c).to_pylist() for c in key_cols]))
        mask = pa.array([k not in have for k in incoming], pa.bool_())
        new = arrow.filter(mask)
        if new.num_rows:
            table.append(new)
        return {"incoming": arrow.num_rows, "written": new.num_rows,
                "skipped_existing": arrow.num_rows - new.num_rows}
    if mode == "replace_scope":
        scope = sorted(set(zip(*[arrow.column(c).to_pylist() for c in scope_cols])))
        # delete per distinct scope tuple (small: dates x wallets)
        from pyiceberg.expressions import EqualTo
        for tup in scope:
            table.delete(_and(*[EqualTo(c, v) for c, v in zip(scope_cols, tup)]))
        table.append(arrow)
        return {"incoming": arrow.num_rows, "written": arrow.num_rows, "replaced_scopes": len(scope)}
    if mode == "replace_keys":
        for c in key_cols[:1]:  # single-column keys for dimension tables
            vals = arrow.column(c).to_pylist()
            for i in range(0, len(vals), 500):
                table.delete(_in_filter(c, vals[i:i + 500]))
        table.append(arrow)
        return {"incoming": arrow.num_rows, "written": arrow.num_rows}
    raise ValueError(mode)


# --- control.table_watermarks ------------------------------------------------------

_WM_COLS = [("namespace", "STRING"), ("table_name", "STRING"), ("layer", "STRING"), ("producer", "STRING"),
            ("watermark_col", "STRING"), ("watermark_date", "DATE"), ("watermark_ts", "TIMESTAMP"),
            ("row_count", "BIGINT"), ("snapshot_id", "STRING"), ("job_name", "STRING"), ("run_id", "STRING"),
            ("updated_at", "TIMESTAMP"), ("status", "STRING")]


def write_watermark(catalog, namespace: str, table_name: str, watermark_col: str,
                    watermark_ts: dt.datetime, row_count: int, snapshot_id, run_id: str) -> None:
    tbl = ensure_table(catalog, CONTROL_NS, "table_watermarks", {"columns": _WM_COLS, "partition": []})
    row = {
        "namespace": namespace, "table_name": table_name, "layer": table_name.split("_")[0],
        "producer": "hl_poc.iceberg_load", "watermark_col": watermark_col,
        "watermark_date": watermark_ts.date().isoformat(), "watermark_ts": watermark_ts.isoformat(),
        "row_count": row_count, "snapshot_id": str(snapshot_id) if snapshot_id is not None else None,
        "job_name": "hypercore_lift", "run_id": run_id,
        "updated_at": dt.datetime.now(tz=dt.timezone.utc).isoformat(), "status": "completed",
    }
    from pyiceberg.expressions import And, EqualTo
    tbl.delete(And(EqualTo("namespace", namespace), EqualTo("table_name", table_name)))
    tbl.append(rows_to_arrow([row], _WM_COLS))


# --- driver ------------------------------------------------------------------------

_WATERMARK_COL = {"l1_wallet_ledger": "block_timestamp", "l3_token_price_usd_1m": "bucket_minute",
                  "l4x_perp_position_snapshot": "as_of_ts", "l1_token_metadata": "updated_at",
                  "l4_wallet_metadata": "updated_at", "wallet_watchlist": "updated_at",
                  "token_whitelist": "updated_at"}


def lift_directory(src_dir: Path, catalog=None, local_dir: Path | None = None,
                   tables: list[str] | None = None) -> dict:
    """Load every known CSV in src_dir into Iceberg. Returns per-table counts."""
    if catalog is None:
        catalog, _ = load_catalog_from_env(local_dir)
    ddl = parse_ddl()
    run_id = uuid.uuid4().hex[:12]
    report: dict[str, dict] = {}
    for name, (key_cols, mode, scope_cols) in TABLE_PLAN.items():
        if tables and name not in tables:
            continue
        csv_path = src_dir / f"{name}.csv"
        if not csv_path.exists():
            continue
        spec = ddl[name]
        rows = read_csv_rows(csv_path)
        arrow = rows_to_arrow(rows, spec["columns"])
        table = ensure_table(catalog, NAMESPACE, name, spec)
        counts = load_table(table, arrow, key_cols, mode, scope_cols)
        table.refresh()
        total = table.scan().to_arrow().num_rows
        snap = table.current_snapshot()
        wm_col = _WATERMARK_COL[name]
        wm_vals = [v for v in arrow.column(wm_col).to_pylist() if v is not None] if arrow.num_rows else []
        if wm_vals:
            wm = max(wm_vals)
            wm_ts = wm if isinstance(wm, dt.datetime) else dt.datetime.combine(wm, dt.time())
            write_watermark(catalog, NAMESPACE, name, wm_col, wm_ts, total,
                            snap.snapshot_id if snap else None, run_id)
        counts["table_total"] = total
        report[name] = counts
        log.info("lift %-28s incoming=%6d written=%6d total=%7d", name, counts["incoming"],
                 counts["written"], total)
    return report


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    ap = argparse.ArgumentParser(description="Lift HyperCore CSVs into Iceberg")
    ap.add_argument("--from", dest="src", default=str(ROOT / "out" / NAMESPACE))
    ap.add_argument("--local-dir", default=None, help="local catalog dir (default out/iceberg)")
    ap.add_argument("--tables", default=None, help="comma-separated subset")
    args = ap.parse_args()
    from .s3_backfill import load_dotenv
    load_dotenv()
    tables = [t for t in args.tables.split(",") if t] if args.tables else None
    rep = lift_directory(Path(args.src), local_dir=Path(args.local_dir) if args.local_dir else None,
                         tables=tables)
    print(json.dumps(rep, indent=2, default=str))


if __name__ == "__main__":
    main()
