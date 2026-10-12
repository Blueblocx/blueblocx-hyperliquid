"""
Lift the HyperCore (or HyperEVM) CSV outputs into Iceberg tables.

Table SHAPES are parsed from docs/PROPOSED_SCHEMA.sql (the nessie.hypercore.*
blocks) so the loader cannot drift from the DDL; the target namespace is a
parameter (hypercore by default, hyperevm shares the same shapes). Two catalog
modes:

  * REST (shared atlas lake): NESSIE_URI (+ NESSIE_BRANCH -> pyiceberg `prefix`,
    WAREHOUSE, S3_ENDPOINT / S3_ACCESS_KEY / S3_SECRET_KEY / S3_REGION) — the
    same variable names the Rust services and python-ingestion read.
  * SQL (standalone host, no object store): HL_CATALOG_URI = a SQLAlchemy URL
    (PostgreSQL in production; SQLite is single-writer) holding the catalog rows,
    WAREHOUSE = where the data lives — file:///mnt/nas/hypercore/warehouse on an
    NFS mount, or an s3:// bucket with the S3_* variables.
  * LOCAL (default when neither is set): a pyiceberg SQLite catalog with a file
    warehouse under HL_OUT_DIR/iceberg/, so the whole lift is testable end to end.

Type policy (matches production): data-table TIMESTAMP columns are Iceberg
`timestamptz` (Spark TIMESTAMP is TIMESTAMP_LTZ; python-ingestion writes
pa.timestamp('us', tz='UTC') into nessie.ethereum.l1_wallet_ledger; the Rust
readers require Timestamptz). control.table_watermarks keeps python-ingestion's
exact schema: naive timestamps and REQUIRED namespace/table_name/watermark_col/
updated_at/status. When a table already exists the Arrow batch is cast to the
table's own Arrow schema, so the loader follows whatever DDL produced it and
fails fast on an incompatible type instead of writing garbage.

Idempotency (pyiceberg has filtered delete/overwrite, which iceberg-rust 0.9
does not):
  * fact tables (l1_wallet_ledger, l3_token_price_usd_1m): INSERT-IF-ABSENT —
    existing natural keys in the touched partitions are read and only new rows
    appended (facts are immutable; l3 only receives CLOSED candles).
  * snapshot / dimension tables: REPLACE — delete the incoming keys (or the
    (as_of_date, wallet) scope for l4x), then append. Null keys are rejected and
    an empty key list never becomes a DELETE predicate.
  * watermarks are written in a FINAL step, monotonic, and never abort data lifts.

Usage:
    python -m hl_poc.iceberg_load --from out/hypercore
    python -m hl_poc.iceberg_load --from out/hyperevm --namespace hyperevm
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

from . import config
from .ops import atomic_write_text

log = logging.getLogger("hl_poc.iceberg_load")

ROOT = Path(__file__).resolve().parent.parent
DDL_PATH = ROOT / "docs" / "PROPOSED_SCHEMA.sql"      # overridable with HL_DDL_PATH
DDL_NAMESPACE = "hypercore"          # the namespace whose DDL blocks define the shapes
CONTROL_NS = "control"
LIFT_STATE_FILE = "_lift_state.json"  # last successful lift per staging dir (bounded incremental lifts)
DEFAULT_PY_IO_IMPL = "pyiceberg.io.pyarrow.PyArrowFileIO"

# table -> (natural key columns, mode, partition/scope columns)
TABLE_PLAN = {
    "l1_wallet_ledger": (["tx_hash", "event_index", "direction"], "insert_if_absent", ["block_date"]),
    "l3_token_price_usd_1m": (["token_address", "bucket_minute"], "insert_if_absent", ["bucket_date"]),
    "l4x_perp_position_snapshot": (["as_of_date", "wallet_address", "coin"], "replace_scope", ["as_of_date", "wallet_address"]),
    "l1_token_metadata": (["token_address"], "replace_keys", []),
    "l4_wallet_metadata": (["wallet_address"], "replace_keys", []),
    "wallet_watchlist": (["wallet_address"], "replace_keys", []),
    "token_whitelist": (["token_address"], "replace_keys", []),
}

_WATERMARK_COL = {"l1_wallet_ledger": "block_timestamp", "l3_token_price_usd_1m": "bucket_minute",
                  "l4x_perp_position_snapshot": "as_of_ts", "l1_token_metadata": "updated_at",
                  "l4_wallet_metadata": "updated_at", "wallet_watchlist": "updated_at",
                  "token_whitelist": "updated_at"}

# control.table_watermarks — python-ingestion/src/table_watermarks.py shape (5 required)
_WM_COLS = [("namespace", "STRING", True), ("table_name", "STRING", True), ("layer", "STRING", False),
            ("producer", "STRING", False), ("watermark_col", "STRING", True), ("watermark_date", "DATE", False),
            ("watermark_ts", "TIMESTAMP", False), ("row_count", "BIGINT", False), ("snapshot_id", "STRING", False),
            ("job_name", "STRING", False), ("run_id", "STRING", False), ("updated_at", "TIMESTAMP", True),
            ("status", "STRING", True)]


def _arrow_type(sql_type: str, tz: bool) -> pa.DataType:
    return {
        "BIGINT": pa.int64(), "INT": pa.int32(), "DOUBLE": pa.float64(), "STRING": pa.string(),
        "DATE": pa.date32(), "BOOLEAN": pa.bool_(),
        "TIMESTAMP": pa.timestamp("us", tz="UTC") if tz else pa.timestamp("us"),
    }[sql_type]


# --- DDL parsing -----------------------------------------------------------------

def parse_ddl(path: Path | None = None, namespace: str = DDL_NAMESPACE) -> dict[str, dict]:
    """{table: {"columns": [(name, SQLTYPE), ...], "partition": [cols]}} from the DDL
    (docs/PROPOSED_SCHEMA.sql by default, HL_DDL_PATH when set)."""
    path = Path(path) if path else config.ddl_path()
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


def arrow_schema(cols: list[tuple], tz: bool = True) -> pa.Schema:
    fields = []
    for c in cols:
        name, sql_type = c[0], c[1]
        required = c[2] if len(c) > 2 else False
        fields.append(pa.field(name, _arrow_type(sql_type, tz), nullable=not required))
    return pa.schema(fields)


def _coerce(value, sql_type: str, tz: bool):
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
        v = value if isinstance(value, dt.datetime) else dt.datetime.fromisoformat(str(value))
        if v.tzinfo is None:
            v = v.replace(tzinfo=dt.timezone.utc)
        v = v.astimezone(dt.timezone.utc)
        return v if tz else v.replace(tzinfo=None)
    return str(value)


def rows_to_arrow(rows: list[dict], cols: list[tuple], tz: bool = True) -> pa.Table:
    data = {c[0]: [_coerce(r.get(c[0]), c[1], tz) for r in rows] for c in cols}
    return pa.Table.from_pydict(data, schema=arrow_schema(cols, tz))


def _parse_iso(value) -> dt.datetime | None:
    if not value:
        return None
    try:
        v = dt.datetime.fromisoformat(str(value))
    except ValueError:
        return None
    return v if v.tzinfo else v.replace(tzinfo=dt.timezone.utc)


def read_csv_rows(path: Path, since_iso: str | None = None, fallback_ts_col: str | None = None) -> list[dict]:
    """Rows of a staged CSV. With `since_iso`, only rows whose timestamp is later —
    `updated_at` when the table has it (what the extractor wrote after the previous
    successful lift), else `fallback_ts_col` (e.g. as_of_ts for the l4x snapshot,
    whose rows carry no updated_at). The filter is applied while streaming, so
    memory is bounded by the NEW rows, not by the file (a market-maker wallet
    stages ~180k rows/day; 45 days of that is gigabytes as Python dicts). Rows
    without a parseable timestamp are kept, never dropped."""
    since = _parse_iso(since_iso)
    out: list[dict] = []
    with path.open("r", newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        header = reader.fieldnames or []
        ts_col = "updated_at" if "updated_at" in header else (fallback_ts_col if fallback_ts_col in header else None)
        for r in reader:
            if since is not None and ts_col is not None:
                ts = _parse_iso(r.get(ts_col))
                if ts is not None and ts <= since:
                    continue
            out.append(r)
    return out


def since_with_margin(last_success_iso: str | None, hours: float = 1.0) -> str | None:
    """Lift cut-off = previous successful run start minus a safety margin (clock
    skew, a manual run that finished during the previous lift). Re-lifted rows are
    skipped by insert-if-absent, so the margin costs little."""
    t = _parse_iso(last_success_iso)
    if t is None:
        return None
    return (t - dt.timedelta(hours=hours)).isoformat()


def read_lift_state(src_dir: Path) -> dict:
    p = Path(src_dir) / LIFT_STATE_FILE
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001 — a corrupt marker just means a full lift
        return {}


def write_lift_state(src_dir: Path, run_iso: str, report: dict | None = None) -> None:
    """Record a SUCCESSFUL lift (lift_directory raised otherwise). The next
    incremental lift only reads rows updated after this run started."""
    state = {"last_success_run_iso": run_iso,
             "written_at": dt.datetime.now(tz=dt.timezone.utc).isoformat(),
             "tables": {k: v.get("written") for k, v in (report or {}).items()
                        if isinstance(v, dict) and "written" in v}}
    atomic_write_text(Path(src_dir) / LIFT_STATE_FILE, json.dumps(state, indent=2))


# --- catalog ---------------------------------------------------------------------

def _rest_props(uri: str) -> dict:
    """pyiceberg REST-catalog properties from the environment (the same variable
    names the atlas Rust services read). Two defaults are deliberate:

      * py-io-impl = PyArrowFileIO. Nessie's /v1/config DEFAULTS advertise
        pyiceberg.io.fsspec.FsspecFileIO, which needs the s3fs package this project
        does not install; client properties beat server defaults, and pyarrow is a
        dependency anyway. HL_PY_IO_IMPL overrides.
      * s3.region = us-east-1 when unset (like the Rust crates): PyArrowFileIO
        otherwise asks AWS for the bucket's region, which fails against MinIO/
        SeaweedFS endpoints.

    Nessie's OVERRIDES — notably s3.endpoint = its configured external-endpoint —
    beat anything set here, so the endpoint the catalog advertises must be
    reachable from THIS host; `hl_poc.preflight --iceberg` checks exactly that.
    """
    branch = os.environ.get("NESSIE_BRANCH") or "main"
    props = {"type": "rest", "uri": uri.rstrip("/"), "prefix": branch,
             "warehouse": os.environ.get("WAREHOUSE") or "s3://warehouse",
             "py-io-impl": os.environ.get("HL_PY_IO_IMPL") or DEFAULT_PY_IO_IMPL}
    token = os.environ.get("NESSIE_TOKEN") or os.environ.get("ICEBERG_REST_TOKEN")
    if token:
        props["token"] = token
    props.update(_s3_props())
    return props


def _s3_props() -> dict:
    """s3.* FileIO properties from the environment (shared by the REST and SQL modes
    whenever the warehouse is an s3:// location)."""
    props: dict = {}
    ep = os.environ.get("S3_ENDPOINT") or os.environ.get("MINIO_ENDPOINT") or os.environ.get("AWS_ENDPOINT_URL")
    if ep:
        props["s3.endpoint"] = ep
    ak = os.environ.get("S3_ACCESS_KEY") or os.environ.get("AWS_ACCESS_KEY_ID")
    sk = os.environ.get("S3_SECRET_KEY") or os.environ.get("AWS_SECRET_ACCESS_KEY")
    if ak and sk:
        props["s3.access-key-id"] = ak
        props["s3.secret-access-key"] = sk
    props["s3.region"] = os.environ.get("S3_REGION") or os.environ.get("AWS_REGION") or "us-east-1"
    return props


def _redacted_uri(uri: str) -> str:
    """Catalog URI safe for logs (password hidden)."""
    try:
        from sqlalchemy.engine import make_url
        return make_url(uri).render_as_string(hide_password=True)
    except Exception:  # noqa: BLE001
        return uri.split("@")[-1] if "@" in uri else uri


def catalog_mode() -> str:
    """Which catalog the environment selects: 'rest' (NESSIE_URI), 'sql'
    (HL_CATALOG_URI: PostgreSQL or SQLite via SQLAlchemy) or 'local' (the dev
    SQLite catalog under HL_OUT_DIR/iceberg)."""
    if os.environ.get("NESSIE_URI"):
        return "rest"
    if os.environ.get("HL_CATALOG_URI"):
        return "sql"
    return "local"


def _sql_props(sql_uri: str, local_dir: Path | None = None) -> dict:
    """pyiceberg SQL-catalog properties: the catalog rows live in the database
    named by HL_CATALOG_URI (PostgreSQL in production — SQLite is single-writer,
    development only); the DATA lives wherever WAREHOUSE points. A file://
    warehouse (e.g. an NFS mount) needs no object store at all; an s3://
    warehouse takes the usual S3_* variables."""
    warehouse = os.environ.get("WAREHOUSE") or ""
    if not warehouse:
        local = Path(local_dir or (config.out_root() / "iceberg")).resolve()
        local.mkdir(parents=True, exist_ok=True)
        warehouse = f"file://{local.as_posix()}/warehouse"
    props = {"type": "sql", "uri": sql_uri, "warehouse": warehouse,
             "py-io-impl": os.environ.get("HL_PY_IO_IMPL") or DEFAULT_PY_IO_IMPL}
    if warehouse.startswith(("s3://", "s3a://", "s3n://")):
        props.update(_s3_props())
    return props


def warehouse_local_path(warehouse: str) -> Path | None:
    """Filesystem path of a file:// warehouse, else None. Accepts file:///abs/path,
    file://localhost/abs/path and the Windows forms file://D:/x and file:///D:/x
    (the dev catalog has always written file://{drive}:/...)."""
    if not warehouse.startswith("file://"):
        return None
    from urllib.parse import unquote
    rest = unquote(warehouse[len("file://"):])
    if rest.startswith("localhost/"):
        rest = rest[len("localhost"):]
    if len(rest) >= 3 and rest[0] == "/" and rest[2] == ":":
        rest = rest[1:]                                # file:///D:/x -> D:/x
    is_posix_abs = rest.startswith("/")
    is_windows_abs = len(rest) >= 2 and rest[1] == ":" and rest[0].isalpha()
    if not (is_posix_abs or is_windows_abs):
        return None                                    # file://host/share is not local
    return Path(rest)


def load_catalog_from_env(local_dir: Path | None = None):
    from pyiceberg.catalog import load_catalog
    mode = catalog_mode()
    if mode == "rest":
        uri = os.environ["NESSIE_URI"]
        props = _rest_props(uri)
        log.info("iceberg catalog: REST %s (prefix/branch %s, io %s)", uri, props["prefix"],
                 props["py-io-impl"].rsplit(".", 1)[-1])
        return load_catalog("nessie", **props), "rest"
    if mode == "sql":
        props = _sql_props(os.environ["HL_CATALOG_URI"], local_dir)
        wh_path = warehouse_local_path(props["warehouse"])
        if wh_path is not None:
            wh_path.mkdir(parents=True, exist_ok=True)
        log.info("iceberg catalog: SQL %s, warehouse %s", _redacted_uri(props["uri"]), props["warehouse"])
        return load_catalog("hl", **props), "sql"
    local = Path(local_dir or (config.out_root() / "iceberg")).resolve()
    local.mkdir(parents=True, exist_ok=True)
    wh = local.as_posix()
    warehouse = os.environ.get("WAREHOUSE") or ""
    if not warehouse.startswith("file://"):
        if warehouse:
            log.warning("WAREHOUSE=%s ignored: without NESSIE_URI or HL_CATALOG_URI the dev catalog "
                        "only writes a file:// warehouse", warehouse)
        warehouse = f"file://{wh}/warehouse"
    else:
        p = warehouse_local_path(warehouse)
        if p is not None:
            p.mkdir(parents=True, exist_ok=True)
    log.info("iceberg catalog: LOCAL sqlite at %s, warehouse %s", wh, warehouse)
    return load_catalog("local", **{"type": "sql", "uri": f"sqlite:///{wh}/catalog.db",
                                   "warehouse": warehouse,
                                   "py-io-impl": os.environ.get("HL_PY_IO_IMPL") or DEFAULT_PY_IO_IMPL}), "local"


def _iceberg_schema(cols: list[tuple], tz: bool = True):
    from pyiceberg.schema import Schema
    from pyiceberg.types import (BooleanType, DateType, DoubleType, IntegerType, LongType,
                                 NestedField, StringType, TimestampType, TimestamptzType)
    m = {"BIGINT": LongType(), "INT": IntegerType(), "DOUBLE": DoubleType(), "STRING": StringType(),
         "DATE": DateType(), "BOOLEAN": BooleanType(),
         "TIMESTAMP": TimestamptzType() if tz else TimestampType()}
    fields = []
    for i, c in enumerate(cols):
        required = c[2] if len(c) > 2 else False
        fields.append(NestedField(i + 1, c[0], m[c[1]], required=required))
    return Schema(*fields)


def _partition_spec(schema, cols: list[str]):
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.transforms import IdentityTransform
    fields = []
    for i, c in enumerate(cols):
        sf = schema.find_field(c)
        fields.append(PartitionField(source_id=sf.field_id, field_id=1000 + i,
                                     transform=IdentityTransform(), name=c))
    return PartitionSpec(*fields)


def ensure_table(catalog, namespace: str, name: str, spec: dict, tz: bool = True):
    catalog.create_namespace_if_not_exists(namespace)
    ident = f"{namespace}.{name}"
    schema = _iceberg_schema(spec["columns"], tz)
    if spec.get("partition"):
        return catalog.create_table_if_not_exists(
            ident, schema=schema, partition_spec=_partition_spec(schema, spec["partition"]))
    return catalog.create_table_if_not_exists(ident, schema=schema)


def conform(arrow: pa.Table, table) -> pa.Table:
    """Cast the batch to the EXISTING table's Arrow schema (column order, types,
    nullability). Raises with a clear message on incompatible types so we never
    write a type the production DDL does not have."""
    target = table.schema().as_arrow()
    missing = [f.name for f in target if f.name not in arrow.column_names]
    extra = [n for n in arrow.column_names if n not in target.names]
    if extra:
        raise ValueError(f"{table.name()}: batch has columns not in the table: {extra}")
    cols = []
    for f in target:
        if f.name in missing:
            cols.append(pa.nulls(arrow.num_rows, type=f.type))
        else:
            cols.append(arrow.column(f.name).cast(f.type))
    return pa.Table.from_arrays(cols, schema=target)


# --- upsert strategies -------------------------------------------------------------

def _in_filter(col: str, values: list):
    """In(col, values) for a NON-EMPTY list of non-null values; None if empty.
    Natural keys must be non-null — a None key raises (never silently widens)."""
    from pyiceberg.expressions import In
    if any(v is None for v in values):
        raise ValueError(f"null natural key in column {col}")
    return In(col, list(values)) if values else None


def _and(*exprs):
    from pyiceberg.expressions import And
    exprs = [e for e in exprs if e is not None]
    if not exprs:
        raise ValueError("refusing to build an empty predicate")
    out = exprs[0]
    for e in exprs[1:]:
        out = And(out, e)
    return out


def load_table(table, arrow: pa.Table, key_cols: list[str], mode: str, scope_cols: list[str]) -> dict:
    """Apply one of the idempotent strategies; returns counts."""
    if arrow.num_rows == 0:
        return {"incoming": 0, "written": 0, "skipped_existing": 0}
    arrow = conform(arrow, table)
    for c in key_cols:
        if arrow.column(c).null_count:
            raise ValueError(f"{table.name()}: null natural key in {c}")
    if mode == "insert_if_absent":
        part_col = scope_cols[0]
        touched = sorted(set(arrow.column(part_col).to_pylist()))
        flt = _in_filter(part_col, touched)
        existing = table.scan(row_filter=flt, selected_fields=tuple(key_cols)).to_arrow()
        have = set(zip(*[existing.column(c).to_pylist() for c in key_cols])) if existing.num_rows else set()
        incoming = list(zip(*[arrow.column(c).to_pylist() for c in key_cols]))
        mask = pa.array([k not in have for k in incoming], pa.bool_())
        new = arrow.filter(mask)
        if new.num_rows:
            table.append(new)
        return {"incoming": arrow.num_rows, "written": new.num_rows,
                "skipped_existing": arrow.num_rows - new.num_rows}
    if mode == "replace_scope":
        from pyiceberg.expressions import EqualTo
        scope = sorted(set(zip(*[arrow.column(c).to_pylist() for c in scope_cols])))
        for tup in scope:
            table.delete(_and(*[EqualTo(c, v) for c, v in zip(scope_cols, tup)]))
        table.append(arrow)
        return {"incoming": arrow.num_rows, "written": arrow.num_rows, "replaced_scopes": len(scope)}
    if mode == "replace_keys":
        c = key_cols[0]  # single-column keys for dimension tables
        vals = arrow.column(c).to_pylist()
        for i in range(0, len(vals), 500):
            flt = _in_filter(c, vals[i:i + 500])
            if flt is not None:
                table.delete(flt)
        table.append(arrow)
        return {"incoming": arrow.num_rows, "written": arrow.num_rows}
    raise ValueError(mode)


# --- control.table_watermarks ------------------------------------------------------

def write_watermark(catalog, namespace: str, table_name: str, watermark_col: str,
                    watermark_ts: dt.datetime, row_count: int, snapshot_id, run_id: str) -> bool:
    """Upsert one watermark row (python-ingestion schema, naive timestamps).
    Monotonic: an older watermark never replaces a newer one. Returns written?"""
    from pyiceberg.expressions import And, EqualTo
    tbl = ensure_table(catalog, CONTROL_NS, "table_watermarks", {"columns": _WM_COLS}, tz=False)
    wm_naive = watermark_ts.astimezone(dt.timezone.utc).replace(tzinfo=None) \
        if watermark_ts.tzinfo else watermark_ts
    pred = And(EqualTo("namespace", namespace), EqualTo("table_name", table_name))
    existing = tbl.scan(row_filter=pred, selected_fields=("watermark_ts",)).to_arrow().to_pylist()
    prev = max((r["watermark_ts"] for r in existing if r.get("watermark_ts")), default=None)
    if prev is not None and prev > wm_naive:
        return False
    row = {
        "namespace": namespace, "table_name": table_name, "layer": table_name.split("_")[0],
        "producer": "hl_poc.iceberg_load", "watermark_col": watermark_col,
        "watermark_date": wm_naive.date(), "watermark_ts": wm_naive,
        "row_count": row_count, "snapshot_id": str(snapshot_id) if snapshot_id is not None else None,
        "job_name": f"{namespace}_lift", "run_id": run_id,
        "updated_at": dt.datetime.now(tz=dt.timezone.utc).replace(tzinfo=None), "status": "completed",
    }
    batch = conform(rows_to_arrow([row], _WM_COLS, tz=False), tbl)
    tbl.delete(pred)
    tbl.append(batch)
    return True


# --- driver ------------------------------------------------------------------------

def _table_row_count(table) -> int | None:
    """Row count from METADATA only: the snapshot summary's total-records, else the
    sum of data-file record counts from the manifests. Never reads data — the old
    `table.scan().to_arrow().num_rows` pulled every table in full over the network
    on every daily run just to report a number."""
    snap = table.current_snapshot()
    if snap is None:
        return 0
    summary = getattr(snap, "summary", None)
    if summary is not None:
        try:
            v = summary.get("total-records") if hasattr(summary, "get") else summary["total-records"]
            if v is not None:
                return int(v)
        except (KeyError, TypeError, ValueError):
            pass
    try:
        return sum(int(t.file.record_count) for t in table.scan().plan_files())
    except Exception:  # noqa: BLE001 — a count is reporting, never worth failing a lift
        return None


def lift_directory(src_dir: Path, catalog=None, local_dir: Path | None = None,
                   tables: list[str] | None = None, namespace: str = DDL_NAMESPACE,
                   since_iso: str | None = None) -> dict:
    """Load every known CSV in src_dir into Iceberg `namespace`. Returns a report.
    Data lifts first; watermarks are a final, non-fatal step. With `since_iso` only
    rows updated after that instant are lifted (bounded incremental lift)."""
    if catalog is None:
        catalog, _ = load_catalog_from_env(local_dir)
    ddl = parse_ddl()
    run_id = uuid.uuid4().hex[:12]
    report: dict[str, dict] = {"_since": since_iso}
    pending_wm: list[tuple] = []
    for name, (key_cols, mode, scope_cols) in TABLE_PLAN.items():
        if tables and name not in tables:
            continue
        csv_path = src_dir / f"{name}.csv"
        if not csv_path.exists():
            continue
        spec = ddl[name]
        rows = read_csv_rows(csv_path, since_iso, fallback_ts_col=_WATERMARK_COL[name])
        arrow = rows_to_arrow(rows, spec["columns"], tz=True)
        table = ensure_table(catalog, namespace, name, spec, tz=True)
        counts = load_table(table, arrow, key_cols, mode, scope_cols)
        table.refresh()
        total = _table_row_count(table)
        snap = table.current_snapshot()
        wm_col = _WATERMARK_COL[name]
        wm_vals = [v for v in arrow.column(wm_col).to_pylist() if v is not None] if arrow.num_rows else []
        if wm_vals:
            wm = max(wm_vals)
            wm_ts = wm if isinstance(wm, dt.datetime) else dt.datetime.combine(wm, dt.time(), dt.timezone.utc)
            pending_wm.append((name, wm_col, wm_ts, total, snap.snapshot_id if snap else None))
        counts["table_total"] = total
        report[name] = counts
        log.info("lift %-28s incoming=%6d written=%6d total=%7s", name, counts["incoming"],
                 counts["written"], "?" if total is None else total)
    wm_status = {}
    for name, wm_col, wm_ts, total, snap_id in pending_wm:
        try:
            wm_status[name] = "written" if write_watermark(
                catalog, namespace, name, wm_col, wm_ts, total, snap_id, run_id) else "kept_newer"
        except Exception as e:  # noqa: BLE001 — never abort data lifts over a watermark row
            wm_status[name] = f"failed: {type(e).__name__}: {str(e)[:120]}"
            log.warning("watermark write failed for %s.%s: %s", namespace, name, e)
    report["_watermarks"] = wm_status
    return report


def main() -> None:
    config.load_env()
    config.setup_logging()
    ap = argparse.ArgumentParser(description="Lift HyperCore/HyperEVM CSVs into Iceberg")
    ap.add_argument("--from", dest="src", default=str(config.out_root() / DDL_NAMESPACE))
    ap.add_argument("--namespace", default=DDL_NAMESPACE, help="target namespace (hypercore | hyperevm)")
    ap.add_argument("--local-dir", default=None, help="local catalog dir (default HL_OUT_DIR/iceberg)")
    ap.add_argument("--tables", default=None, help="comma-separated subset")
    ap.add_argument("--since", default=None,
                    help="lift only rows updated after this ISO instant (default: the last successful "
                         f"lift recorded in {LIFT_STATE_FILE} minus 1h)")
    ap.add_argument("--full", action="store_true", help="ignore the lift state: lift every staged row")
    args = ap.parse_args()
    tables = [t for t in args.tables.split(",") if t] if args.tables else None
    src = Path(args.src)
    run_iso = dt.datetime.now(tz=dt.timezone.utc).isoformat()
    since = None
    if not args.full:
        since = args.since or since_with_margin(read_lift_state(src).get("last_success_run_iso"))
    rep = lift_directory(src, local_dir=Path(args.local_dir) if args.local_dir else None,
                         tables=tables, namespace=args.namespace, since_iso=since)
    if not tables:                    # a partial (subset) lift must not advance the marker
        write_lift_state(src, run_iso, rep)
    print(json.dumps(rep, indent=2, default=str))


if __name__ == "__main__":
    main()
