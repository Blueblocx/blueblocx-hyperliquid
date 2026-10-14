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
    (as_of_date, wallet) scope for l4x) and append the batch inside ONE Iceberg
    transaction, i.e. one catalog commit: a reader never observes the keys
    missing, and a crash leaves the previous snapshot intact. Null keys are
    rejected and an empty key list never becomes a DELETE predicate.
  * lift order = TABLE_PLAN order: FK parents (token metadata, gates) first, then
    facts, then derived tables, so a lift that fails part-way never leaves ledger
    rows whose token has no metadata row.
  * watermarks are written in a FINAL step (one commit for all tables), monotonic,
    and never abort data lifts; a table whose lift raised gets a 'failed' row.

Readers (the ETL's DuckDB, see docs/DEPLOY.md "Serving the lake to DuckDB readers")
resolve metadata_location from the catalog's iceberg_tables and iceberg_scan() it;
every lift advances a table's pointer at most once (plus the watermark table).

Usage:
    python -m blueblocx_hyperliquid.iceberg_load --from out/hypercore
    python -m blueblocx_hyperliquid.iceberg_load --from out/hyperevm --namespace hyperevm
    NESSIE_URI=http://nessie:19120/iceberg python -m blueblocx_hyperliquid.iceberg_load --from out/hypercore
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import logging
import os
import re
import sys
import uuid
import warnings
from pathlib import Path

import pyarrow as pa

from . import config
# re-exported under their historical names: preflight/maintenance/tests import them from here
from .config import catalog_mode, redacted_uri as _redacted_uri, warehouse_local_path  # noqa: F401
from .ops import LockHeld, RunLock, atomic_write_text

log = logging.getLogger("blueblocx_hyperliquid.iceberg_load")

ROOT = Path(__file__).resolve().parent.parent
DDL_PATH = ROOT / "docs" / "PROPOSED_SCHEMA.sql"      # overridable with HL_DDL_PATH
DDL_NAMESPACE = "hypercore"          # the namespace whose DDL blocks define the shapes
CONTROL_NS = "control"
LIFT_STATE_FILE = "_lift_state.json"  # last successful lift per staging dir (bounded incremental lifts)
DEFAULT_PY_IO_IMPL = "pyiceberg.io.pyarrow.PyArrowFileIO"

# table -> (natural key columns, mode, partition/scope columns)
# Dict order IS the lift order (lift_directory iterates it): FK parents first
# (l1_token_metadata is the parent of every token_address; the two gates next),
# then the facts that reference them, then tables derived from the facts. A lift
# that fails part-way therefore never leaves child rows without their parent row
# (ETL contract 4.3: referential closure at export time).
TABLE_PLAN = {
    "l1_token_metadata": (["token_address"], "replace_keys", []),
    "token_whitelist": (["token_address"], "replace_keys", []),
    "wallet_watchlist": (["wallet_address"], "replace_keys", []),
    "l1_wallet_ledger": (["tx_hash", "event_index", "direction"], "insert_if_absent", ["block_date"]),
    "l3_token_price_usd_1m": (["token_address", "bucket_minute"], "insert_if_absent", ["bucket_date"]),
    "l4_wallet_metadata": (["wallet_address"], "replace_keys", []),
    "l4x_perp_position_snapshot": (["as_of_date", "wallet_address", "coin"], "replace_scope", ["as_of_date", "wallet_address"]),
}

# Tables that production is not expected to populate: l3_token_price_usd_1m is the
# development-only output of `--with-prices` (the production unit never passes it),
# so its absence from the catalog is normal, not a missing-table warning.
OPTIONAL_TABLES = frozenset({"l3_token_price_usd_1m"})

# Per table: the per-row WRITE stamp (what the ETL's Tier-A cursor reads: `updated_at`,
# except l4x which has none), the business-date column (for watermark_date) and the
# column the bounded lift falls back to when a CSV has no updated_at.
_STAMP_COL = {name: "updated_at" for name in TABLE_PLAN}
_STAMP_COL["l4x_perp_position_snapshot"] = "created_at"
_BUSINESS_DATE_COL = {"l1_wallet_ledger": "block_date", "l3_token_price_usd_1m": "bucket_date",
                      "l4x_perp_position_snapshot": "as_of_date"}
_SINCE_FALLBACK_COL = {"l4x_perp_position_snapshot": "as_of_ts"}

# Columns that are NOT NULL by construction (natural keys, business dates, stamps,
# the gates' is_active). The Iceberg schema of the tables created before v1.4.0
# declares every column optional (Iceberg cannot evolve optional -> required), so the
# guarantee is enforced at lift time: load_table refuses a batch with a NULL in one of
# these BEFORE anything is committed. Tables created from v1.4.0 on carry required=True.
REQUIRED_COLS = {
    "l1_wallet_ledger": ["tx_hash", "event_index", "direction", "block_timestamp", "block_date", "event_type",
                         "wallet_address", "token_address", "created_at", "updated_at"],
    "l3_token_price_usd_1m": ["token_address", "bucket_minute", "bucket_date", "created_at", "updated_at"],
    "l4x_perp_position_snapshot": ["as_of_ts", "as_of_date", "wallet_address", "coin"],
    "l1_token_metadata": ["token_address", "standard", "is_stablecoin", "created_at", "updated_at"],
    "l4_wallet_metadata": ["wallet_address", "updated_at"],
    "wallet_watchlist": ["wallet_address", "is_active", "created_at", "updated_at"],
    "token_whitelist": ["token_address", "is_active", "created_at", "updated_at"],
}

# control.table_watermarks status vocabulary. 'completed' is python-ingestion's value
# (kept so the shared atlas table stays consistent); the ETL contract proposes
# ok/running/failed/restated - the constant makes a vocabulary change a one-line edit.
WM_STATUS_OK = "completed"
WM_STATUS_FAILED = "failed"          # the last lift of that table raised; previous watermark kept
WM_STATUS_RESTATED = "restated"      # an operator re-lifted the table (--full / --since)

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


def lift_since(last_success_iso: str | None, run_iso: str | None, hours: float = 1.0) -> str | None:
    """since_with_margin, but never later than THIS run's own stamp minus the margin.

    Rows are stamped with the run's start instant. A long hand-run (an S3 backfill)
    that overlaps a scheduled run would otherwise find the lift marker advanced past
    its own stamp and filter its own rows out of its lift - silently. Compares
    datetimes, never ISO strings (isoformat drops .000000, so string order lies).
    None = no marker yet = full lift."""
    prev = _parse_iso(last_success_iso)
    if prev is None:
        return None
    run = _parse_iso(run_iso)
    base = min(prev, run) if run is not None else prev
    return (base - dt.timedelta(hours=hours)).isoformat()


def read_lift_state(src_dir: Path) -> dict:
    p = Path(src_dir) / LIFT_STATE_FILE
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001 — a corrupt marker just means a full lift
        return {}


def write_lift_state(src_dir: Path, run_iso: str, report: dict | None = None,
                     extra: dict | None = None) -> None:
    """Record a SUCCESSFUL lift (lift_directory raised otherwise). The next
    incremental lift only reads rows updated after this run started. Keys not
    owned here (e.g. last_prune_iso) survive unless overridden via `extra`."""
    state = read_lift_state(src_dir)
    state.update({"last_success_run_iso": run_iso,
                  "written_at": dt.datetime.now(tz=dt.timezone.utc).isoformat(),
                  "tables": {k: v.get("written") for k, v in (report or {}).items()
                             if isinstance(v, dict) and "written" in v}})
    state.update(extra or {})
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
    reachable from THIS host; `blueblocx_hyperliquid.preflight --iceberg` checks exactly that.
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
            require_warehouse_mount(wh_path)
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


# Metadata retention: without these every commit keeps the previous metadata.json
# forever (hourly cadence = 24 new files a day per table) and the metadata log grows
# unbounded. pyiceberg honours both properties.
TABLE_PROPERTIES = {
    "write.metadata.delete-after-commit.enabled": "true",
    "write.metadata.previous-versions-max": "20",
}


def ensure_table(catalog, namespace: str, name: str, spec: dict, tz: bool = True, required=()):
    """Create the table from the DDL shape when absent (required=True for the columns
    in `required`); an EXISTING table is returned as is - Iceberg cannot promote an
    optional column to required, so older tables keep their nullable schema."""
    catalog.create_namespace_if_not_exists(namespace)
    ident = f"{namespace}.{name}"
    req = set(required or ())
    cols = [(c[0], c[1], (c[2] if len(c) > 2 else c[0] in req)) for c in spec["columns"]]
    schema = _iceberg_schema(cols, tz)
    if spec.get("partition"):
        table = catalog.create_table_if_not_exists(
            ident, schema=schema, partition_spec=_partition_spec(schema, spec["partition"]),
            properties=dict(TABLE_PROPERTIES))
    else:
        table = catalog.create_table_if_not_exists(ident, schema=schema, properties=dict(TABLE_PROPERTIES))
    missing = {k: v for k, v in TABLE_PROPERTIES.items() if table.properties.get(k) != v}
    if missing:                                   # tables created before this rule: one-time commit
        try:
            with table.transaction() as tx:
                tx.set_properties(**missing)
            table.refresh()
        except Exception as e:  # noqa: BLE001 — retention is hygiene, never worth failing a lift
            log.warning("could not set metadata-retention properties on %s: %s", ident, e)
    return table


def require_warehouse_mount(wh_path: Path, mounts_text: str | None = None) -> None:
    """Fail fast when HL_WAREHOUSE_MOUNT is set and the warehouse path is not on that
    filesystem: with the NAS unmounted, mkdir would quietly create the directory on
    the root disk and the catalog would start pointing at files that vanish when the
    mount comes back. Inside the systemd unit the warehouse shows up as its own
    ReadWritePaths bind mount below the NAS mount point; ops.warehouse_mount_check
    accepts that. No-op when the mount table is unavailable (Windows)."""
    expected = (os.environ.get("HL_WAREHOUSE_MOUNT") or "").rstrip("/")
    if not expected:
        return
    from .ops import warehouse_mount_check
    reason = warehouse_mount_check(wh_path.as_posix(), expected, mounts_text)
    if reason:
        raise RuntimeError(f"{reason} (HL_WAREHOUSE_MOUNT={expected})")


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


_NO_MATCH_WARNING = "Delete operation did not match any records"


def _replace(table, arrow: pa.Table, predicates: list) -> int:
    """Delete every row matching `predicates`, then append `arrow`, in ONE Iceberg
    transaction = one new metadata.json and one optimistic catalog UPDATE. The
    committed current snapshot is always the append, so a reader never sees the
    replaced keys missing and a failure before the commit leaves the previous
    snapshot untouched (verified against pyiceberg 0.12: metadata_log grows by one).
    A delete that matches nothing (new keys) only warns inside pyiceberg; that
    warning is expected here and silenced. Returns the number of catalog commits (1)."""
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message=_NO_MATCH_WARNING)
        with table.transaction() as tx:
            for p in predicates:
                if p is not None:
                    tx.delete(p)
            tx.append(arrow)
    return 1


def check_batch(table_name: str, arrow: pa.Table, key_cols: list[str], required_cols=()) -> None:
    """Refuse a batch that would violate the contract BEFORE any commit: a NULL in a
    natural-key or NOT-NULL-by-construction column, or two rows with the same natural
    key inside one batch (insert-if-absent only compares against the lake; the
    staged CSV is keyed, so a duplicate means a hand-edited file)."""
    for c in list(key_cols) + [c for c in required_cols if c not in key_cols]:
        if c in arrow.column_names and arrow.column(c).null_count:
            kind = "natural key" if c in key_cols else "NOT-NULL-by-construction column"
            raise ValueError(f"{table_name}: NULL in {kind} {c} ({arrow.column(c).null_count} row(s))")
    keys = list(zip(*[arrow.column(c).to_pylist() for c in key_cols]))
    if len(set(keys)) != len(keys):
        raise ValueError(f"{table_name}: {len(keys) - len(set(keys))} duplicate natural key(s) within the batch")


def _ident(table) -> str:
    name = table.name()
    return ".".join(name) if isinstance(name, tuple) else str(name)


def load_table(table, arrow: pa.Table, key_cols: list[str], mode: str, scope_cols: list[str],
               required_cols=()) -> dict:
    """Apply one of the idempotent strategies; returns counts (`commits` = catalog
    commits made for this table, 0 when nothing had to be written)."""
    if arrow.num_rows == 0:
        return {"incoming": 0, "written": 0, "skipped_existing": 0, "commits": 0}
    check_batch(_ident(table), arrow, key_cols, required_cols)
    arrow = conform(arrow, table)
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
                "skipped_existing": arrow.num_rows - new.num_rows, "commits": 1 if new.num_rows else 0}
    if mode == "replace_scope":
        from pyiceberg.expressions import EqualTo
        scope = sorted(set(zip(*[arrow.column(c).to_pylist() for c in scope_cols])))
        # One predicate per leading scope value (the partition, e.g. as_of_date) with an
        # In() over the trailing column (wallets); all of them plus the append go into
        # one transaction (one commit per lift for this table, whatever the day count).
        predicates = []
        if len(scope_cols) == 2:
            by_lead: dict = {}
            for lead, tail in scope:
                by_lead.setdefault(lead, []).append(tail)
            for lead, tails in sorted(by_lead.items()):
                predicates.append(_and(EqualTo(scope_cols[0], lead), _in_filter(scope_cols[1], tails)))
        else:
            for tup in scope:
                predicates.append(_and(*[EqualTo(c, v) for c, v in zip(scope_cols, tup)]))
        commits = _replace(table, arrow, predicates)
        return {"incoming": arrow.num_rows, "written": arrow.num_rows, "replaced_scopes": len(scope),
                "commits": commits}
    if mode == "replace_keys":
        c = key_cols[0]  # single-column keys for dimension tables
        commits = _replace(table, arrow, [_in_filter(c, arrow.column(c).to_pylist())])
        return {"incoming": arrow.num_rows, "written": arrow.num_rows, "commits": commits}
    raise ValueError(mode)


# --- control.table_watermarks ------------------------------------------------------

def _naive_utc(ts):
    if ts is None:
        return None
    return ts.astimezone(dt.timezone.utc).replace(tzinfo=None) if ts.tzinfo else ts


def write_watermarks(catalog, namespace: str, entries: list[tuple], run_id: str) -> dict[str, str]:
    """Upsert the watermark rows of one lift in ONE commit (delete + append in one
    transaction). `entries` = (table_name, watermark_col, watermark_ts, row_count,
    snapshot_id[, watermark_date[, status]]):
      * status completed (default): monotonic per table - an older watermark never
        replaces a newer one ("kept_newer"); watermark_ts = max WRITE stamp of the
        lifted batch, watermark_date = max business date of the batch;
      * failed: the lift of that table raised; the previous row's watermark values
        are kept and only status/run_id/updated_at change;
      * restated: an operator re-lifted the table (--full / --since); written even
        when the stamp is not newer.
    Returns {table_name: written|kept_newer}."""
    from pyiceberg.expressions import EqualTo
    if not entries:
        return {}
    tbl = ensure_table(catalog, CONTROL_NS, "table_watermarks", {"columns": _WM_COLS}, tz=False)
    names = sorted({e[0] for e in entries})
    pred = _and(EqualTo("namespace", namespace), _in_filter("table_name", names))
    existing = tbl.scan(row_filter=pred, selected_fields=("table_name", "watermark_col", "watermark_ts",
                                                          "watermark_date", "row_count")).to_arrow().to_pylist()
    prev: dict[str, dict] = {}
    for r in existing:
        cur = prev.get(r["table_name"])
        if cur is None or (r.get("watermark_ts") or dt.datetime.min) > (cur.get("watermark_ts") or dt.datetime.min):
            prev[r["table_name"]] = r
    now_naive = dt.datetime.now(tz=dt.timezone.utc).replace(tzinfo=None)
    status: dict[str, str] = {}
    rows: list[dict] = []
    for entry in entries:
        table_name, watermark_col, watermark_ts, row_count, snapshot_id = entry[:5]
        watermark_date = entry[5] if len(entry) > 5 else None
        wm_status = entry[6] if len(entry) > 6 else WM_STATUS_OK
        wm_naive = _naive_utc(watermark_ts)
        old = prev.get(table_name)
        if wm_status == WM_STATUS_OK and old is not None and old.get("watermark_ts") is not None \
                and wm_naive is not None and old["watermark_ts"] > wm_naive:
            status[table_name] = "kept_newer"
            continue
        if wm_status == WM_STATUS_FAILED and old is not None:
            # keep the last good watermark (value AND the column it was measured on);
            # only the status says the table is behind
            wm_naive = old.get("watermark_ts")
            watermark_col = old.get("watermark_col") or watermark_col
            watermark_date = old.get("watermark_date")
            row_count = old.get("row_count") if row_count is None else row_count
        if watermark_date is None and wm_naive is not None:
            watermark_date = wm_naive.date()
        status[table_name] = "written"
        rows.append({
            "namespace": namespace, "table_name": table_name, "layer": table_name.split("_")[0],
            "producer": "blueblocx_hyperliquid.iceberg_load", "watermark_col": watermark_col,
            "watermark_date": watermark_date, "watermark_ts": wm_naive,
            "row_count": row_count, "snapshot_id": str(snapshot_id) if snapshot_id is not None else None,
            "job_name": f"{namespace}_lift", "run_id": run_id, "updated_at": now_naive, "status": wm_status,
        })
    if rows:
        written = [r["table_name"] for r in rows]
        _replace(tbl, conform(rows_to_arrow(rows, _WM_COLS, tz=False), tbl),
                 [_and(EqualTo("namespace", namespace), _in_filter("table_name", written))])
    return status


def write_watermark(catalog, namespace: str, table_name: str, watermark_col: str,
                    watermark_ts: dt.datetime, row_count: int, snapshot_id, run_id: str) -> bool:
    """Single-row form of write_watermarks (kept for callers/tests)."""
    status = write_watermarks(catalog, namespace,
                              [(table_name, watermark_col, watermark_ts, row_count, snapshot_id)], run_id)
    return status.get(table_name) == "written"


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


def _max_value(arrow: pa.Table, col: str):
    """max() of a column computed in Arrow (no Python materialisation; nulls skipped);
    None for an absent column, an empty batch or an all-null column."""
    import pyarrow.compute as pc
    if col not in arrow.column_names or arrow.num_rows == 0:
        return None
    return pc.max(arrow.column(col)).as_py()


def dispose_catalog(catalog) -> None:
    """Release a SQL catalog's connection pool (SQLite keeps the file open on Windows;
    PostgreSQL holds a server connection). A no-op for REST catalogs."""
    engine = getattr(catalog, "engine", None)
    if engine is not None:
        engine.dispose()


def missing_tables(have) -> tuple[list[str], list[str]]:
    """(tables the lift creates and are absent, optional tables absent) given the set
    of table names present in the namespace - the one place that knows which planned
    tables production is not expected to populate."""
    have = set(have)
    return ([t for t in TABLE_PLAN if t not in have and t not in OPTIONAL_TABLES],
            [t for t in TABLE_PLAN if t not in have and t in OPTIONAL_TABLES])


def lift_directory(src_dir: Path, catalog=None, local_dir: Path | None = None,
                   tables: list[str] | None = None, namespace: str = DDL_NAMESPACE,
                   since_iso: str | None = None, status: str = WM_STATUS_OK,
                   watermarks: bool = True) -> dict:
    """Load every known CSV in src_dir into Iceberg `namespace`. Returns a report.
    Data lifts first, in TABLE_PLAN order; watermarks are a final, non-fatal step
    (`watermarks=False` skips them, e.g. for an early partial lift). With `since_iso`
    only rows updated after that instant are lifted (bounded incremental lift).
    `status` is the control.table_watermarks status to record (restated for an
    operator re-lift). If a table's lift raises, a 'failed' watermark row is written
    for it (best effort) and the exception propagates."""
    own_catalog = catalog is None
    if own_catalog:
        catalog, _ = load_catalog_from_env(local_dir)
    try:
        return _lift_directory(src_dir, catalog, tables, namespace, since_iso, status, watermarks)
    finally:
        if own_catalog:
            dispose_catalog(catalog)


def _lift_directory(src_dir: Path, catalog, tables, namespace: str, since_iso: str | None,
                    status: str, watermarks: bool) -> dict:
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
        stamp_col = _STAMP_COL[name]
        table = None
        try:
            rows = read_csv_rows(csv_path, since_iso, fallback_ts_col=_SINCE_FALLBACK_COL.get(name, stamp_col))
            arrow = rows_to_arrow(rows, spec["columns"], tz=True)
            table = ensure_table(catalog, namespace, name, spec, tz=True, required=REQUIRED_COLS.get(name, ()))
            counts = load_table(table, arrow, key_cols, mode, scope_cols, required_cols=REQUIRED_COLS.get(name, ()))
            table.refresh()
        except Exception as e:
            log.error("lift %s failed: %s: %s", name, type(e).__name__, str(e)[:200])
            if watermarks:
                failed = (name, stamp_col, None, _table_row_count(table) if table is not None else None, None,
                          None, WM_STATUS_FAILED)
                try:
                    write_watermarks(catalog, namespace, pending_wm + [failed], run_id)
                except Exception as e2:  # noqa: BLE001 — the data error is the one to report
                    log.warning("could not record the failed watermark for %s: %s", name, e2)
            raise
        total = _table_row_count(table)
        snap = table.current_snapshot()
        wm_ts = _max_value(arrow, stamp_col)
        if wm_ts is not None:
            assert isinstance(wm_ts, dt.datetime), f"{name}: stamp column {stamp_col} is not a timestamp"
            biz_col = _BUSINESS_DATE_COL.get(name)
            wm_date = _max_value(arrow, biz_col) if biz_col else None
            if isinstance(wm_date, dt.datetime):
                wm_date = wm_date.date()
            pending_wm.append((name, stamp_col, wm_ts, total, snap.snapshot_id if snap else None, wm_date, status))
        counts["table_total"] = total
        report[name] = counts
        log.info("lift %-28s incoming=%6d written=%6d commits=%d total=%7s", name, counts["incoming"],
                 counts["written"], counts.get("commits", 0), "?" if total is None else total)
    wm_status: dict[str, str] = {}
    if pending_wm and watermarks:
        try:
            wm_status = write_watermarks(catalog, namespace, pending_wm, run_id)      # 1 commit for all tables
        except Exception as e:  # noqa: BLE001 — never abort data lifts over watermark rows
            wm_status = {name: f"failed: {type(e).__name__}: {str(e)[:120]}" for name, *_ in pending_wm}
            log.warning("watermark write failed for %s: %s", namespace, e)
    report["_watermarks"] = wm_status
    report["_run_id"] = run_id
    return report


def main(argv=None) -> int:
    """Exit codes: 0 lifted, 1 the lift raised (traceback), 3 another run holds the
    state dir's lock (daily / extract / maintenance share it; retry when it is free)."""
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
    args = ap.parse_args(argv)
    tables = [t for t in args.tables.split(",") if t] if args.tables else None
    src = Path(args.src)
    run_iso = dt.datetime.now(tz=dt.timezone.utc).isoformat()
    since = None
    if not args.full:
        since = args.since or lift_since(read_lift_state(src).get("last_success_run_iso"), run_iso)
    # a deliberate re-lift (--full / --since) re-lands rows with their existing stamps:
    # recorded as 'restated' so a reader of control.table_watermarks can tell
    status = WM_STATUS_RESTATED if (args.full or args.since) else WM_STATUS_OK
    try:
        with RunLock(src / ".daily.lock"):
            rep = lift_directory(src, local_dir=Path(args.local_dir) if args.local_dir else None,
                                 tables=tables, namespace=args.namespace, since_iso=since, status=status)
            if not tables:                    # a partial (subset) lift must not advance the marker
                write_lift_state(src, run_iso, rep)
    except LockHeld as e:
        log.error("%s - another run owns the state dir; retry when it has finished", e)
        return 3
    print(json.dumps(rep, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
