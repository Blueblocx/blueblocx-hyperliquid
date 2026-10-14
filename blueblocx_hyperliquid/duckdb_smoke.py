"""
Read this lake the way the ETL's DuckDB will, and report what works.

The dagster_blueblocx ETL reads hypercore with DuckDB through the pyiceberg SQL
catalog (owner decision, 2026-10-14): one SELECT on the catalog's `iceberg_tables`
gives each table's current `metadata_location`, then `iceberg_scan(<that path>)`.
pyiceberg writes no version-hint.text and stores absolute `file:///mnt/...`
locations, so the reader must mount the NAS at the IDENTICAL path, and which
`iceberg_scan` form DuckDB accepts is exactly what this smoke test settles. It
tries, per table, and reports the first that works:

  raw       iceberg_scan('<metadata_location as the catalog holds it>')
  stripped  iceberg_scan('<the same as a plain local path>')          (file:// only)
  moved     iceberg_scan('<table root>', version = '<version>', allow_moved_paths = true)
            with <metadata_location> = <table root>/metadata/<version>.metadata.json:
            reads exactly the committed version named by the pointer and resolves
            every data/manifest path relative to the root, so it never depends on
            how DuckDB treats the absolute file:// locations stored INSIDE manifests.

Verified 2026-10-15 with DuckDB 1.5.6 (iceberg extension 890b78a9c): DuckDB resolves
standard three-slash `file:///...` locations (the form production writes), rejects the
two-slash `file://C:/...` form the Windows DEV catalog writes (pyiceberg on Windows
cannot write the three-slash form), and the `moved` form reads every table in both
cases - on a Windows dev lake it is the only one that works.

duckdb is deliberately NOT a dependency of the pipeline (requirements.txt): the
production venv never imports it. Run this from a throwaway venv as the login
user, with the READ-ONLY catalog role the bootstrap creates:

    python3 -m venv ~/duckdb-venv && ~/duckdb-venv/bin/pip install duckdb sqlalchemy psycopg2-binary
    cd ~/blueblocx-hyperliquid && HL_ENV_FILE=/etc/hypercore/reader.env ~/duckdb-venv/bin/python \
        -m blueblocx_hyperliquid.duckdb_smoke --json

or from the ETL host through the same role + read-only NFS mount (that run IS the
ETL's acceptance test). `preflight --duckdb` calls the same code when duckdb happens
to be importable and SKIPs otherwise.

The pointer query goes through SQLAlchemy (installed with pyiceberg), never through
pyiceberg's SqlCatalog whose constructor runs CREATE TABLE IF NOT EXISTS: a reader
must need nothing but SELECT.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path

from . import config
from .config import catalog_mode, redacted_uri as _redacted_uri, warehouse_local_path

try:  # the lift's table plan (names + which are development-only) needs pyarrow/pyiceberg ...
    from .iceberg_load import OPTIONAL_TABLES, TABLE_PLAN
    PLANNED_TABLES: list[str] | None = list(TABLE_PLAN)
except ModuleNotFoundError:  # ... which a reader venv (duckdb + sqlalchemy only) does not have:
    PLANNED_TABLES = None     # then the smoke reads whatever the catalog lists, like the ETL does
    OPTIONAL_TABLES = frozenset()

log = logging.getLogger("blueblocx_hyperliquid.duckdb_smoke")

# pyiceberg's SqlCatalog names its rows after the catalog name load_catalog() was given
CATALOG_NAME = {"sql": "hl", "local": "local"}


def _q(value: str) -> str:
    """A SQL string literal: iceberg_scan() takes a constant, so the path is inlined
    with single quotes doubled (a NAS share named "Leo's" must not break the statement)."""
    return "'" + str(value).replace("'", "''") + "'"


def _first_line(exc: BaseException, limit: int = 300) -> str:
    """DuckDB errors carry a 'LINE 1: ...' context block; the first line is the reason."""
    text = str(exc).strip().splitlines()
    return (text[0] if text else "")[:limit]


def duckdb_available() -> bool:
    try:
        import duckdb  # noqa: F401
        return True
    except Exception:  # noqa: BLE001
        return False


def pointer_query(catalog_name: str, namespace: str) -> tuple[str, dict]:
    """(sql, params) selecting every table's current metadata_location in a namespace.
    This is the ONE catalog read a DuckDB consumer needs per run (fetch pointers at
    the start of every run; metadata files older than write.metadata.previous-versions-max
    commits are deleted, so never cache a pointer across runs)."""
    sql = ("SELECT table_name, metadata_location FROM iceberg_tables "
           "WHERE catalog_name = :catalog_name AND table_namespace = :namespace ORDER BY table_name")
    return sql, {"catalog_name": catalog_name, "namespace": namespace}


def local_path(metadata_location: str) -> str:
    """The filesystem path of a file:// metadata location (the string unchanged for
    any other scheme, e.g. s3://)."""
    p = warehouse_local_path(metadata_location)
    return p.as_posix() if p is not None else metadata_location


def table_root_and_version(metadata_location: str) -> tuple[str, str] | None:
    """(table root, version) of a pointer shaped <root>/metadata/<version>.metadata.json
    (pyiceberg's naming: 00009-<uuid>.metadata.json; a .gz.metadata.json is accepted),
    i.e. what iceberg_scan(<root>, version = <version>, allow_moved_paths = true)
    needs. DuckDB's default version_name_format tries v<version>.metadata.json and
    <version>.metadata.json, so the file stem IS the version. None for any other shape."""
    loc = metadata_location.rstrip("/")
    head, sep, fname = loc.rpartition("/")
    if not sep or not head.endswith("/metadata"):
        return None
    for suffix in (".gz.metadata.json", ".metadata.json"):
        if fname.endswith(suffix) and len(fname) > len(suffix):
            return head[: -len("/metadata")], fname[: -len(suffix)]
    return None


def scan_variants(metadata_location: str) -> list[tuple[str, str]]:
    """The iceberg_scan() forms to try, in order (see the module docstring): the raw
    location as the catalog holds it, the stripped local path (file:// only), and the
    table root with the pointer's version pinned plus allow_moved_paths. iceberg_scan
    needs a constant path - a scalar subquery is not accepted - hence the two-step read."""
    stripped = local_path(metadata_location)
    variants = [("raw", f"iceberg_scan({_q(metadata_location)})")]
    if stripped != metadata_location:
        variants.append(("stripped", f"iceberg_scan({_q(stripped)})"))
    rv = table_root_and_version(stripped)
    if rv is not None:
        root, version = rv
        variants.append(("moved", f"iceberg_scan({_q(root)}, version = {_q(version)}, allow_moved_paths = true)"))
    return variants


def fetch_pointers(catalog_uri: str, catalog_name: str, namespace: str) -> dict[str, str]:
    """table_name -> metadata_location via SQLAlchemy (read-only SELECT)."""
    import sqlalchemy as sa
    sql, params = pointer_query(catalog_name, namespace)
    engine = sa.create_engine(catalog_uri)
    try:
        with engine.connect() as conn:
            rows = conn.execute(sa.text(sql), params).fetchall()
    finally:
        engine.dispose()
    return {str(r[0]): str(r[1]) for r in rows}


def run(catalog_uri: str | None = None, namespace: str = "hypercore", tables: list[str] | None = None,
        timezone: str = "UTC", out_dir: Path | None = None) -> dict:
    """Read every planned table through DuckDB; returns a JSON-able report."""
    import duckdb
    mode = catalog_mode()
    if catalog_uri is None:
        if mode == "rest":
            raise RuntimeError("REST catalog (NESSIE_URI): DuckDB reads of a Nessie lake are not this smoke's job")
        if mode == "sql":
            catalog_uri = os.environ["HL_CATALOG_URI"]
        else:
            # the dev catalog lives where the lift creates it; a READ never creates it
            db = Path(out_dir or config.out_root()) / "iceberg" / "catalog.db"
            catalog_uri = f"sqlite:///{db.resolve().as_posix()}"
            if not db.exists():
                return {"catalog": _redacted_uri(catalog_uri), "catalog_name": CATALOG_NAME["local"],
                        "namespace": namespace, "duckdb_version": duckdb.__version__, "variant": None,
                        "tables": {t: {"absent": True, "optional": t in OPTIONAL_TABLES}
                                   for t in (tables or PLANNED_TABLES or [])},
                        "note": "no dev catalog yet (nothing lifted)"}
    catalog_name = CATALOG_NAME.get(mode, "hl")
    report: dict = {"catalog": _redacted_uri(catalog_uri), "catalog_name": catalog_name, "namespace": namespace,
                    "duckdb_version": duckdb.__version__, "tables": {}, "variant": None}
    pointers = fetch_pointers(catalog_uri, catalog_name, namespace)
    wanted = tables or PLANNED_TABLES or sorted(pointers)
    con = duckdb.connect()
    try:
        try:
            con.execute("INSTALL iceberg; LOAD iceberg;")
        except Exception as e:  # noqa: BLE001 — the extension download needs egress to extensions.duckdb.org
            report["iceberg_extension"] = f"unavailable: {type(e).__name__}: {str(e)[:120]}"
            for t in wanted:
                report["tables"][t] = {"error": "iceberg extension not loadable"}
            return report
        try:
            con.execute(f"SET TimeZone = {_q(timezone)}")
        except Exception as e:  # noqa: BLE001 — needs the icu extension; report, do not fail
            report["timezone_note"] = f"SET TimeZone failed: {type(e).__name__}"
        ext = con.execute("SELECT extension_version FROM duckdb_extensions() WHERE extension_name = 'iceberg'").fetchone()
        report["iceberg_extension"] = ext[0] if ext else "loaded"
        for name in wanted:
            loc = pointers.get(name)
            if loc is None:
                report["tables"][name] = {"absent": True, "optional": name in OPTIONAL_TABLES}
                continue
            entry: dict = {"pointer": loc}
            errors: list[str] = []
            for variant, scan in scan_variants(loc):
                t0 = time.monotonic()
                try:
                    n = con.execute(f"SELECT count(*) FROM {scan}").fetchone()[0]
                    entry.update({"variant": variant, "scan": scan, "rows": int(n),
                                  "ms": round((time.monotonic() - t0) * 1000)})
                    report["variant"] = report["variant"] or variant
                    stamp = "created_at" if name == "l4x_perp_position_snapshot" else "updated_at"
                    try:
                        mx = con.execute(f"SELECT CAST(max({stamp}) AS VARCHAR) FROM {scan}").fetchone()[0]
                        entry["max_stamp"] = mx
                        entry["tz_ok"] = bool(mx) and (mx.endswith("+00") or mx.endswith("+00:00") or mx.endswith("Z"))
                    except Exception as e:  # noqa: BLE001
                        entry["max_stamp_error"] = f"{type(e).__name__}: {_first_line(e)}"
                    break
                except Exception as e:  # noqa: BLE001
                    errors.append(f"{variant}: {type(e).__name__}: {_first_line(e)}")
            if "rows" not in entry:
                entry["error"] = " | ".join(errors) or "no variant applicable"
            report["tables"][name] = entry
    finally:
        con.close()
    return report


def main(argv=None) -> int:
    config.load_env()
    config.setup_logging(level="WARNING")
    ap = argparse.ArgumentParser(description="Read the hypercore lake with DuckDB the way the ETL will")
    ap.add_argument("--namespace", default="hypercore")
    ap.add_argument("--tables", default=None, help="comma-separated subset (default: every planned table)")
    ap.add_argument("--timezone", default="UTC")
    ap.add_argument("--out", default=str(config.out_root()), help="state dir (dev catalog location)")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args(argv)
    if not duckdb_available():
        print("duckdb is not importable in this interpreter: pip install duckdb (in a throwaway venv, "
              "never in /opt)", file=sys.stderr)
        return 2
    tables = [t for t in args.tables.split(",") if t] if args.tables else None
    rep = run(namespace=args.namespace, tables=tables, timezone=args.timezone, out_dir=Path(args.out))
    failed = [t for t, r in rep["tables"].items() if r.get("error")]
    if args.json:
        print(json.dumps(rep, indent=2, default=str))
    else:
        print(f"duckdb {rep['duckdb_version']} iceberg {rep.get('iceberg_extension')} catalog {rep['catalog']} "
              f"({rep['catalog_name']}.{rep['namespace']}) variant {rep.get('variant')}")
        for t, r in rep["tables"].items():
            if r.get("absent"):
                print(f"  {'OPT ' if r.get('optional') else 'MISS'} {t:<28} not in the catalog")
            elif r.get("error"):
                print(f"  FAIL {t:<28} {r['error']}")
            else:
                print(f"  OK   {t:<28} rows={r['rows']} via {r['variant']} max_stamp={r.get('max_stamp')} "
                      f"tz_ok={r.get('tz_ok')} ({r['ms']} ms)")
        ok_scan = next((r["scan"] for r in rep["tables"].values() if r.get("scan")), None)
        if ok_scan:
            print(f"  read form that worked: {ok_scan}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
