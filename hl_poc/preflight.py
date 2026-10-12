"""
Pre-deployment / pre-run health check. One line per check, exit 1 if anything
FAILS. Secret values are never printed (presence only).

    python -m hl_poc.preflight                       # everything the current env allows
    python -m hl_poc.preflight --offline             # CI: deps, DDL, env sanity, paths - no network
    python -m hl_poc.preflight --iceberg --s3 --wallets-file /etc/hypercore/wallets.txt
    python -m hl_poc.preflight --json                # machine-readable

Statuses: OK · WARN (runs, but read it) · FAIL (do not schedule) · SKIP (not applicable).
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import shutil
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path

from . import __version__, config

_ADDR = re.compile(r"^0x[0-9a-f]{40}$")
_HL_MAX_WEIGHT = 1200          # documented per-IP cap
_MIN_RETENTION_DAYS = 31       # the 30d analytics windows need this much staged history
# Filesystem types where the state dir should NOT live: staged CSVs are rewritten
# whole on every upsert, the dev catalog is SQLite, and every run does thousands
# of small file operations - all slow or unsafe over a network filesystem.
_NETWORK_FS = {"nfs", "nfs3", "nfs4", "cifs", "smb3", "smbfs", "9p", "fuse.sshfs", "fuse.rclone",
               "fuse.s3fs", "fuse.juicefs", "glusterfs", "ceph", "lustre", "afs", "davfs", "fuse.davfs2"}


def mount_fstype(target: str, mounts_text: str | None = None) -> tuple[str, str] | None:
    """(mount_point, fstype) of the longest-prefix mount containing `target`, read from
    /proc/mounts (Linux). None when the table is unavailable (Windows, macOS)."""
    if mounts_text is None:
        try:
            mounts_text = Path("/proc/mounts").read_text(encoding="utf-8")
        except OSError:
            return None
    target = target.rstrip("/") or "/"
    best: tuple[str, str] | None = None
    for line in mounts_text.splitlines():
        parts = line.split()
        if len(parts) < 3:
            continue
        mp = parts[1].replace("\\040", " ")
        if mp == "/" or target == mp or target.startswith(mp.rstrip("/") + "/"):
            if best is None or len(mp) > len(best[0]):
                best = (mp, parts[2])
    return best


@dataclass
class Check:
    name: str
    status: str          # OK | WARN | FAIL | SKIP
    detail: str


def _fail(name, detail):
    return Check(name, "FAIL", detail)


def _ok(name, detail):
    return Check(name, "OK", detail)


def _warn(name, detail):
    return Check(name, "WARN", detail)


def _skip(name, detail):
    return Check(name, "SKIP", detail)


# --- local checks (no network) ----------------------------------------------------------

def check_python() -> Check:
    v = sys.version_info
    detail = f"{v.major}.{v.minor}.{v.micro} ({sys.executable})"
    if v < (3, 10):
        return _fail("python", f"{detail} - needs >= 3.10")
    return _ok("python", detail)


def check_imports() -> Check:
    want = ["requests", "xxhash", "pyarrow", "pyiceberg", "boto3", "lz4"]
    have, missing = [], []
    for mod in want:
        try:
            m = __import__(mod)
            have.append(f"{mod} {getattr(m, '__version__', getattr(m, 'VERSION', '?'))}")
        except Exception as e:  # noqa: BLE001
            missing.append(f"{mod} ({type(e).__name__})")
    if missing:
        return _fail("deps", "missing: " + ", ".join(missing) + " - pip install -r requirements.txt -c constraints.txt")
    return _ok("deps", ", ".join(have))


def check_tzdata() -> Check:
    """pyarrow needs the IANA zone database for timestamptz columns (absent on Windows
    without the tzdata wheel; present on Linux via /usr/share/zoneinfo)."""
    try:
        import zoneinfo
        zoneinfo.ZoneInfo("UTC")
        zoneinfo.ZoneInfo("America/New_York")
        import pyarrow as pa
        arr = pa.array([dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)], type=pa.timestamp("us", tz="UTC"))
        arr.cast(pa.timestamp("us")).to_pylist()
        return _ok("tzdata", "zoneinfo + pyarrow timestamptz cast ok")
    except Exception as e:  # noqa: BLE001
        return _fail("tzdata", f"{type(e).__name__}: {str(e)[:120]} - pip install tzdata")


def check_ddl() -> Check:
    path = config.ddl_path()
    if not path.is_file():
        return _fail("ddl", f"{path} not found (HL_DDL_PATH)")
    try:
        from .iceberg_load import TABLE_PLAN, parse_ddl
        ddl = parse_ddl(path)
    except Exception as e:  # noqa: BLE001
        return _fail("ddl", f"{type(e).__name__}: {str(e)[:120]}")
    missing = [t for t in TABLE_PLAN if t not in ddl]
    if missing:
        return _fail("ddl", f"{path}: tables missing from the DDL: {missing}")
    return _ok("ddl", f"{path.name}: {len(ddl)} hypercore table shapes")


def check_out_dir(out_dir: Path) -> Check:
    try:
        ns = out_dir / "hypercore"
        ns.mkdir(parents=True, exist_ok=True)
        probe = ns / ".preflight_write_test"
        probe.write_text("ok", encoding="utf-8")
        probe.unlink()
    except Exception as e:  # noqa: BLE001
        return _fail("out_dir", f"{out_dir} not writable: {type(e).__name__}: {str(e)[:100]}")
    try:
        free_gb = shutil.disk_usage(out_dir).free / 1e9
    except Exception:  # noqa: BLE001
        free_gb = None
    wm = out_dir / "hypercore" / "_watermarks.json"
    state = "existing state (watermarks present)" if wm.exists() else "fresh (no watermarks yet)"
    detail = f"{out_dir} writable, {state}" + (f", {free_gb:.1f} GB free" if free_gb is not None else "")
    notes = []
    if free_gb is not None and free_gb < 2.0:
        notes.append("under 2 GB; S3 hour files decompress to ~330 MB each")
    try:
        mount = mount_fstype(out_dir.resolve().as_posix())
    except OSError:
        mount = None
    if mount and mount[1] in _NETWORK_FS:
        notes.append(f"on a network filesystem ({mount[1]} at {mount[0]}): staged CSVs are rewritten "
                     f"whole on every upsert and the dev catalog is SQLite - put HL_OUT_DIR on a local disk")
    elif mount:
        detail += f", {mount[1]}"
    if notes:
        return _warn("out_dir", detail + " - " + "; ".join(notes))
    return _ok("out_dir", detail)


def check_env(env_file: Path | None) -> list[Check]:
    out: list[Check] = []
    out.append(_ok("env_file", f"loaded {env_file}") if env_file
               else _warn("env_file", "no .env found (HL_ENV_FILE / ./.env / <repo>/.env) - relying on the process environment"))
    try:
        budget = config.env_int("HL_WEIGHT_BUDGET")
        if budget <= 0 or budget > _HL_MAX_WEIGHT:
            out.append(_fail("weight_budget", f"HL_WEIGHT_BUDGET={budget} - must be 1..{_HL_MAX_WEIGHT} (HL per-IP cap)"))
        elif budget > 900:
            out.append(_warn("weight_budget", f"{budget}/min - leaves <300 for other pollers on this IP (TS platform-backend)"))
        else:
            out.append(_ok("weight_budget", f"{budget}/min of {_HL_MAX_WEIGHT}"))
    except ValueError as e:
        out.append(_fail("weight_budget", str(e)))
    try:
        ret = config.env_int("HL_RETENTION_DAYS")
        if ret < 0 or (0 < ret < _MIN_RETENTION_DAYS):
            out.append(_fail("retention", f"HL_RETENTION_DAYS={ret} - 0 (keep all) or >= {_MIN_RETENTION_DAYS} (30d windows)"))
        else:
            out.append(_ok("retention", "keep all staged rows" if ret == 0 else f"{ret} days after a successful lift"))
    except ValueError as e:
        out.append(_fail("retention", str(e)))
    try:
        config.env_int("HL_HTTP_TIMEOUT")
        float(config.env("HL_LOCK_STALE_HOURS"))
    except ValueError as e:
        out.append(_fail("env_numeric", str(e)))
    desc = config.describe_env()
    secrets_set = sorted(k for k in config.SECRET_VARS if desc[k]["set"])
    out.append(_ok("secrets", "set: " + (", ".join(secrets_set) if secrets_set else "none") + " (values not shown)"))
    unknown_hl = sorted(k for k in os.environ if k.startswith("HL_") and k not in config.ENV_VARS)
    if unknown_hl:
        out.append(_warn("env_unknown", f"HL_* variables nothing reads: {unknown_hl} (typo?)"))
    return out


def check_wallets(path: Path | None) -> Check:
    if path is None:
        return _skip("wallets", "no --wallets-file / HL_WALLETS_FILE")
    if not path.is_file():
        return _fail("wallets", f"{path} not found")
    from .daily import read_wallets
    wallets = read_wallets(path)
    bad = [w for w in wallets if not _ADDR.match(w)]
    dupes = len(wallets) - len(set(wallets))
    if bad:
        return _fail("wallets", f"{path}: {len(bad)} invalid address(es), e.g. {bad[0][:20]!r}")
    if not wallets:
        return _warn("wallets", f"{path}: empty")
    detail = f"{path}: {len(wallets)} wallet(s)"
    if dupes:
        return _warn("wallets", detail + f", {dupes} duplicate line(s)")
    return _ok("wallets", detail)


# --- network checks -----------------------------------------------------------------------

def check_hl_api() -> list[Check]:
    import requests
    url = config.env("HL_INFO_URL")
    timeout = config.env_int("HL_HTTP_TIMEOUT")
    t0 = time.monotonic()
    try:
        r = requests.post(url, json={"type": "meta"}, timeout=timeout)
        latency = (time.monotonic() - t0) * 1000
        r.raise_for_status()
        n = len((r.json() or {}).get("universe", []))
    except Exception as e:  # noqa: BLE001
        return [_fail("hl_api", f"{url}: {type(e).__name__}: {str(e)[:120]}")]
    out = [_ok("hl_api", f"{url} reachable, {n} perp assets, {latency:.0f} ms")]
    date_hdr = r.headers.get("Date")
    if date_hdr:
        try:
            from email.utils import parsedate_to_datetime
            server = parsedate_to_datetime(date_hdr)
            skew = abs((dt.datetime.now(tz=dt.timezone.utc) - server).total_seconds())
            out.append(_ok("clock", f"skew vs HL {skew:.0f}s") if skew <= 60
                       else _warn("clock", f"host clock is {skew:.0f}s off HL - time windows and watermarks depend on it (enable NTP)"))
        except Exception:  # noqa: BLE001
            pass
    return out


def check_s3(required: bool) -> Check:
    from .s3_backfill import BUCKET, _client, has_credentials, hour_keys
    if not has_credentials():
        msg = "no AWS credentials ---s3-backfill cannot run (API-capped wallets stay incomplete)"
        return _fail("s3", msg) if required else _skip("s3", msg)
    try:
        from botocore.exceptions import ClientError
        s3 = _client()
    except Exception as e:  # noqa: BLE001
        return _fail("s3", f"{type(e).__name__}: {str(e)[:120]}")
    now_ms = int(time.time() * 1000)
    last_key = ""
    # the newest hour file appears ~H+1:02, so probe two hours back, then three
    for back in (2, 3):
        key = hour_keys(now_ms - back * 3_600_000, now_ms - (back - 1) * 3_600_000)[0]
        last_key = key
        try:
            head = s3.head_object(Bucket=BUCKET, Key=key, RequestPayer="requester")
            mb = float(head.get("ContentLength", 0) or 0) / 1e6
            return _ok("s3", f"requester-pays HEAD ok: s3://{BUCKET}/{key} ({mb:.1f} MB)")
        except ClientError as e:
            code = str(e.response.get("Error", {}).get("Code", ""))
            status = str(e.response.get("ResponseMetadata", {}).get("HTTPStatusCode", ""))
            if code in ("404", "NoSuchKey", "NotFound") or status == "404":
                continue
            return _fail("s3", f"{code or status} on {key} - key invalid/rotated, or the IAM policy lacks "
                               f"s3:GetObject on {BUCKET} with requester-pays")
        except Exception as e:  # noqa: BLE001
            return _fail("s3", f"{type(e).__name__}: {str(e)[:120]}")
    return _warn("s3", f"credentials accepted but the recent hour files are not published yet (last tried {last_key})")


def _tcp_reachable(url: str, timeout: float = 3.0) -> bool:
    """Can this host open a TCP connection to the host:port of `url`?"""
    import socket
    from urllib.parse import urlsplit
    parts = urlsplit(url if "://" in url else f"http://{url}")
    host = parts.hostname
    port = parts.port or (443 if parts.scheme == "https" else 80)
    if not host:
        return False
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _check_sql_catalog(required: bool) -> Check:
    """Standalone mode: HL_CATALOG_URI (PostgreSQL/SQLite) + WAREHOUSE (file:// or s3://)."""
    from .iceberg_load import TABLE_PLAN, _redacted_uri, load_catalog_from_env, warehouse_local_path
    uri = os.environ.get("HL_CATALOG_URI", "")
    warehouse = os.environ.get("WAREHOUSE", "")
    notes, fails = [], []
    if uri.startswith("sqlite"):
        notes.append("SQLite catalog is single-writer (pyiceberg: development only) - use PostgreSQL for production")
    if not warehouse:
        fails.append("WAREHOUSE unset - set it to the data directory, e.g. file:///mnt/nas/hypercore/warehouse")
    wh_path = warehouse_local_path(warehouse) if warehouse else None
    if warehouse and wh_path is None and not warehouse.startswith(("s3://", "s3a://", "s3n://")):
        fails.append(f"WAREHOUSE {warehouse} is neither file:// nor s3://")
    if wh_path is not None:
        try:
            wh_path.mkdir(parents=True, exist_ok=True)
            probe = wh_path / ".preflight_write_test"
            probe.write_text("ok", encoding="utf-8")
            probe.unlink()
            try:
                free_gb = shutil.disk_usage(wh_path).free / 1e9
            except Exception:  # noqa: BLE001
                free_gb = None
            mount = mount_fstype(wh_path.resolve().as_posix())
            notes.append(f"warehouse {wh_path} writable" + (f", {free_gb:.0f} GB free" if free_gb is not None else "")
                         + (f", {mount[1]}" if mount else ""))
        except Exception as e:  # noqa: BLE001
            fails.append(f"warehouse {warehouse} not writable: {type(e).__name__}: {str(e)[:100]}")
    if fails:
        return _fail("iceberg", f"SQL catalog {_redacted_uri(uri)}: " + "; ".join(fails))
    catalog = None
    try:
        catalog, _ = load_catalog_from_env()
        namespaces = {".".join(n) if isinstance(n, tuple) else str(n) for n in catalog.list_namespaces()}
        have: set[str] = set()
        if "hypercore" in namespaces:
            have = {t[-1] if isinstance(t, tuple) else str(t) for t in catalog.list_tables("hypercore")}
    except Exception as e:  # noqa: BLE001
        return _fail("iceberg", f"SQL catalog {_redacted_uri(uri)}: {type(e).__name__}: {str(e)[:140]}"
                                f" - is PostgreSQL running and the driver (psycopg2-binary) installed?")
    finally:
        engine = getattr(catalog, "engine", None)
        if engine is not None:
            engine.dispose()                     # release the connection (SQLite keeps the file open)
    missing = [t for t in TABLE_PLAN if t not in have]
    detail = (f"SQL catalog {_redacted_uri(uri)}: {len(namespaces)} namespace(s), hypercore tables "
              f"{len(have)}/{len(TABLE_PLAN)}, warehouse {warehouse}")
    if missing:
        notes.append(f"missing {missing} - created from the DDL on the first lift")
    if notes:
        return _warn("iceberg", detail + "; " + "; ".join(notes))
    return _ok("iceberg", detail)


def check_iceberg(required: bool) -> Check:
    from .iceberg_load import catalog_mode
    mode = catalog_mode()
    if mode == "sql":
        return _check_sql_catalog(required)
    uri = os.environ.get("NESSIE_URI")
    if not uri:
        msg = ("neither NESSIE_URI nor HL_CATALOG_URI set - local SQLite catalog under HL_OUT_DIR/iceberg "
               "(development only)")
        return _fail("iceberg", msg) if required else _warn("iceberg", msg)
    notes = []
    if not os.environ.get("WAREHOUSE"):
        notes.append("WAREHOUSE unset (defaults to s3://warehouse)")
    try:
        from .iceberg_load import TABLE_PLAN, load_catalog_from_env
        catalog, _ = load_catalog_from_env()
        namespaces = {".".join(n) if isinstance(n, tuple) else str(n) for n in catalog.list_namespaces()}
        have: set[str] = set()
        if "hypercore" in namespaces:
            have = {t[-1] if isinstance(t, tuple) else str(t) for t in catalog.list_tables("hypercore")}
    except Exception as e:  # noqa: BLE001
        return _fail("iceberg", f"REST {uri}: {type(e).__name__}: {str(e)[:140]}")
    # The REST server's /v1/config OVERRIDES win over our properties. Nessie pushes
    # its `external-endpoint` as s3.endpoint: if that is a Docker-gateway or
    # otherwise private address, listing namespaces works but every data read and
    # write would hang. Same for the FileIO class: a server default of FsspecFileIO
    # needs s3fs, which is not installed; our client property should have won.
    props = dict(getattr(catalog, "properties", {}) or {})
    s3_ep = props.get("s3.endpoint")
    io_impl = props.get("py-io-impl") or ""
    fails = []
    if s3_ep and not _tcp_reachable(s3_ep):
        fails.append(f"the catalog advertises S3 endpoint {s3_ep}, unreachable from this host - "
                     f"Nessie external-endpoint must be an address this host can reach")
    if "fsspec" in io_impl:
        try:
            import s3fs  # noqa: F401
        except Exception:  # noqa: BLE001
            fails.append(f"effective py-io-impl is {io_impl} but s3fs is not installed - "
                         f"set HL_PY_IO_IMPL=pyiceberg.io.pyarrow.PyArrowFileIO or pip install s3fs")
    missing = [t for t in TABLE_PLAN if t not in have]
    detail = (f"REST {uri} branch {os.environ.get('NESSIE_BRANCH', 'main')}: {len(namespaces)} namespace(s), "
              f"hypercore tables {len(have)}/{len(TABLE_PLAN)}, s3 {s3_ep or '(none advertised)'}, "
              f"io {io_impl.rsplit('.', 1)[-1] or '?'}")
    if fails:
        return _fail("iceberg", detail + "; " + "; ".join(fails))
    if missing:
        notes.append(f"missing {missing} - created from the DDL on the first lift (or create them via docs/PROPOSED_SCHEMA.sql first)")
    if notes:
        return _warn("iceberg", detail + "; " + "; ".join(notes))
    return _ok("iceberg", detail)


def check_evm() -> Check:
    url = os.environ.get("HYPEREVM_RPC_URL")
    if not url:
        return _skip("hyperevm", "HYPEREVM_RPC_URL unset")
    import requests
    try:
        r = requests.post(url, json={"jsonrpc": "2.0", "id": 1, "method": "eth_blockNumber", "params": []},
                          timeout=config.env_int("HL_HTTP_TIMEOUT"))
        r.raise_for_status()
        head = int(r.json()["result"], 16)
        return _ok("hyperevm", f"rpc reachable, head block {head}")
    except Exception as e:  # noqa: BLE001
        return _fail("hyperevm", f"{type(e).__name__}: {str(e)[:120]}")


# --- driver -------------------------------------------------------------------------------

def run_checks(*, offline: bool = False, require_s3: bool = False, require_iceberg: bool = False,
               wallets_file: Path | None = None, out_dir: Path | None = None,
               env_file: Path | None = None) -> list[Check]:
    checks: list[Check] = [check_python(), check_imports(), check_tzdata(), check_ddl(),
                           check_out_dir(out_dir or config.out_root())]
    checks += check_env(env_file)
    checks.append(check_wallets(wallets_file))
    if offline:
        checks += [_skip("hl_api", "offline"), _skip("s3", "offline"),
                   check_iceberg(require_iceberg) if not os.environ.get("NESSIE_URI") else _skip("iceberg", "offline"),
                   _skip("hyperevm", "offline")]
        return checks
    checks += check_hl_api()
    checks.append(check_s3(require_s3))
    checks.append(check_iceberg(require_iceberg))
    checks.append(check_evm())
    return checks


def main(argv=None) -> int:
    env_file = config.load_env()
    config.setup_logging(level="WARNING")
    ap = argparse.ArgumentParser(description="HyperCore pipeline preflight")
    ap.add_argument("--offline", action="store_true", help="no network (CI)")
    ap.add_argument("--s3", action="store_true", help="S3 node-data access is REQUIRED")
    ap.add_argument("--iceberg", action="store_true", help="an Iceberg REST catalog is REQUIRED")
    ap.add_argument("--wallets-file", default=config.env("HL_WALLETS_FILE") or None)
    ap.add_argument("--out", default=str(config.out_root()))
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args(argv)
    checks = run_checks(offline=args.offline, require_s3=args.s3, require_iceberg=args.iceberg,
                        wallets_file=Path(args.wallets_file) if args.wallets_file else None,
                        out_dir=Path(args.out), env_file=env_file)
    failed = [c for c in checks if c.status == "FAIL"]
    if args.json:
        print(json.dumps({"version": __version__, "ok": not failed, "checks": [asdict(c) for c in checks]}, indent=2))
    else:
        print(f"hl_poc {__version__} preflight")
        for c in checks:
            print(f"  {c.status:<4} {c.name:<14} {c.detail}")
        print(f"  => {'FAIL' if failed else 'OK'} ({len(failed)} failing, "
              f"{sum(c.status == 'WARN' for c in checks)} warnings)")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
