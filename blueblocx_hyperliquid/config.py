"""
Runtime configuration for a server deployment.

Everything an operator can tune is an environment variable (12-factor). A `.env`
file is loaded as a convenience for hosts that are not containers (a systemd
`EnvironmentFile=` does the same job); variables already present in the process
environment always win, and values are never logged. `ENV_VARS` is the single
authoritative list: `preflight` validates against it and `deploy/.env.example`
mirrors it.
"""
from __future__ import annotations

import datetime as dt
import json
import logging
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

# name -> (default, is_secret, description)
ENV_VARS: dict[str, tuple[str, bool, str]] = {
    # --- Hyperliquid Info API -------------------------------------------------------
    "HL_INFO_URL": ("https://api.hyperliquid.xyz/info", False, "Info API endpoint"),
    "HL_WEIGHT_BUDGET": ("900", False,
                         "request weight per minute THIS process may spend. Hyperliquid allows "
                         "1200/min per IP and every other poller behind the same egress IP (e.g. "
                         "the TS platform-backend) shares that cap"),
    "HL_HTTP_TIMEOUT": ("30", False, "seconds per Info API request"),
    # --- state / paths ----------------------------------------------------------------
    "HL_OUT_DIR": (str(REPO_ROOT / "out"), False,
                   "state directory: staged CSVs, watermarks, run summaries, local Iceberg catalog"),
    "HL_ENV_FILE": ("", False, "explicit .env path (default: ./.env, then <repo>/.env)"),
    "HL_DDL_PATH": (str(REPO_ROOT / "docs" / "PROPOSED_SCHEMA.sql"), False,
                    "DDL the Iceberg loader derives table shapes from"),
    "HL_RETENTION_DAYS": ("45", False,
                          "after a successful Iceberg lift, drop staged l1/l3 CSV rows older than "
                          "this many days (0 = keep everything). Must stay >= 31 so the 30d "
                          "analytics windows remain complete"),
    # --- which wallets (tracked.py) ---------------------------------------------------
    "HL_WALLETS_FILE": ("", False,
                        "pinned wallets, one address per line: always pulled, first. Optional once "
                        "HL_TRACKED_SOURCE is set"),
    "HL_TRACKED_SOURCE": ("", False,
                          "'leaderboard' = also pull the tracked set the TS platform-backend sweeps live "
                          "(top whales by account value + smart money from the Hyperliquid leaderboard), "
                          "refreshed every run and cached. Empty = the wallets file only"),
    "HL_LEADERBOARD_URL": ("https://stats-data.hyperliquid.xyz/Mainnet/leaderboard", False,
                           "leaderboard payload (~39 MB; a separate stats host, no Info API weight)"),
    "HL_WHALE_TOP_N": ("100", False, "tracked whales: top N leaderboard rows by accountValue (TS WHALE_TOP_N)"),
    "HL_SMART_TOP_N": ("50", False,
                       "tracked smart money: top N by 30d pnl among rows passing the three floors below "
                       "(TS SMART_TOP_N)"),
    "HL_SMART_MIN_ACCOUNT_VALUE": ("100000", False,
                                   "smart-money floor: account value, USD (a value below 100000 also "
                                   "lowers the leaderboard scan floor so it takes effect)"),
    "HL_SMART_MIN_MONTH_ROI": ("0.2", False, "smart-money floor: 30d roi as a fraction (0.2 = 20%)"),
    "HL_SMART_MIN_MONTH_VLM": ("5000000", False, "smart-money floor: 30d volume, USD"),
    "HL_TRACKED_MAX_AGE_HOURS": ("48", False,
                                 "stats host down: reuse the cached tracked set up to this age, then run "
                                 "the pins only and report tracked_unavailable"),
    "HL_RUN_SOFT_DEADLINE_MIN": ("0", False,
                                 "minutes into a run after which no further wallet is started; the rest "
                                 "are deferred to the next run (rotating order) instead of the service "
                                 "timeout killing the whole run. Hourly cadence: 35 (TimeoutStartSec=50min). "
                                 "0 = off"),
    # --- daily runner -----------------------------------------------------------------
    "HL_LOCK_STALE_HOURS": ("2", False,
                            "a run lock older than this is treated as abandoned (a lock whose PID is dead is "
                            "discarded immediately on Linux regardless of age)"),
    "HL_ALERT_WEBHOOK_URL": ("", True,
                             "Slack/Discord-style incoming webhook; POSTed when a run has problems"),
    "HL_ALERT_ON_SUCCESS": ("0", False, "1 = also POST a summary when the run is clean"),
    "HL_ALERT_REPEAT_HOURS": ("6", False,
                              "re-send the webhook for an UNCHANGED problem set only this often (hourly runs "
                              "would otherwise post 24 times a day)"),
    "HL_WAREHOUSE_MOUNT": ("", False,
                           "standalone mode: the mount point the file:// warehouse must live on, e.g. "
                           "/mnt/blueblocx - the lift refuses to run (and preflight fails) when the NAS is not "
                           "mounted, instead of writing to the root disk"),
    "HL_LOG_LEVEL": ("INFO", False, "DEBUG | INFO | WARNING"),
    "HL_LOG_JSON": ("0", False, "1 = one JSON object per log line (for a log shipper)"),
    # --- S3 node data (fills beyond the API cap; Requester Pays) ---------------------
    "AWS_ACCESS_KEY_ID": ("", True,
                          "IAM key with s3:GetObject on hl-mainnet-node-data. Requester Pays: "
                          "every download is billed to THIS account"),
    "AWS_SECRET_ACCESS_KEY": ("", True, ""),
    "AWS_REGION": ("us-east-1", False, "region for the S3 client (bucket redirects are followed)"),
    # --- Iceberg catalog: REST (shared atlas lake) or SQL (standalone host) ------------
    "NESSIE_URI": ("", False,
                   "Iceberg REST endpoint, e.g. http://nessie:19120/iceberg (the shared atlas lake). "
                   "Unset and HL_CATALOG_URI unset = local SQLite catalog under HL_OUT_DIR/iceberg "
                   "(development only)"),
    "NESSIE_BRANCH": ("main", False, "Nessie branch (pyiceberg REST `prefix`)"),
    "NESSIE_TOKEN": ("", True, "bearer token when the REST catalog is authenticated"),
    "HL_CATALOG_URI": ("", True,
                       "standalone mode: SQLAlchemy URL of the pyiceberg SQL catalog, e.g. "
                       "postgresql+psycopg2://hl:pw@localhost:5432/hl_catalog (SQLite is single-writer, "
                       "development only). No object store or Nessie needed; data goes to WAREHOUSE"),
    "HL_PY_IO_IMPL": ("pyiceberg.io.pyarrow.PyArrowFileIO", False,
                      "pyiceberg FileIO class. Nessie advertises FsspecFileIO, which needs the s3fs "
                      "package this project does not install"),
    "WAREHOUSE": ("", False,
                  "where table data lives: REST mode s3://warehouse (bucket); SQL/local mode a plain "
                  "directory such as file:///mnt/blueblocx/hypercore/warehouse (NFS is fine for the "
                  "warehouse; HL_OUT_DIR must stay local) or an s3:// bucket with the S3_* variables"),
    "S3_ENDPOINT": ("", False, "object-store endpoint for an s3:// warehouse (MinIO/SeaweedFS/S3); unset = AWS"),
    "S3_ACCESS_KEY": ("", True, "warehouse object-store key"),
    "S3_SECRET_KEY": ("", True, ""),
    "S3_REGION": ("", False, "warehouse object-store region"),
    # --- HyperEVM (optional) ----------------------------------------------------------
    "HYPEREVM_RPC_URL": ("", False, "JSON-RPC endpoint for the HyperEVM extract"),
}
SECRET_VARS = frozenset(k for k, (_, secret, _) in ENV_VARS.items() if secret)


# --- .env --------------------------------------------------------------------------

def _apply_env_file(path: Path) -> int:
    """KEY=VALUE lines -> os.environ (setdefault). Returns how many keys were set."""
    n = 0
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        if line.startswith("export "):
            line = line[len("export "):].strip()
        key, value = line.split("=", 1)
        key, value = key.strip(), value.strip()
        if len(value) >= 2 and value[0] in "\"'" and value[-1] == value[0]:
            value = value[1:-1]
        elif " #" in value:                      # inline comment on an unquoted value
            value = value.split(" #", 1)[0].rstrip()
        if key and key not in os.environ:
            os.environ[key] = value
            n += 1
    return n


def load_env(path: str | Path | None = None) -> Path | None:
    """Load the first .env found (explicit `path`, $HL_ENV_FILE, ./.env, <repo>/.env)
    without overriding variables already in the environment. Returns the file used
    or None. Values are never logged."""
    candidates: list[Path] = []
    if path:
        candidates.append(Path(path))
    if os.environ.get("HL_ENV_FILE"):
        candidates.append(Path(os.environ["HL_ENV_FILE"]))
    candidates += [Path.cwd() / ".env", REPO_ROOT / ".env"]
    for p in candidates:
        if p.is_file():
            _apply_env_file(p)
            return p
    return None


# --- typed accessors --------------------------------------------------------------

def env(name: str, default: str | None = None) -> str:
    """Value of `name`, else `default`, else the documented default ('' if unknown)."""
    v = os.environ.get(name)
    if v not in (None, ""):
        return v
    if default is not None:
        return default
    return ENV_VARS.get(name, ("", False, ""))[0]


def env_int(name: str, default: int | None = None) -> int:
    raw = env(name, None if default is None else str(default))
    try:
        return int(float(raw))
    except (TypeError, ValueError):
        raise ValueError(f"{name} must be an integer, got {raw!r}") from None


def env_float(name: str, default: float | None = None) -> float:
    raw = env(name, None if default is None else str(default))
    try:
        return float(raw)
    except (TypeError, ValueError):
        raise ValueError(f"{name} must be a number, got {raw!r}") from None


def env_bool(name: str, default: bool = False) -> bool:
    raw = env(name, "1" if default else "0")
    return raw.strip().lower() in ("1", "true", "yes", "on")


def out_root() -> Path:
    return Path(env("HL_OUT_DIR")).expanduser()


def ddl_path() -> Path:
    return Path(env("HL_DDL_PATH")).expanduser()


def describe_env() -> dict[str, dict]:
    """{name: {set, value|<redacted>, default}} for preflight output. Secrets are
    reported as present/absent only."""
    out = {}
    for name, (default, secret, _) in ENV_VARS.items():
        raw = os.environ.get(name)
        is_set = raw not in (None, "")
        out[name] = {"set": is_set, "default": default,
                     "value": ("<redacted>" if secret else raw) if is_set else None}
    return out


# --- logging ----------------------------------------------------------------------

class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:  # noqa: D401
        d = {
            "ts": dt.datetime.fromtimestamp(record.created, tz=dt.timezone.utc)
            .isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if record.exc_info:
            d["exc"] = self.formatException(record.exc_info)
        return json.dumps(d, ensure_ascii=False)


def setup_logging(level: str | None = None, json_lines: bool | None = None, stream=None) -> None:
    """Root logger -> stderr, plain or JSON lines (HL_LOG_JSON=1). Idempotent."""
    root = logging.getLogger()
    for h in list(root.handlers):
        root.removeHandler(h)
    handler = logging.StreamHandler(stream or sys.stderr)
    use_json = env_bool("HL_LOG_JSON") if json_lines is None else json_lines
    if use_json:
        handler.setFormatter(_JsonFormatter())
    else:
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    root.addHandler(handler)
    root.setLevel((level or env("HL_LOG_LEVEL")).upper())
    for noisy in ("botocore", "boto3", "urllib3", "s3transfer", "pyiceberg"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
