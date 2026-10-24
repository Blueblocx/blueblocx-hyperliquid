#!/usr/bin/env bash
# One-shot server bootstrap for the standalone layout (Ubuntu 22.04/24.04, systemd,
# NAS already mounted at $NAS_MOUNT). Run as your login user with sudo rights, from
# a working-copy clone of this repo:
#
#   git clone git@github-blueblocx-hyperliquid:Blueblocx/blueblocx-hyperliquid.git ~/blueblocx-hyperliquid
#   bash ~/blueblocx-hyperliquid/deploy/bootstrap.sh
#
# What it does (idempotent; re-running is safe while no timer or run is active):
#   1. apt packages (python3-venv, git, postgresql, jq, nfs-common)
#   2. service user `hypercore` (no shell, no keys); you are added to its group
#   3. production checkout /opt/blueblocx-hyperliquid at $RELEASE_TAG (owner you,
#      group-readable by the service), its venv, the test suite
#   4. working-copy venv in the clone you ran this from
#   5. PostgreSQL role + database for the pyiceberg catalog (password generated once)
#   6. /etc/hypercore/hypercore.env (standalone: PostgreSQL catalog + NAS warehouse,
#      S3 backfill OFF, wallet list = leaderboard tracked set + optional pins; an
#      existing file keeps its values and only gains the keys new in this release),
#      /etc/hypercore/wallets.txt placeholder for the pins
#   7. NAS warehouse directory, written as the service user (catches uid squash)
#   8. systemd units: service + hourly timer + nightly maintenance, with a drop-in
#      that removes the S3 flags (no AWS key in this layout) and caps a run at 50 min;
#      the web monitor (hypercore-web.service, port 8787) is enabled and started;
#      /usr/local/bin/hypercore-status and hypercore-sql (read-only SQL over the lake)
#   8b. lake schema migrations as the service user (python -m blueblocx_hyperliquid.migrate --apply:
#      idempotent; the only way a production table's schema changes; fatal when it fails)
#   9. preflight as the service user
#   5b. (only with READER_CIDR=<etl host ip>/32) a READ-ONLY catalog role `hl_ro` for the
#      ETL's DuckDB reader + /etc/hypercore/reader.env, PostgreSQL listening on the LAN
#      address with a pg_hba rule for that CIDR only (docs/DEPLOY.md "Serving the lake
#      to DuckDB readers"); the NFS read-only export is a Synology setting, printed below
# It does NOT start the timers or run the pipeline (next steps are printed at the end).
# Overrides: export RELEASE_TAG / GIT_ALIAS / NAS_MOUNT / WEIGHT_BUDGET / READER_CIDR (/ HOST_LAN_IP) before running.
set -euo pipefail

# ----------------------------------------------------------------------------- settings
RELEASE_TAG="${RELEASE_TAG:-v1.10.9}"
GIT_ALIAS="${GIT_ALIAS:-github-blueblocx-hyperliquid}"        # Host alias in ~/.ssh/config
REPO_URL="git@${GIT_ALIAS}:Blueblocx/blueblocx-hyperliquid.git"
PROD_DIR="/opt/blueblocx-hyperliquid"
SERVICE_USER="hypercore"
STATE_DIR="/var/lib/hypercore"
CONF_DIR="/etc/hypercore"
ENV_FILE="$CONF_DIR/hypercore.env"
WALLETS_FILE="$CONF_DIR/wallets.txt"
NAS_MOUNT="${NAS_MOUNT:-/mnt/blueblocx}"
NAS_BASE="${NAS_MOUNT}/hypercore"
WAREHOUSE_DIR="${NAS_BASE}/warehouse"
PG_DB="hl_catalog"
PG_ROLE="hl"
WEIGHT_BUDGET="${WEIGHT_BUDGET:-900}"
READER_CIDR="${READER_CIDR:-}"            # e.g. 192.168.1.50/32: the ETL host allowed to read the catalog
READER_ENV="$CONF_DIR/reader.env"
PG_RO_ROLE="hl_ro"
ME="$(id -un)"
WORKDIR="$(cd "$(dirname "$0")/.." && pwd)"
TEST_LOG="/tmp/hypercore-bootstrap-tests.log"

say()  { printf '\n\033[1;34m== %s\033[0m\n' "$*"; }
# A venv is bound to the interpreter it was created with (bin/python -> /usr/bin/python3,
# site-packages under lib/python3.X). When the OS replaces python3 (release upgrade:
# 3.12 -> 3.14 on 2026-10-20 took the hourly run down at preflight with
# "No module named xxhash"), the venv silently loses every package. Recreate it then.
ensure_venv() {
  local dir="$1" sys_ver venv_ver
  sys_ver="$(python3 -c 'import sys; print("%d.%d" % sys.version_info[:2])')"
  if [ -x "$dir/bin/python" ]; then
    venv_ver="$(sed -n 's/^version = \([0-9]*\.[0-9]*\).*/\1/p' "$dir/pyvenv.cfg" 2>/dev/null)"
    if [ "$venv_ver" = "$sys_ver" ] && "$dir/bin/python" -c 'import pip' >/dev/null 2>&1; then
      return 0
    fi
    warn "venv ${dir} was built for python ${venv_ver:-?} but python3 is ${sys_ver} (or pip is missing) - recreating it"
    rm -rf "$dir"
  fi
  python3 -m venv "$dir"
}
warn() { printf '\033[1;33m!! %s\033[0m\n' "$*"; }
die()  { printf '\033[1;31mxx %s\033[0m\n' "$*" >&2; exit 1; }

# ----------------------------------------------------------------------------- preconditions
[ "$ME" != "root" ] || die "run as your login user (with sudo), not as root"
# sudo-rs (Ubuntu 26.04) refuses `sudo -v` without a terminal even when no password is needed: only prompt when needed
sudo -n true 2>/dev/null || sudo -v
mountpoint -q "$NAS_MOUNT" || die "$NAS_MOUNT is not a mount point - mount the NAS first (fstab: vers=4.1,hard,rsize=1048576,wsize=1048576,noatime,_netdev)"
# this script rewrites $PROD_DIR: refuse while anything could execute it
for unit in hypercore-daily.service hypercore-maintenance.service hypercore-hourly.timer hypercore-daily.timer hypercore-maintenance.timer; do
  if systemctl is-active --quiet "$unit"; then
    die "$unit is active - stop the timers first and wait for the services to finish: sudo systemctl stop hypercore-hourly.timer hypercore-daily.timer hypercore-maintenance.timer && while systemctl is-active --quiet hypercore-daily.service; do sleep 10; done  (upgrades: docs/DEPLOY.md section 8)"
  fi
done
# a HAND-RUN extract / lift / maintenance is invisible to systemctl but holds the state-dir
# lock and executes code from a checkout: refuse while one is alive
if pgrep -f 'blueblocx_hyperliquid\.(daily|extract|iceberg_load|maintenance|migrate)( |$)' >/dev/null 2>&1; then
  die "a hand-run pipeline process is active ($(pgrep -af 'blueblocx_hyperliquid\.' | head -3 | tr '\n' ';')) - let it finish or stop it first"
fi
LOCK_FILE="$STATE_DIR/hypercore/.daily.lock"
if sudo test -f "$LOCK_FILE"; then
  LOCK_PID="$(sudo jq -r '.pid // empty' "$LOCK_FILE" 2>/dev/null || true)"
  if [ -n "$LOCK_PID" ] && [ -d "/proc/$LOCK_PID" ]; then
    die "the run lock $LOCK_FILE is held by live pid $LOCK_PID - wait for that run to finish"
  fi
  warn "stale run lock $LOCK_FILE (holder gone) - the next run discards it"
fi
# GitHub closes the test connection with exit 1 even on success, so capture and match
SSH_OUT="$(ssh -T -o BatchMode=yes -o StrictHostKeyChecking=accept-new "git@${GIT_ALIAS}" 2>&1 || true)"
case "$SSH_OUT" in
  *"successfully authenticated"*) ;;
  *) die "ssh to ${GIT_ALIAS} failed - add the deploy key (write access) in GitHub and check ~/.ssh/config: ${SSH_OUT}" ;;
esac

# ----------------------------------------------------------------------------- 1. packages
say "packages"
sudo apt-get update -qq
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq python3-venv python3-pip git postgresql jq nfs-common openssl >/dev/null
python3 --version

# ----------------------------------------------------------------------------- 2. users + dirs
say "service user ${SERVICE_USER}"
if ! id -u "$SERVICE_USER" >/dev/null 2>&1; then
  sudo useradd --system --home-dir "$STATE_DIR" --no-create-home --shell /usr/sbin/nologin "$SERVICE_USER"
fi
sudo usermod -aG "$SERVICE_USER" "$ME"
sudo mkdir -p "$CONF_DIR" "$STATE_DIR/hypercore"
sudo chown -R "$SERVICE_USER:$SERVICE_USER" "$STATE_DIR"
sudo chmod 750 "$STATE_DIR"

# ----------------------------------------------------------------------------- 3. production checkout
say "production checkout ${PROD_DIR} @ ${RELEASE_TAG}"
if [ ! -d "$PROD_DIR/.git" ]; then
  sudo mkdir -p "$PROD_DIR"
  sudo chown "$ME:$SERVICE_USER" "$PROD_DIR"
  git clone --quiet --branch "$RELEASE_TAG" "$REPO_URL" "$PROD_DIR"
else
  git -C "$PROD_DIR" fetch --tags --quiet
  git -C "$PROD_DIR" checkout --detach --quiet "$RELEASE_TAG"
fi
ensure_venv "$PROD_DIR/.venv"
echo "installing pinned dependencies (a few minutes the first time)"
"$PROD_DIR/.venv/bin/pip" install -q --disable-pip-version-check -r "$PROD_DIR/requirements.txt" -c "$PROD_DIR/constraints.txt"
( cd "$PROD_DIR" && .venv/bin/python -m unittest discover -s tests -t . >"$TEST_LOG" 2>&1 ) \
  || { tail -40 "$TEST_LOG"; die "test suite failed in ${PROD_DIR} (full log: ${TEST_LOG})"; }
tail -3 "$TEST_LOG"
# the service runs with /opt read-only (ProtectSystem=strict): byte-compile now, as the owner
"$PROD_DIR/.venv/bin/python" -m compileall -q "$PROD_DIR/blueblocx_hyperliquid" >/dev/null
sudo chown -R "$ME:$SERVICE_USER" "$PROD_DIR"
chmod -R g+rX,o-rwx "$PROD_DIR"

# ----------------------------------------------------------------------------- 4. working copy venv
say "working copy ${WORKDIR}"
ensure_venv "$WORKDIR/.venv"
"$WORKDIR/.venv/bin/pip" install -q --disable-pip-version-check -r "$WORKDIR/requirements.txt" -c "$WORKDIR/constraints.txt"

# ----------------------------------------------------------------------------- 5. PostgreSQL catalog
say "PostgreSQL catalog ${PG_DB}"
sudo systemctl enable --now postgresql >/dev/null
PG_PASS=""
if sudo test -f "$ENV_FILE"; then
  PG_PASS="$(sudo sed -n 's#^HL_CATALOG_URI=postgresql+psycopg2://[^:]*:\([^@]*\)@.*#\1#p' "$ENV_FILE")"
  [ -n "$PG_PASS" ] || warn "$ENV_FILE has no postgresql+psycopg2://${PG_ROLE}:<password>@ catalog URI - PostgreSQL step skipped"
else
  PG_PASS="$(openssl rand -hex 24)"
fi
if [ -n "$PG_PASS" ]; then
  # from /: sudo keeps the cwd and the postgres user cannot enter your home
  ( cd / && sudo -u postgres psql -v ON_ERROR_STOP=1 -q <<SQL
DO \$\$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '${PG_ROLE}') THEN
    CREATE ROLE ${PG_ROLE} LOGIN PASSWORD '${PG_PASS}';
  ELSE
    ALTER ROLE ${PG_ROLE} WITH LOGIN PASSWORD '${PG_PASS}';
  END IF;
END \$\$;
SELECT 'CREATE DATABASE ${PG_DB} OWNER ${PG_ROLE}' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '${PG_DB}')\gexec
SQL
  )
fi

# ----------------------------------------------------------------------------- 5b. read-only catalog role for the ETL's DuckDB reader
if [ -n "$READER_CIDR" ]; then
  say "read-only catalog role ${PG_RO_ROLE} for ${READER_CIDR}"
  RO_PASS=""
  if sudo test -f "$READER_ENV"; then
    RO_PASS="$(sudo sed -n 's#^HL_CATALOG_URI=postgresql+psycopg2://[^:]*:\([^@]*\)@.*#\1#p' "$READER_ENV")"
  fi
  [ -n "$RO_PASS" ] || RO_PASS="$(openssl rand -hex 24)"
  # the address the ETL host can reach = the source address of the route TO it (not the
  # first entry of `hostname -I`, which may be docker0 or a VPN interface); HOST_LAN_IP overrides
  READER_HOST="${READER_CIDR%%/*}"
  HOST_LAN_IP="${HOST_LAN_IP:-$(ip -o route get "$READER_HOST" 2>/dev/null | sed -n 's/.* src \([0-9.]*\).*/\1/p' | head -1)}"
  [ -n "$HOST_LAN_IP" ] || HOST_LAN_IP="$(hostname -I 2>/dev/null | awk '{print $1}')"
  [ -n "$HOST_LAN_IP" ] || die "could not determine this host's address towards ${READER_CIDR} - export HOST_LAN_IP=<lan ip>"
  echo "reader connects to ${HOST_LAN_IP}:5432 (override with HOST_LAN_IP=... if that is not the LAN address)"
  # SELECT-only role; default_transaction_read_only stops even an accidental UPDATE of the
  # catalog pointer (the writer's commit path) from this role
  ( cd / && sudo -u postgres psql -v ON_ERROR_STOP=1 -q <<SQL
DO \$\$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '${PG_RO_ROLE}') THEN
    CREATE ROLE ${PG_RO_ROLE} LOGIN PASSWORD '${RO_PASS}';
  ELSE
    ALTER ROLE ${PG_RO_ROLE} WITH LOGIN PASSWORD '${RO_PASS}';
  END IF;
END \$\$;
ALTER ROLE ${PG_RO_ROLE} SET default_transaction_read_only = on;
GRANT CONNECT ON DATABASE ${PG_DB} TO ${PG_RO_ROLE};
SQL
  )
  # the catalog tables are created lazily by role hl on the first lift: grant what exists
  # now AND default privileges for whatever hl creates later
  ( cd / && sudo -u postgres psql -v ON_ERROR_STOP=1 -q -d "$PG_DB" <<SQL
GRANT USAGE ON SCHEMA public TO ${PG_RO_ROLE};
GRANT SELECT ON ALL TABLES IN SCHEMA public TO ${PG_RO_ROLE};
ALTER DEFAULT PRIVILEGES FOR ROLE ${PG_ROLE} IN SCHEMA public GRANT SELECT ON TABLES TO ${PG_RO_ROLE};
SQL
  )
  # reader.env: the connection the ETL host (and blueblocx_hyperliquid.duckdb_smoke) use;
  # yours, mode 600, never pasted anywhere
  sudo install -m 600 -o "$ME" -g "$ME" /dev/null "$READER_ENV"
  sudo tee "$READER_ENV" >/dev/null <<EOF
# read-only access to the hypercore Iceberg catalog for DuckDB readers (ETL). Never commit.
HL_CATALOG_URI=postgresql+psycopg2://${PG_RO_ROLE}:${RO_PASS}@${HOST_LAN_IP}:5432/${PG_DB}
WAREHOUSE=file://${WAREHOUSE_DIR}
EOF
  # PostgreSQL: listen on the LAN address too, accept ONLY the reader role from the ETL CIDR
  # the cluster that actually serves the catalog (a host may carry an older, stopped
  # cluster too: /etc/postgresql/16 next to the live 18 - the first directory is not it)
  PG_MAIN="$(dirname "$(cd / && sudo -u postgres psql -Atc 'SHOW config_file')")"
  [ -n "$PG_MAIN" ] && [ -d "$PG_MAIN" ] || die "could not locate the running PostgreSQL cluster's config directory"
  sudo mkdir -p "$PG_MAIN/conf.d"
  printf "# written by deploy/bootstrap.sh (READER_CIDR): the ETL's DuckDB reader connects from the LAN\nlisten_addresses = 'localhost,%s'\n" "$HOST_LAN_IP" \
    | sudo tee "$PG_MAIN/conf.d/hypercore-reader.conf" >/dev/null
  PG_HBA="$(cd / && sudo -u postgres psql -Atc 'SHOW hba_file')"
  PG_SSL="$(cd / && sudo -u postgres psql -Atc 'SHOW ssl')"
  HBA_TYPE="host"; [ "$PG_SSL" = "on" ] && HBA_TYPE="hostssl"
  HBA_LINE="${HBA_TYPE} ${PG_DB} ${PG_RO_ROLE} ${READER_CIDR} scram-sha-256"
  if ! sudo grep -qF "${PG_DB} ${PG_RO_ROLE} ${READER_CIDR}" "$PG_HBA"; then
    printf "# hypercore reader (deploy/bootstrap.sh READER_CIDR)\n%s\n" "$HBA_LINE" | sudo tee -a "$PG_HBA" >/dev/null
  fi
  if command -v ufw >/dev/null 2>&1 && sudo ufw status 2>/dev/null | grep -q "Status: active"; then
    sudo ufw allow from "$READER_CIDR" to any port 5432 proto tcp >/dev/null
  fi
  sudo systemctl restart postgresql           # listen_addresses needs a restart; no run is active here
  echo "reader role ready: ${READER_ENV} (600, ${ME}); PostgreSQL listens on ${HOST_LAN_IP}:5432 for ${PG_RO_ROLE}@${READER_CIDR} (${HBA_TYPE})"
  echo "NAS: add a Synology NFS rule for the ETL host (Read only) so it can mount ${NAS_MOUNT} at the identical path"
fi

# ----------------------------------------------------------------------------- 6. env + wallets
say "config ${CONF_DIR}"
if ! sudo test -f "$ENV_FILE"; then
  # create with the final mode first: the file holds the catalog password
  sudo install -m 640 -o root -g "$SERVICE_USER" /dev/null "$ENV_FILE"
  sudo tee "$ENV_FILE" >/dev/null <<EOF
# blueblocx-hyperliquid - standalone host (PostgreSQL catalog + NAS warehouse). Never commit.
HL_INFO_URL=https://api.hyperliquid.xyz/info
HL_WEIGHT_BUDGET=${WEIGHT_BUDGET}
HL_HTTP_TIMEOUT=30

HL_OUT_DIR=${STATE_DIR}
HL_RETENTION_DAYS=45

# Which wallets: the Info API is per address, so a run needs a list. Pinned wallets in
# HL_WALLETS_FILE (optional, always first) + the tracked set the TS platform-backend
# sweeps live: top HL_WHALE_TOP_N leaderboard rows by account value and up to
# HL_SMART_TOP_N smart-money rows, refreshed every run (stats host, no Info API weight)
HL_WALLETS_FILE=${WALLETS_FILE}
HL_TRACKED_SOURCE=leaderboard
HL_WHALE_TOP_N=100
HL_SMART_TOP_N=50
# hourly cadence: start no new wallet after 35 min (service timeout 50), defer the rest
HL_RUN_SOFT_DEADLINE_MIN=35
HL_LOG_LEVEL=INFO
HL_LOG_JSON=1
HL_LOCK_STALE_HOURS=2
# web monitor (hypercore-web.service): http://<this host>:8787/ - no authentication, LAN only
HL_WEB_BIND=0.0.0.0
HL_WEB_PORT=8787

# Iceberg: pyiceberg SQL catalog in PostgreSQL, data as plain files on the NAS
HL_CATALOG_URI=postgresql+psycopg2://${PG_ROLE}:${PG_PASS}@localhost:5432/${PG_DB}
WAREHOUSE=file://${WAREHOUSE_DIR}
HL_WAREHOUSE_MOUNT=${NAS_MOUNT}

# S3 node-data backfill is OFF in this layout (no AWS key). To enable: scoped IAM key
# here + add --s3-backfill / --s3 back in /etc/systemd/system/hypercore-daily.service.d/local.conf
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_REGION=us-east-1
EOF
else
  warn "$ENV_FILE exists - values left untouched (keys new in this release are appended below)"
fi
sudo chown root:"$SERVICE_USER" "$ENV_FILE"
sudo chmod 640 "$ENV_FILE"
# upgrade path: a key this release reads but an older env file lacks is appended with its
# default; a key that is present (even empty) is never changed
ensure_env() {  # ensure_env KEY VALUE COMMENT
  if ! sudo grep -q "^$1=" "$ENV_FILE"; then
    printf '\n# %s\n%s=%s\n' "$3" "$1" "$2" | sudo tee -a "$ENV_FILE" >/dev/null
    echo "added $1=$2"
  fi
}
ensure_env HL_TRACKED_SOURCE leaderboard "v1.1: automatic wallet list (top whales + smart money from the leaderboard) in addition to the pins"
ensure_env HL_WHALE_TOP_N 100 "v1.1: tracked whales, top N by account value"
ensure_env HL_SMART_TOP_N 50 "v1.1: tracked smart money, top N by 30d pnl"
ensure_env HL_RUN_SOFT_DEADLINE_MIN 35 "v1.1: hourly cadence - start no new wallet after 35 min (service timeout 50), defer the rest"
ensure_env HL_WEB_BIND 0.0.0.0 "v1.3: web monitor address (no authentication: LAN/VPN only)"
ensure_env HL_WEB_PORT 8787 "v1.3: web monitor port"
ensure_env HL_PRICE_TIME_BUDGET_MIN 3 "v1.7: minutes per run (first) for native 1h/1d candles -> l3_token_price_usd_1h/_1d (ETL token_price_usd_1h/1d); 0 = off"
ensure_env HL_PRICE_HISTORY_DAYS 32 "v1.7: candle history fetched on a token's first visit (the ETL loads 32 days)"
ensure_env HL_ROLLUPS 1 "v1.9: after every lift, recompute the wallet-level L4 rollup tables of the ETL contract (0 = off)"
ensure_env HL_ROLLUP_TIME_BUDGET_MIN 8 "v1.9: minutes per run for the rollup step; later tables are skipped + reported and caught up next run"
ensure_env HL_PRICE_HISTORY_DAYS_1D 820 "v1.10: 1d candle history per token (the market fear index calibrates against 730 trailing days)"
ensure_env HL_PRICE_1M_DAYS 3 "v1.10: 1-minute candles into l3_token_price_usd_1m for the token views, market signals and token quality (0 = off)"
ensure_env HL_SUPPLY_REFRESH_HOURS 24 "v1.10: hours between two Info tokenDetails calls per token (total supply, deploy time)"
# keys of features removed in v1.3 (webhook / heartbeat alerts): drop them so preflight
# does not warn about variables nothing reads
if sudo grep -qE '^(HL_ALERT_WEBHOOK_URL|HL_ALERT_ON_SUCCESS|HL_ALERT_REPEAT_HOURS|HL_HEARTBEAT_URL)=' "$ENV_FILE"; then
  sudo sed -i -E '/^(HL_ALERT_WEBHOOK_URL|HL_ALERT_ON_SUCCESS|HL_ALERT_REPEAT_HOURS|HL_HEARTBEAT_URL)=/d' "$ENV_FILE"
  echo "removed the HL_ALERT_* / HL_HEARTBEAT_URL keys (alerts were removed in v1.3)"
fi
if ! sudo test -f "$WALLETS_FILE"; then
  sudo install -m 640 -o "$ME" -g "$SERVICE_USER" /dev/null "$WALLETS_FILE"
  sudo tee "$WALLETS_FILE" >/dev/null <<'EOF'
# PINNED wallets: one Hyperliquid address per line (0x + 40 hex), # comments allowed.
# Optional: the run also pulls the leaderboard tracked set (HL_TRACKED_SOURCE in
# hypercore.env). Pins are always pulled first and their API-capped windows are hard
# problems; put customer / watchlist wallets here.
EOF
fi
# the wallet list is yours to edit without sudo; the service only reads it
sudo chown "$ME:$SERVICE_USER" "$WALLETS_FILE"
sudo chmod 640 "$WALLETS_FILE"

# ----------------------------------------------------------------------------- 7. NAS warehouse dir
say "warehouse ${WAREHOUSE_DIR} (as ${SERVICE_USER})"
sudo mkdir -p "$NAS_BASE" 2>/dev/null || warn "root could not mkdir ${NAS_BASE} on the NAS (root squash) - the service-user write test below decides"
sudo chown "$SERVICE_USER:$SERVICE_USER" "$NAS_BASE" 2>/dev/null || warn "chown on the NAS was refused (uid squash?) - fix the Synology NFS rule if the write test below fails"
if sudo -u "$SERVICE_USER" sh -c "mkdir -p '$WAREHOUSE_DIR' && touch '$WAREHOUSE_DIR/.w' && rm '$WAREHOUSE_DIR/.w'"; then
  echo "write test OK: $(ls -ldn "$WAREHOUSE_DIR")"
else
  die "service user cannot write ${WAREHOUSE_DIR} - Synology: Control Panel > Shared Folder > NFS Permissions, rule for this host: Squash = 'No mapping' (then uid $(id -u "$SERVICE_USER") must own ${NAS_BASE}) or 'Map all users to admin'; never chmod -R 777"
fi

# ----------------------------------------------------------------------------- 8. systemd
say "systemd units"
for unit in hypercore-daily.service hypercore-hourly.timer hypercore-maintenance.service hypercore-maintenance.timer hypercore-web.service; do
  # the shipped units hard-code /mnt/blueblocx (RequiresMountsFor, ReadWritePaths): follow $NAS_MOUNT
  sed "s#/mnt/blueblocx#${NAS_MOUNT}#g" "$PROD_DIR/deploy/$unit" | sudo tee "/etc/systemd/system/$unit" >/dev/null
  sudo chmod 644 "/etc/systemd/system/$unit"
done
# removed in v1.3 (the OnFailure notifier): make sure no stale copy is left behind
sudo rm -f /etc/systemd/system/hypercore-failure@.service
# `hypercore-status` for everyone in the hypercore group: running / stuck / last verdict;
# `hypercore-sql`: read-only SQL over the lake (pyiceberg -> DuckDB, writes nothing)
sudo install -m 0755 "$PROD_DIR/deploy/hypercore-status" /usr/local/bin/hypercore-status
sudo install -m 0755 "$PROD_DIR/deploy/hypercore-sql" /usr/local/bin/hypercore-sql
sudo mkdir -p /etc/systemd/system/hypercore-daily.service.d
sudo tee /etc/systemd/system/hypercore-daily.service.d/local.conf >/dev/null <<EOF
# written by deploy/bootstrap.sh - standalone layout: no S3 backfill (no AWS key),
# hourly cadence -> a run must finish inside the hour
[Service]
Environment=HL_ENV_FILE=${ENV_FILE}
ExecStartPre=
ExecStartPre=${PROD_DIR}/.venv/bin/python -m blueblocx_hyperliquid.preflight --iceberg --wallets-file ${WALLETS_FILE}
ExecStart=
ExecStart=${PROD_DIR}/.venv/bin/python -m blueblocx_hyperliquid.daily --wallets-file ${WALLETS_FILE} --iceberg
TimeoutStartSec=50min
EOF
sudo systemctl daemon-reload
# the web monitor is read-only and safe to (re)start now; a restart picks up the new code
sudo systemctl enable --now hypercore-web.service >/dev/null
sudo systemctl restart hypercore-web.service

# ----------------------------------------------------------------------------- 8b. lake schema migrations
say "lake schema migrations (as ${SERVICE_USER})"
# The only way a production table's schema changes (docs/DEPLOY.md section 8): idempotent,
# under the shared .daily.lock, one schema commit + one backfill commit per migration, and
# the staged CSV rewritten where a release changed a column type. The lift and preflight
# refuse a drifted table, so a failure here is fatal: production must not run on it.
# Rollback of such a release: `migrate --revert --apply` from the NEW checkout first, then
# bootstrap with the previous tag.
if ( cd "$PROD_DIR" && sudo -u "$SERVICE_USER" env HL_ENV_FILE="$ENV_FILE" "$PROD_DIR/.venv/bin/python" -m blueblocx_hyperliquid.migrate --apply ); then
  echo "migrations: up to date"
else
  rc=$?
  [ "$rc" -eq 3 ] && die "migrate: another run holds the state-dir lock (${STATE_DIR:-HL_OUT_DIR}/hypercore/.daily.lock) - stop it and re-run bootstrap"
  die "migrate --apply failed (exit $rc) - see the output above; production must not run on a drifted schema"
fi

# ----------------------------------------------------------------------------- 9. preflight
say "preflight (as ${SERVICE_USER})"
# from $PROD_DIR: the package is imported from the checkout, not installed into the venv
if ( cd "$PROD_DIR" && sudo -u "$SERVICE_USER" env HL_ENV_FILE="$ENV_FILE" "$PROD_DIR/.venv/bin/python" -m blueblocx_hyperliquid.preflight --iceberg --wallets-file "$WALLETS_FILE" ); then
  echo "preflight OK"
else
  warn "preflight reported FAIL rows - fix them before enabling the timers"
fi

# what the service will actually use (an existing env file keeps its own values)
env_val() { sudo sed -n "s/^$1=//p" "$ENV_FILE" | tail -1; }
TRACKED_SRC="$(env_val HL_TRACKED_SOURCE)"
WHALE_N="$(env_val HL_WHALE_TOP_N)"
SMART_N="$(env_val HL_SMART_TOP_N)"
STUCK_MIN="$(env_val HL_STUCK_MINUTES)"
WEB_PORT="$(env_val HL_WEB_PORT)"
HOST_IP="$(hostname -I 2>/dev/null | awk '{print $1}')"

say "done: ${PROD_DIR} @ $(git -C "$PROD_DIR" describe --tags --always). next steps"
cat <<EOF
Wallet list (HL_TRACKED_SOURCE=${TRACKED_SRC:-unset}): top ${WHALE_N:-?} whales + up to ${SMART_N:-?} smart-money
wallets from the Hyperliquid leaderboard (same rule as the live platform-backend), refreshed every run.
Web monitor:               http://${HOST_IP:-<this-host>}:${WEB_PORT:-8787}/    (running / stuck / last run; LAN only, no login)
1. Optional pins:          nano ${WALLETS_FILE}        (customer / watchlist wallets, one 0x... per line)
2. First run by hand:      sudo systemctl start --no-block hypercore-daily.service
   watch it:               the web monitor, or hypercore-status   (STUCK after ${STUCK_MIN:-15} quiet minutes)
   then:                   sudo jq '{ok,wallets_ok,wallets_deferred,tracked,problems,warnings}' ${STATE_DIR}/hypercore/_run_summary.json
3. Enable the schedule:    sudo systemctl enable --now hypercore-hourly.timer hypercore-maintenance.timer
4. Any time:               the web monitor, or hypercore-status (exit 0 = healthy; --json for scripts)
5. Query the lake (SQL):   hypercore-sql --list;  hypercore-sql "select event_type, count(*) from l1_wallet_ledger group by 1"
6. ETL / DuckDB readers:   READER_CIDR=<etl host ip>/32 bash deploy/bootstrap.sh creates the read-only role and
                           ${READER_ENV}; smoke test: HL_ENV_FILE=${READER_ENV} <venv with duckdb>/bin/python -m blueblocx_hyperliquid.duckdb_smoke
Note: your group membership (${SERVICE_USER}) applies after you log in again (hypercore-status and hypercore-sql need it).
EOF
