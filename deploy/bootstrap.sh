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
#      S3 backfill OFF), /etc/hypercore/wallets.txt placeholder
#   7. NAS warehouse directory, written as the service user (catches uid squash)
#   8. systemd units: service + hourly timer + nightly maintenance, with a drop-in
#      that removes the S3 flags (no AWS key in this layout) and caps a run at 50 min
#   9. preflight as the service user
# It does NOT start the timers or run the pipeline: add wallets first (printed at the end).
# Overrides: export RELEASE_TAG / GIT_ALIAS / NAS_MOUNT / WEIGHT_BUDGET before running.
set -euo pipefail

# ----------------------------------------------------------------------------- settings
RELEASE_TAG="${RELEASE_TAG:-v1.0.1}"
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
ME="$(id -un)"
WORKDIR="$(cd "$(dirname "$0")/.." && pwd)"
TEST_LOG="/tmp/hypercore-bootstrap-tests.log"

say()  { printf '\n\033[1;34m== %s\033[0m\n' "$*"; }
warn() { printf '\033[1;33m!! %s\033[0m\n' "$*"; }
die()  { printf '\033[1;31mxx %s\033[0m\n' "$*" >&2; exit 1; }

# ----------------------------------------------------------------------------- preconditions
[ "$ME" != "root" ] || die "run as your login user (with sudo), not as root"
sudo -v
mountpoint -q "$NAS_MOUNT" || die "$NAS_MOUNT is not a mount point - mount the NAS first (fstab: vers=4.1,hard,rsize=1048576,wsize=1048576,noatime,_netdev)"
# this script rewrites $PROD_DIR: refuse while anything could execute it
for unit in hypercore-daily.service hypercore-maintenance.service hypercore-hourly.timer hypercore-daily.timer hypercore-maintenance.timer; do
  if systemctl is-active --quiet "$unit"; then
    die "$unit is active - stop the timers first and wait for the services to finish: sudo systemctl stop hypercore-hourly.timer hypercore-daily.timer hypercore-maintenance.timer && while systemctl is-active --quiet hypercore-daily.service; do sleep 10; done  (upgrades: docs/DEPLOY.md section 8)"
  fi
done
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
[ -x "$PROD_DIR/.venv/bin/python" ] || python3 -m venv "$PROD_DIR/.venv"
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
[ -x "$WORKDIR/.venv/bin/python" ] || python3 -m venv "$WORKDIR/.venv"
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
HL_WALLETS_FILE=${WALLETS_FILE}
HL_RETENTION_DAYS=45
HL_LOG_LEVEL=INFO
HL_LOG_JSON=1
HL_LOCK_STALE_HOURS=2
HL_ALERT_WEBHOOK_URL=
HL_ALERT_ON_SUCCESS=0
HL_ALERT_REPEAT_HOURS=6

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
  warn "$ENV_FILE exists - left untouched"
fi
sudo chown root:"$SERVICE_USER" "$ENV_FILE"
sudo chmod 640 "$ENV_FILE"
if ! sudo test -f "$WALLETS_FILE"; then
  sudo install -m 640 -o "$ME" -g "$SERVICE_USER" /dev/null "$WALLETS_FILE"
  sudo tee "$WALLETS_FILE" >/dev/null <<'EOF'
# one Hyperliquid wallet address per line (0x + 40 hex); # comments allowed
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
for unit in hypercore-daily.service hypercore-hourly.timer hypercore-maintenance.service hypercore-maintenance.timer; do
  # the shipped units hard-code /mnt/blueblocx (RequiresMountsFor, ReadWritePaths): follow $NAS_MOUNT
  sed "s#/mnt/blueblocx#${NAS_MOUNT}#g" "$PROD_DIR/deploy/$unit" | sudo tee "/etc/systemd/system/$unit" >/dev/null
  sudo chmod 644 "/etc/systemd/system/$unit"
done
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

# ----------------------------------------------------------------------------- 9. preflight
say "preflight (as ${SERVICE_USER})"
# from $PROD_DIR: the package is imported from the checkout, not installed into the venv
if ( cd "$PROD_DIR" && sudo -u "$SERVICE_USER" env HL_ENV_FILE="$ENV_FILE" "$PROD_DIR/.venv/bin/python" -m blueblocx_hyperliquid.preflight --iceberg --wallets-file "$WALLETS_FILE" ); then
  echo "preflight OK ('WARN wallets ... empty' is expected until you add wallets)"
else
  warn "preflight reported FAIL rows - fix them before enabling the timers"
fi

say "done: ${PROD_DIR} @ $(git -C "$PROD_DIR" describe --tags --always). next steps"
cat <<EOF
1. Add wallets:            nano ${WALLETS_FILE}        (one 0x... per line; the file is yours)
2. Optional webhook:       sudoedit ${ENV_FILE}   -> HL_ALERT_WEBHOOK_URL=
3. Small first run (2-3 wallets, docs/DEPLOY.md section 5):
     cd ${PROD_DIR} && sudo -u ${SERVICE_USER} env HL_ENV_FILE=${ENV_FILE} .venv/bin/python -m blueblocx_hyperliquid.extract --wallet 0xA,0xB --days 2 --incremental --iceberg
   then read ${STATE_DIR}/hypercore/_reconciliation.json (natural_key_ok, fidelity_ok, oracle.*)
4. Full run once by hand:  sudo systemctl start --no-block hypercore-daily.service && sudo journalctl -u hypercore-daily -f
   then:                   sudo jq '{ok,wallets_ok,problems,warnings}' ${STATE_DIR}/hypercore/_run_summary.json
5. Enable the schedule:    sudo systemctl enable --now hypercore-hourly.timer hypercore-maintenance.timer
6. Check:                  systemctl list-timers 'hypercore-*'
Note: your group membership (${SERVICE_USER}) applies after you log in again.
EOF
