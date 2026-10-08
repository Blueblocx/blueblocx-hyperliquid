#!/usr/bin/env bash
# cron-friendly wrapper around `python -m hl_poc.daily` (use the systemd units in
# this directory when the host has systemd; this script is for plain cron hosts).
#
#   20 0 * * *  hypercore  /opt/hyperliquid-poc/deploy/run_daily.sh >> /var/log/hypercore/daily.log 2>&1
#
# - loads the env file (default /etc/hypercore/hypercore.env, override with HL_ENV_FILE)
# - serialises runs with flock so an overrun never overlaps the next start
# - exit code is the runner's: 0 clean, 1 problems/error, 2 bad config, 3 lock held
set -euo pipefail

REPO="${HL_REPO:-/opt/hyperliquid-poc}"
PY="${HL_PYTHON:-$REPO/.venv/bin/python}"
ENV_FILE="${HL_ENV_FILE:-/etc/hypercore/hypercore.env}"
WALLETS="${HL_WALLETS_FILE:-/etc/hypercore/wallets.txt}"
LOCK="${HL_CRON_LOCK:-/var/lib/hypercore/.cron.lock}"

cd "$REPO"
export HL_ENV_FILE="$ENV_FILE"

exec flock -n "$LOCK" \
  "$PY" -m hl_poc.daily --wallets-file "$WALLETS" --iceberg --s3-backfill "$@"
