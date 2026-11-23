#!/usr/bin/env bash
# Sudo-less release flow (2026-11-23): moves the production checkout /opt/blueblocx-hyperliquid
# (owned by the operating user) to $RELEASE_TAG and reloads the user-level systemd units.
# Prerequisites (done once with root, docs/runbooks/2026-11-23_sudo-less-operation.md): the state dir
# belongs to this user, `loginctl enable-linger` is set, the system units are gone.
# Usage, between timer ticks:  RELEASE_TAG=vX.Y.Z bash deploy/bootstrap-user.sh
set -euo pipefail
RELEASE_TAG="${RELEASE_TAG:-v1.11.0}"
PROD_DIR="${PROD_DIR:-/opt/blueblocx-hyperliquid}"
export XDG_RUNTIME_DIR="${XDG_RUNTIME_DIR:-/run/user/$(id -u)}"
say() { printf '\n== %s\n' "$*"; }
die() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }
[ -d "$PROD_DIR/.git" ] || die "$PROD_DIR is not a git checkout"
[ -w "$PROD_DIR" ] || die "$PROD_DIR is not writable by $(id -un)"
say "stop the timers and wait for a running extract"
systemctl --user stop hypercore-hourly.timer hypercore-maintenance.timer 2>/dev/null || true
while systemctl --user is-active --quiet hypercore-daily.service; do sleep 10; done
pgrep -af 'blueblocx_hyperliquid\.(daily|extract|maintenance)' && die "a hand-run pipeline process is still live" || true
say "checkout $RELEASE_TAG"
git -C "$PROD_DIR" fetch --quiet --tags origin
git -C "$PROD_DIR" checkout --detach --quiet "$RELEASE_TAG"
say "dependencies + tests in the production venv"
"$PROD_DIR/.venv/bin/pip" install --quiet -r "$PROD_DIR/requirements.txt" -c "$PROD_DIR/constraints.txt"
( cd "$PROD_DIR" && .venv/bin/python -m unittest discover -s tests -t . 2>&1 | tail -3 )
say "lake schema migrations (idempotent) + preflight"
( cd "$PROD_DIR" && HL_ENV_FILE="$HOME/.config/hypercore/hypercore.env" .venv/bin/python -m blueblocx_hyperliquid.migrate --apply )
( cd "$PROD_DIR" && HL_ENV_FILE="$HOME/.config/hypercore/hypercore.env" .venv/bin/python -m blueblocx_hyperliquid.preflight --iceberg --wallets-file "$HOME/.config/hypercore/wallets.txt" )
say "user units"
install -m 644 "$PROD_DIR"/deploy/user/hypercore-*.service "$PROD_DIR"/deploy/user/hypercore-*.timer "$HOME/.config/systemd/user/"
systemctl --user daemon-reload
systemctl --user restart hypercore-web.service
systemctl --user enable --now hypercore-hourly.timer hypercore-maintenance.timer
systemctl --user list-timers --all 'hypercore-*' --no-pager
say "done: $PROD_DIR @ $(git -C "$PROD_DIR" describe --tags --always)"
