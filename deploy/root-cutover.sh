#!/usr/bin/env bash
# One-time ROOT cutover to sudo-less operation (docs/runbooks/2026-11-23_sudo-less-operation.md).
# Run on server4090-2 as root:  sudo bash /home/hatsadin/blueblocx-hyperliquid/deploy/root-cutover.sh
set -euo pipefail
echo "1/5 stop the system timers + web monitor"
systemctl disable --now hypercore-hourly.timer hypercore-maintenance.timer hypercore-web.service
echo "2/5 wait for the running extract (max 50 min)"
while systemctl is-active --quiet hypercore-daily.service; do sleep 10; done
echo "3/5 state dir -> hatsadin"
chown -R hatsadin:hatsadin /var/lib/hypercore
echo "4/5 user systemd runs without a login"
loginctl enable-linger hatsadin
echo "5/5 as hatsadin: production code -> v1.11.0, user timers on"
sudo -u hatsadin -H env XDG_RUNTIME_DIR=/run/user/1100 bash -c '
  git -C /opt/blueblocx-hyperliquid fetch -q --tags origin
  git -C /opt/blueblocx-hyperliquid checkout -q --detach v1.11.0
  systemctl --user daemon-reload
  systemctl --user enable --now hypercore-web.service hypercore-hourly.timer hypercore-maintenance.timer
  systemctl --user list-timers --all "hypercore-*" --no-pager'
echo "done"
