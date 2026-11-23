#!/usr/bin/env bash
# One-time ROOT part of the switch to sudo-less operation (docs/runbooks/2026-11-23_sudo-less-operation.md).
# Run on server4090-2:  sudo bash /home/hatsadin/blueblocx-hyperliquid/deploy/root-cutover.sh
# The user-side steps (checkout v1.11.0 in /opt, systemctl --user enable the units) are done by hatsadin afterwards.
set -euo pipefail
systemctl disable --now hypercore-hourly.timer hypercore-maintenance.timer hypercore-web.service
while [ "$(systemctl is-active hypercore-daily.service)" != inactive ]; do sleep 10; done   # a running oneshot reports 'activating'
chown -R hatsadin:hatsadin /var/lib/hypercore
loginctl enable-linger hatsadin
