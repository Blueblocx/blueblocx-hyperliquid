# Runbook: move the pipeline to sudo-less operation (user-level systemd)

Why: the login user is losing sudo. The pipeline keeps systemd (timers, 50-minute cap, MemoryMax,
journal) but runs as the login user `hatsadin` under `systemctl --user`, so the day-to-day release
flow (`deploy/bootstrap-user.sh`) needs no root. Root is needed exactly once, for the cutover below.

## Already prepared as the user (no root)
- `~/.config/hypercore/hypercore.env` (copy of /etc/hypercore/hypercore.env, mode 600, `HL_WALLETS_FILE`
  points at `~/.config/hypercore/wallets.txt`) and `~/.config/hypercore/wallets.txt`
- user units in `~/.config/systemd/user/` = `deploy/user/*` (verified with `systemd-analyze --user verify`)
- `~/.local/bin/hypercore-sql`, `~/.local/bin/hypercore-status` (same wrappers, user env file)

## Cutover (ROOT, once; run between timer ticks - :02 to :10 UTC - or let step 1 wait)
```bash
set -e
U=hatsadin
# 1. stop the system timers and wait for the running extract to finish
systemctl disable --now hypercore-hourly.timer hypercore-maintenance.timer
while systemctl is-active --quiet hypercore-daily.service; do sleep 10; done
systemctl disable --now hypercore-web.service
systemctl stop hypercore-daily.service hypercore-maintenance.service 2>/dev/null || true
# 2. the state dir (runs, locks, staged CSVs) becomes the user's
chown -R "$U:$U" /var/lib/hypercore
# 3. the user's systemd instance must run without a login session
loginctl enable-linger "$U"
systemctl start "user@$(id -u "$U").service"
# 4. as the user: production code -> v1.11.0, start the user units
sudo -u "$U" -H bash -lc '
  set -e
  export XDG_RUNTIME_DIR=/run/user/$(id -u)
  cd /opt/blueblocx-hyperliquid && git fetch -q --tags origin && git checkout -q --detach v1.11.0
  systemctl --user daemon-reload
  systemctl --user enable --now hypercore-web.service hypercore-hourly.timer hypercore-maintenance.timer
  systemctl --user list-timers --all "hypercore-*" --no-pager
'
# 5. retire the root-owned units (config dir may stay read-only for reference)
rm -f /etc/systemd/system/hypercore-daily.service /etc/systemd/system/hypercore-maintenance.service \
      /etc/systemd/system/hypercore-web.service /etc/systemd/system/hypercore-hourly.timer \
      /etc/systemd/system/hypercore-maintenance.timer
rm -rf /etc/systemd/system/hypercore-daily.service.d /etc/systemd/system.control/hypercore-daily.service.d
systemctl daemon-reload
```

## Verify (as the user)
- `systemctl --user list-timers --all 'hypercore-*'` shows the hourly timer with a next elapse at :10 UTC
- after the first run: `journalctl --user -u hypercore-daily -n 50`, `hypercore-status`,
  `hypercore-sql "select count(*) from l4x_perp_position_snap"` (v1.11.0: the heatmap tables appear on the first lift)
- web monitor still answers on port 8787 (`systemctl --user status hypercore-web`)

## Rollback (root)
Reverse the cutover: `loginctl disable-linger`, re-install `deploy/hypercore-*.service|timer` under
/etc/systemd/system, `chown -R hypercore:hypercore /var/lib/hypercore`, `systemctl enable --now hypercore-hourly.timer`.
Code rollback stays user-level: `git -C /opt/blueblocx-hyperliquid checkout --detach v1.10.14`.

## From now on (no root)
- Release: `RELEASE_TAG=vX.Y.Z bash deploy/bootstrap-user.sh` (stops the user timers, waits, checkout,
  pip, tests, migrate --apply, preflight, unit reload, timers back on)
- Operate: `systemctl --user stop|start hypercore-hourly.timer`, `journalctl --user -u hypercore-daily -f`
- Still root-only (rare): apt packages, the local PostgreSQL service, NFS mount, limits on other users' processes
