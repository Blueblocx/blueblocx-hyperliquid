# Working on this repo (read first)

This is the production pipeline `blueblocx-hyperliquid` (Hyperliquid HyperCore ->
Iceberg). On the production host **server4090-2** the same code exists twice, and
the distinction is the whole safety model:

| path | what it is | you may |
|---|---|---|
| `~/blueblocx-hyperliquid` (this working copy, branch `main`) | development: edit, test, commit, push, tag | everything |
| `/opt/blueblocx-hyperliquid` | **production checkout**, detached at a release tag; the hourly timer executes whatever is on disk here | read only. Never edit, never `git pull`, never run pip in it by hand. It moves only via the release flow below |
| `/etc/hypercore/hypercore.env` | production config, root:hypercore 640, contains the catalog password | read (group); edit only with `sudoedit`; **never print or paste its values** |
| `/etc/hypercore/wallets.txt` | optional pinned wallets (owned by the login user) | edit |
| `/etc/hypercore/reader.env` | read-only catalog credentials for the ETL's DuckDB reader (`hl_ro`), written by `READER_CIDR=… bash deploy/bootstrap.sh`; mode 600, owned by the login user | read; **never print or paste its values** |
| `/var/lib/hypercore/hypercore/` | production state: `_run_summary.json`, `_run_progress.json`, `_run_history.jsonl`, `_watermarks.json`, `_tracked_wallets.json`, staged CSVs | read; never write or delete by hand |
| `/mnt/blueblocx/hypercore/warehouse` | the Iceberg warehouse on the NAS | never touch by hand (the catalog in PostgreSQL points at these files) |

## Observing production

- Web monitor: `http://<host ip>:8787/` (read-only page; `/api/status`, `/healthz`).
- CLI with the same verdict: `hypercore-status` (`--json`; exit 0 = healthy).
- Logs: `sudo journalctl -u hypercore-daily -f`; units/timers: `systemctl list-timers --all 'hypercore-*'`.
- Last verdict: `sudo jq '{ok,wallets_ok,wallets_deferred,tracked,problems,warnings}' /var/lib/hypercore/hypercore/_run_summary.json`.

## Developing here (never against production state)

- Use this checkout's own venv: `.venv/bin/python` (created by `deploy/bootstrap.sh`).
- For any run from the working copy, point the pipeline at a scratch state dir and the
  local SQLite catalog: create `.env` here (gitignored) with at least
  `HL_OUT_DIR=/home/hatsadin/hc-dev`, `HL_TRACKED_SOURCE=` (empty, or a small wallets
  file via `--wallets-file`), and **no** `HL_CATALOG_URI` / `WAREHOUSE` / `HL_WAREHOUSE_MOUNT`.
  Never run `daily` or `extract --iceberg` with `HL_ENV_FILE=/etc/hypercore/hypercore.env`
  from here: it would write into the production state dir and catalog.
- Tests: `.venv/bin/python -m unittest discover -s tests -t .` (must pass; no network,
  no systemd). Offline health check: `.venv/bin/python -m blueblocx_hyperliquid.preflight --offline`.
- `blueblocx_hyperliquid/config.py::ENV_VARS` is the authoritative env list; preflight
  warns about any `HL_*` variable it does not know. Document a new variable there,
  in `deploy/.env.example`, and (if the host needs it) via `ensure_env` in `deploy/bootstrap.sh`.
- Never commit `.env`, `key`, `wallets.txt` (gitignored) and never put secret values
  in code, docs, commit messages or logs.
- Line endings are LF (`.gitattributes`); shell scripts need the exec bit in git
  (`git add --chmod=+x`).

## Release flow (the only way production changes)

```bash
# in ~/blueblocx-hyperliquid, after tests pass
# bump the version in: pyproject.toml, blueblocx_hyperliquid/__init__.py,
#   deploy/bootstrap.sh (RELEASE_TAG default), docs/DEPLOY.md (git clone --branch line)
git add -A && git commit -m "..." && git push
git tag -a vX.Y.Z -m "..." && git push origin vX.Y.Z

# move production between timer ticks (bootstrap refuses while a timer, a run or a hand-run
# pipeline process is active; a hand run holds .daily.lock and is invisible to systemctl)
sudo systemctl stop hypercore-hourly.timer hypercore-maintenance.timer
while systemctl is-active --quiet hypercore-daily.service; do sleep 10; done
pgrep -af 'blueblocx_hyperliquid\.' ; sudo cat /var/lib/hypercore/hypercore/.daily.lock 2>/dev/null   # nothing live
# when the release notes (docs/DEPLOY.md section 8) list a state-file change: tar HL_OUT_DIR first
RELEASE_TAG=vX.Y.Z bash deploy/bootstrap.sh        # checkout, pip, tests, units, env keys, web monitor restart, preflight
sudo systemctl enable --now hypercore-hourly.timer hypercore-maintenance.timer
```

Rollback is the same with the previous tag (always via bootstrap when the release
changed `deploy/`; a release that changed the lake schema says in its release note
what must run before the tag moves back). `docs/DEPLOY.md` has the long form and
the reasoning; `README.md` the module map; `docs/ETL_HANDOFF_HYPERCORE.md` is what
the ETL team reads.

## Decisions already made (do not reopen without the owner)

- The wallet list is automatic (`HL_TRACKED_SOURCE=leaderboard`: the TS
  platform-backend's whale/smart-money sets) with optional pins; no hand-typed lists.
- Monitoring is the web monitor + `hypercore-status`. **No push alerting** (no Discord,
  Slack, healthchecks, email): the owner removed it deliberately.
- No Dagster or other scheduler daemon on this host; systemd is the controller.
- Hourly cadence with the 35-minute soft deadline and the 50-minute unit timeout.
