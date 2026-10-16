# Runbook: move production to v1.8.0 (October 2026)

**Operator:** the AI assistant (Claude Code) or a human, logged in on `server4090-2` as
`hatsadin`, working in `~/blueblocx-hyperliquid`. **Supervisor:** the owner, who reads
the report at the end. Follow the steps in order. Every line marked **STOP** is a
point where you stop, report what you see, and wait for the owner instead of
improvising.

The steps are idempotent: a step that already happened (a key already in the env
file, a migration already applied, a timer already stopped) is harmless to repeat.

> **2026-10-16:** the first attempt stopped in step 7: the v1.8.0 test suite fails inside
> `/opt/blueblocx-hyperliquid` (4 `RunLock` tests; the lock's recycled-PID check mistook the test
> process for an unrelated one) and `bootstrap.sh` refused. v1.8.1 fixes the lock check and
> changes nothing else about the upgrade: read every `v1.8.0` below as **`v1.8.1`**
> (`RELEASE_TAG=v1.8.1`, log `~/bootstrap-v1.8.1-$STAMP.log`, monitor shows `version 1.8.1`).

## 0. Rules that override everything below (from CLAUDE.md)

- Never edit, `git pull`, or run `pip` inside `/opt/blueblocx-hyperliquid`. It moves
  only through `deploy/bootstrap.sh` (step 6).
- Never print, `cat`, `grep` or paste the contents of `/etc/hypercore/hypercore.env`
  or `/etc/hypercore/reader.env`: they hold passwords. `ls -l` on them is fine.
- Never write or delete anything under `/var/lib/hypercore/` or
  `/mnt/blueblocx/hypercore/warehouse` by hand. The only state-changing commands you
  run are the ones written in this runbook, exactly as written.
- Never run `daily`, `extract`, `iceberg_load`, `maintenance` or `migrate` from
  `~/blueblocx-hyperliquid` with `HL_ENV_FILE=/etc/hypercore/hypercore.env`.
  Production code runs from `/opt/...` as user `hypercore`; the commands below do
  that explicitly.
- **No placeholders are typed literally.** `ETL_HOST_IP` is a shell variable you set
  in step 2. Never type angle brackets: `<ETL-host-ip>` would be a shell redirection
  (that is the `No such file or directory` error).
- Do not stop the timers twice or start the service twice: read the output of each
  command before running the next.
- If a command fails in a way this runbook does not describe: stop, keep the output,
  report. Do not retry a state-changing step blindly.

## 1. What this upgrade changes (so you can judge the output)

Production is expected at **v1.3.0**; the target is **v1.8.0** in one bootstrap. The
bootstrap of v1.8.0 is cumulative (every step of the skipped releases is included):

| release | effect on the host |
|---|---|
| v1.4.0 | replaced tables rewritten in one commit; guards; optional read-only PostgreSQL role `hl_ro` + `/etc/hypercore/reader.env` for the ETL's DuckDB (only when `READER_CIDR` is given) |
| v1.5.0 | lake schema migration: `l4_wallet_metadata.first_tx` / `latest_tx` STRING → TIMESTAMPTZ (bootstrap runs `migrate --apply`); staged `l4_wallet_metadata.csv` rewritten |
| v1.6.0 | `_watermarks.json` gains a `meta` key per wallet; dimension rows keep their stamps when unchanged; **one-time** `maintenance --fix-first-seen` (step 7) |
| v1.7.0 | two new tables `l3_token_price_usd_1h` / `_1d`, new state file `_price_state.json`, two new env keys `HL_PRICE_TIME_BUDGET_MIN=3` / `HL_PRICE_HISTORY_DAYS=32` (bootstrap appends them) |
| v1.8.0 | `hypercore-maintenance.service` gains `--compact` (nightly compaction) |

Expected after the upgrade: the web monitor shows `version 1.8.0`; the first run
re-stamps every wallet's `l4_wallet_metadata` row once; for a few hours the run
summary carries the warnings `prices_history_pending=N` / `prices_deferred=N` while
the price step fetches 32 days of candles for every token. These are not problems.

## 2. Inputs and preconditions

Set the one input. The ETL host's IPv4 address is needed only to create the reader
role; if you do not know it, leave it empty and the role is created later by
re-running step 6 with the variable set (that re-run is safe).

```bash
ETL_HOST_IP=""        # e.g. ETL_HOST_IP=10.0.0.42   (dotted IPv4, no slash, no brackets)
STAMP=$(date -u +%Y%m%dT%H%M)
if [ -n "$ETL_HOST_IP" ] && [[ "$ETL_HOST_IP" =~ ^([0-9]{1,3}\.){3}[0-9]{1,3}$ ]]; then echo "reader role for ${ETL_HOST_IP}/32"; else echo "no ETL ip: reader role skipped"; ETL_HOST_IP=""; fi
```

The bootstrap and several commands use `sudo`. Check once whether sudo works without
a prompt in your shell:

```bash
sudo -n true && echo "sudo ok" || echo "sudo needs a password in this shell"
```

**STOP** if it needs a password and you cannot enter one: ask the owner to run
`sudo -v` in your shell (or to run steps 4 to 9 themselves in an interactive
terminal). Do not try to work around sudo.

Other preconditions the bootstrap checks itself and refuses otherwise: the NAS is
mounted at `/mnt/blueblocx`; no `hypercore-*` unit is active; no hand-run pipeline
process; ssh to GitHub works with the deploy key (`ssh -T git@github-blueblocx-hyperliquid`).

## 3. Record the starting state (read-only)

```bash
cd ~/blueblocx-hyperliquid
git -C /opt/blueblocx-hyperliquid describe --tags --always
hypercore-status || true
systemctl list-timers --all 'hypercore-*'
sudo jq '{version,ok,wallets_ok,wallets_deferred,problems,warnings,run_at}' /var/lib/hypercore/hypercore/_run_summary.json
```

Expected: `/opt` at `v1.3.0` (if it shows another tag, note it in the report and
continue; the bootstrap is cumulative), `hypercore-status` reporting the last run.
Keep this output for the report.

## 4. Update the working copy

```bash
cd ~/blueblocx-hyperliquid
git status --short                                   # expected: empty, or only "?? docs/ETL_TEAM_REQUIREMENT/"
git fetch --tags origin && git checkout main && git pull --ff-only
git describe --tags                                  # expected: v1.8.0-N-g... (main is a few doc commits past the tag)
git tag --list 'v1.8.0'                              # expected: v1.8.0
```

**STOP** if `git status` shows modified tracked files or `git pull --ff-only` refuses:
report, do not reset or stash.

## 5. Wait for a free window and stop the schedule

The hourly run starts at minute 10 (plus up to 2 minutes of jitter) and ends by
minute 58 at the latest. Stopping the timers prevents the next start; a running
service is left to finish.

```bash
sudo systemctl stop hypercore-hourly.timer hypercore-maintenance.timer
while systemctl is-active --quiet hypercore-daily.service; do sleep 20; done
systemctl is-active hypercore-daily.service hypercore-maintenance.service   # expected: inactive  inactive
pgrep -af 'blueblocx_hyperliquid\.' || echo "no hand-run pipeline process"
sudo cat /var/lib/hypercore/hypercore/.daily.lock 2>/dev/null || echo "no lock file"
```

The lock file is small JSON (`pid`, `host`, `started_at`), no secrets. **STOP** if it
names a pid that `ps -p PID` shows alive: a hand run is in progress, wait for it.

From here the schedule is paused. Aim to reach step 9 within about an hour; nothing
breaks if it takes longer (the next run resumes every wallet from its watermark).

## 6. Back up state and catalog

```bash
sudo tar -C /var/lib -czf ~/hypercore-state-$STAMP.tgz hypercore
sudo -u postgres pg_dump hl_catalog | gzip > ~/hl_catalog-$STAMP.sql.gz
ls -lh ~/hypercore-state-$STAMP.tgz ~/hl_catalog-$STAMP.sql.gz
```

**STOP** if either file is missing or implausibly small (the catalog dump is at least
a few KB, the state archive many MB).

## 7. Bootstrap production to v1.8.0

Takes roughly 3 to 8 minutes (apt, pip, the test suite, units, migration,
preflight). Run it with the longest timeout your tool allows; the log goes to a file
in your home directory (readable only by you).

```bash
cd ~/blueblocx-hyperliquid
umask 077
if [ -n "$ETL_HOST_IP" ]; then
  READER_CIDR="${ETL_HOST_IP}/32" RELEASE_TAG=v1.8.0 bash deploy/bootstrap.sh 2>&1 | tee ~/bootstrap-v1.8.0-$STAMP.log
else
  RELEASE_TAG=v1.8.0 bash deploy/bootstrap.sh 2>&1 | tee ~/bootstrap-v1.8.0-$STAMP.log
fi
echo "bootstrap exit: ${PIPESTATUS[0]}"
```

Expected lines in the output, in this order:

- `production checkout /opt/blueblocx-hyperliquid @ v1.8.0`, then `Ran NNN tests ... OK`
- `added HL_PRICE_TIME_BUDGET_MIN=3` and `added HL_PRICE_HISTORY_DAYS=32` (first time only)
- with `READER_CIDR`: `reader connects to IP:5432 ...` and the role / pg_hba lines
- `lake schema migrations (as hypercore)`, a plan for
  `l4_wallet_metadata_first_tx_timestamptz` with `applied: {... 'retyped': ['first_tx', 'latest_tx'], 'backfilled_rows': N ...}`,
  then `up to date` and `migrations: up to date`
- `preflight OK`. WARN rows are acceptable here: `iceberg ... missing ['l3_token_price_usd_1h', 'l3_token_price_usd_1d'] - created from the DDL on the first lift`
- `done: /opt/blueblocx-hyperliquid @ v1.8.0. next steps`

**STOP** if the exit code is not 0 or a line starts with `xx`: report the last 40
lines of `~/bootstrap-v1.8.0-$STAMP.log`. Typical causes: the ssh deploy-key test to
GitHub, the NAS not mounted, a unit still active, the test suite failing in `/opt`,
`migrate --apply` failing (then production is still on the old checkout; nothing was
enabled). Do not enable the timers in that state.

## 8. One-time repair of token first_seen (from v1.6.0)

Must run once after this upgrade and **before 2026-11-26**. Do it now, while the
schedule is still stopped:

```bash
cd /opt/blueblocx-hyperliquid
sudo -u hypercore env HL_ENV_FILE=/etc/hypercore/hypercore.env .venv/bin/python -m blueblocx_hyperliquid.maintenance --fix-first-seen
cd ~/blueblocx-hyperliquid
```

Expected: JSON with `"fix_first_seen": {"tokens": N, "changed": M, "ledger_tokens": K}`
and exit 0 (`changed` may be 0). Exit 3 means the run lock is held, which cannot
happen with the timers stopped: **STOP** and report.

## 9. One real run by hand, then verify

Start the service without waiting (a run takes up to 50 minutes; never block one
command on it), then poll:

```bash
sudo systemctl start --no-block hypercore-daily.service
hypercore-status            # repeat every few minutes: "running : phase ..., wallet N/M"
```

When `systemctl is-active hypercore-daily.service` prints `inactive` (or `failed`):

```bash
systemctl status hypercore-daily.service --no-pager -l | head -n 15
sudo journalctl -u hypercore-daily -n 120 --no-pager
hypercore-status
sudo jq '{version,ok,wallets_total,wallets_ok,wallets_deferred,problems,warnings,prices:{tokens:.prices.tokens,hot:.prices.hot,visited:.prices.visited,deferred:.prices.deferred,history_pending:.prices.history_pending,rows_1h:.prices.rows_1h,rows_1d:.prices.rows_1d,errors:.prices.errors},iceberg_tables:((.iceberg // {})|keys)}' /var/lib/hypercore/hypercore/_run_summary.json
```

Expected: `version: "1.8.0"`, `ok: true`, `problems: []`, `iceberg_tables` including
`l3_token_price_usd_1h` and `l3_token_price_usd_1d`. Acceptable warnings:
`prices_history_pending=N`, `prices_deferred=N`, `deferred=N`, `0x...:fills_truncated`
(auto-tracked wallets), `0x...:pnl_oracle`, `0x...:unpriced=...`.

**STOP** (keep the timers stopped) if `ok` is false, if `problems` contains
`error:...`, `natural_key` or `...:skipped`, or if the service exited with status 3
(lock held) or 1. Report the `problems` list and the last 40 journal lines; see the
rollback section.

## 10. Re-enable the schedule

```bash
sudo systemctl enable --now hypercore-hourly.timer hypercore-maintenance.timer
systemctl list-timers --all 'hypercore-*'          # expected: both timers with a NEXT time
```

## 11. Reader hand-off (only when step 7 ran with READER_CIDR)

```bash
ls -l /etc/hypercore/reader.env                                                   # expected: -rw------- hatsadin ...  (never cat it)
sudo -u postgres psql -Atc "select rolname, rolcanlogin from pg_roles where rolname = 'hl_ro'"   # expected: hl_ro|t
sudo ss -ltn | grep ':5432 '                                                      # expected: 127.0.0.1 and the LAN address
```

Smoke test from this host, in a throwaway venv (never install anything into `/opt`):

```bash
python3 -m venv ~/duckdb-venv && ~/duckdb-venv/bin/pip install -q duckdb sqlalchemy psycopg2-binary
cd ~/blueblocx-hyperliquid && HL_ENV_FILE=/etc/hypercore/reader.env ~/duckdb-venv/bin/python -m blueblocx_hyperliquid.duckdb_smoke
```

Expected: `OK` for every table except `l3_token_price_usd_1m` (`OPT ... not in the
catalog` is normal), exit 0, and a last line `read form that worked: iceberg_scan(...)`.
The first run downloads the DuckDB iceberg extension (needs egress). If every table
shows `FAIL ... Cannot open file`, report the lines; the ETL host's own smoke is the
deciding test anyway.

Hand-off items for the owner (not for you to do): send the contents of
`/etc/hypercore/reader.env` to the ETL team out of band; add a Synology NFS rule for
`ETL_HOST_IP` with privilege *Read only* on the export; the ETL team mounts it
read-only at `/mnt/blueblocx` and runs the same smoke from their host, then follows
the onboarding checklist in `docs/ETL_HANDOFF_HYPERCORE.md` section 8.

## 12. Follow-up checks (hours later, next morning)

```bash
sudo jq '{version,ok,warnings,history_pending:.prices.history_pending}' /var/lib/hypercore/hypercore/_run_summary.json
sudo journalctl -u hypercore-maintenance -n 80 --no-pager      # after 00:05 UTC: compaction + expiry; exit 3 = yielded to a data run, not an error
```

`history_pending` must reach 0 within a few hours; until then the ETL must not start
loading `hypercore` (their onboarding gate).

## Rollback

Only on the owner's decision. Stop the schedule first, exactly as in step 5.

```bash
sudo systemctl stop hypercore-hourly.timer hypercore-maintenance.timer
while systemctl is-active --quiet hypercore-daily.service; do sleep 20; done
# ONLY when rolling back below v1.5.0: undo the lake schema change first, from the v1.8.0 checkout
cd /opt/blueblocx-hyperliquid && sudo -u hypercore env HL_ENV_FILE=/etc/hypercore/hypercore.env .venv/bin/python -m blueblocx_hyperliquid.migrate --revert --apply
cd ~/blueblocx-hyperliquid && RELEASE_TAG=v1.3.0 bash deploy/bootstrap.sh
sudo systemctl enable --now hypercore-hourly.timer hypercore-maintenance.timer
```

Rolling back to v1.5.0, v1.6.0 or v1.7.0 needs no `--revert`. The older preflight
warns `env_unknown` about the two `HL_PRICE_*` keys; that is a warning, not a
failure. Per-release notes: `docs/DEPLOY.md` section 8.

## Report to the owner (fill in every line)

- `/opt` tag before → after (step 3 / step 7)
- bootstrap exit code; migration line (`retyped`, `backfilled_rows`); env keys added; reader role created yes/no
- `--fix-first-seen`: `tokens`, `changed`
- first run: `version`, `ok`, `problems`, `warnings`, `prices.history_pending`, `prices.tokens`
- timers enabled at (time), `list-timers` output
- smoke result and the `read form that worked` line (no credentials)
- anything skipped or stopped at, with the exact output
