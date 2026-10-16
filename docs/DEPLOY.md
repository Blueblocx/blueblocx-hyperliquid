# Deploying the HyperCore pipeline

Runbook for putting `blueblocx-hyperliquid` on a Linux server as a scheduled batch job.
It is a **batch job, not a service**: once a day it pulls each watched wallet's
Hyperliquid history, validates it against Hyperliquid's own portfolio numbers,
and lifts the rows into Iceberg (`nessie.hypercore.*`). Serving those rows to the
product (Rust L4 crates, Trino view, Postgres export, core-api) is work in the
`atlas-project` repo — see [§11](#11-what-this-repo-does-not-do).

Validated on 39 live wallets (6 account shapes); the code and its unit tests
run on Linux (Ubuntu 24.04 / Python 3.12.3) and Windows (Python 3.14); CI covers
Python 3.12 + 3.13 and the Docker image build. The lake is read by the
dagster_blueblocx ETL with DuckDB through the PostgreSQL catalog; what the ETL team
needs is in [`docs/ETL_HANDOFF_HYPERCORE.md`](ETL_HANDOFF_HYPERCORE.md).

---

## 0. Before anything else — the AWS key

The AWS access key that was used while developing the S3 backfill was pasted
into a chat. **Treat it as compromised**: in the AWS console, deactivate and
delete it, then create a *dedicated IAM user* for the server with exactly this
policy (Requester Pays reads from Hyperliquid's bucket, nothing else):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {"Effect": "Allow", "Action": ["s3:GetObject"], "Resource": "arn:aws:s3:::hl-mainnet-node-data/*"},
    {"Effect": "Allow", "Action": ["s3:ListBucket"], "Resource": "arn:aws:s3:::hl-mainnet-node-data"}
  ]
}
```

Requester Pays means **every download is billed to this account**. One 24h
window for a market-maker wallet is ~23 hour files × 25–70 MB ≈ 1–1.5 GB of
transfer; set a billing alert anyway.

## 1. Host requirements

| | minimum | notes |
|---|---|---|
| OS | Linux x86_64 (Ubuntu 22.04+/Debian 12+) | Windows only for development |
| Python | 3.12 or 3.13 | CI-tested; 3.14 works for development |
| CPU / RAM | 2 vCPU / 4 GB | S3 hour files decompress to ~130–330 MB each and are parsed one at a time; peak RSS ~1.5 GB |
| Disk | 20 GB for `HL_OUT_DIR` on a **local** disk | staged CSVs are bounded by `HL_RETENTION_DAYS`; a MM wallet stages ~180k rows/day. Not on an NFS/CIFS mount: every upsert rewrites the CSV whole and the dev catalog is SQLite (preflight warns) |
| Network | outbound HTTPS to `api.hyperliquid.xyz`, `*.amazonaws.com`, and the Nessie / warehouse endpoints | no inbound ports |
| Clock | NTP | time windows and watermarks depend on it; preflight checks skew against HL |

**Rate limit is per egress IP, not per process.** Hyperliquid allows 1200
weight/min per IP. If the TS `platform-backend` poller (or anything else that
calls the Info API) shares this host's public IP, split the budget:
`HL_WEIGHT_BUDGET=600` leaves half for them. Throughput at 600/min is roughly
one light wallet per 30–60 s; a 100-wallet list finishes in ~1 h.

## 2. Install

Three identities, two checkouts:

| who / what | role |
|---|---|
| `hypercore` (system user, no shell, no keys) | runs the timers; may only **read** the code and **write** `/var/lib/hypercore` and the NAS warehouse |
| your login user (`hatsadin`) | owns the git key and both checkouts; Claude Code works as this user |
| `/opt/blueblocx-hyperliquid` | **production checkout**: moved only with `git fetch --tags && git checkout --detach <tag>` (§8); never edited in place — the hourly timer would run half-edited code |
| `~/blueblocx-hyperliquid` | **working copy**: where code is edited, tested, committed and pushed (its own `.venv`) |

Git access for push *and* pull from the server: a **per-repo deploy key with
write access** held by your login user (GitHub → repo → Settings → Deploy keys →
Add → tick "Allow write access"), bound to a host alias because a deploy key is
valid for exactly one repo. Alternative with no long-lived key on the box: SSH
agent forwarding from your laptop (`ssh -A`), which only works while you are
connected.

**Shortcut:** after the key block below, `bash ~/blueblocx-hyperliquid/deploy/bootstrap.sh`
(run from the working-copy clone) performs the rest of this section and §3, creates
the PostgreSQL catalog, the NAS warehouse directory and the systemd units for the
hourly cadence, then runs a preflight — idempotently; it refuses to run while a
timer or a run is active. The commands below are the manual equivalent.

```bash
# as hatsadin
ssh-keygen -t ed25519 -N "" -C "hatsadin@server4090-2 blueblocx-hyperliquid" -f ~/.ssh/blueblocx_hyperliquid
cat >> ~/.ssh/config <<'EOF'
Host github-blueblocx-hyperliquid
  HostName github.com
  User git
  IdentityFile ~/.ssh/blueblocx_hyperliquid
  IdentitiesOnly yes
  StrictHostKeyChecking accept-new
EOF
chmod 600 ~/.ssh/config ~/.ssh/blueblocx_hyperliquid
cat ~/.ssh/blueblocx_hyperliquid.pub        # -> GitHub deploy key, write access ON
git config --global user.name "Leo Hatsadin" && git config --global user.email "<your github email>"

# service user + production checkout (owner = you, group-readable by the service)
sudo useradd --system --home-dir /var/lib/hypercore --no-create-home --shell /usr/sbin/nologin hypercore
sudo usermod -aG hypercore hatsadin
sudo mkdir -p /var/lib/hypercore && sudo chown hypercore:hypercore /var/lib/hypercore && sudo chmod 750 /var/lib/hypercore
sudo mkdir -p /opt/blueblocx-hyperliquid && sudo chown hatsadin:hypercore /opt/blueblocx-hyperliquid
git clone --branch v1.8.0 git@github-blueblocx-hyperliquid:Blueblocx/blueblocx-hyperliquid.git /opt/blueblocx-hyperliquid
cd /opt/blueblocx-hyperliquid && python3 -m venv .venv && .venv/bin/pip install -r requirements.txt -c constraints.txt
.venv/bin/python -m unittest discover -s tests -t .
chmod -R g+rX,o-rwx /opt/blueblocx-hyperliquid

# working copy for development (Claude Code lives here, never in /opt)
git clone git@github-blueblocx-hyperliquid:Blueblocx/blueblocx-hyperliquid.git ~/blueblocx-hyperliquid
cd ~/blueblocx-hyperliquid && python3 -m venv .venv && .venv/bin/pip install -r requirements.txt -c constraints.txt
```

`constraints.txt` pins the full transitive closure the pipeline was validated
with; `requirements.txt` alone would float the indirect packages. Point the
working copy at a scratch state dir (`HL_OUT_DIR=~/hc-dev`, local SQLite
catalog) so development runs never touch `/var/lib/hypercore` or the production
catalog.

## 3. Configure

```bash
sudo mkdir -p /etc/hypercore
sudo cp /opt/blueblocx-hyperliquid/deploy/.env.example /etc/hypercore/hypercore.env
sudo chown root:hypercore /etc/hypercore/hypercore.env && sudo chmod 640 /etc/hypercore/hypercore.env
sudoedit /etc/hypercore/hypercore.env            # fill in the values
printf '%s\n' 0x... 0x... | sudo tee /etc/hypercore/wallets.txt     # optional PINS: one address per line, # comments ok
```

**Which wallets.** The Info API is per address, so a run works from a list. It is
the union of the **pins** in `wallets.txt` (customer / watchlist wallets, always
first, may be empty) and — with `HL_TRACKED_SOURCE=leaderboard` — the **tracked
set** the TS platform-backend sweeps live: the top 100 leaderboard rows by account
value plus up to 50 smart-money rows (account value ≥ 100k, 30-day roi ≥ 20 %,
30-day volume ≥ 5M, ranked by 30-day pnl). It is refreshed from the stats host at
the start of every run (~39 MB, no Info API weight), cached in
`$HL_OUT_DIR/hypercore/_tracked_wallets.json`, and the automatic part is rotated
so wallets deferred by the soft deadline run first next hour. Nothing has to be
typed for the tracked set; pins are for wallets the product must have complete.

Every variable is documented in `blueblocx_hyperliquid/config.py` (`ENV_VARS`); the important ones:

| variable | set it to |
|---|---|
| `HL_OUT_DIR` | `/var/lib/hypercore` — the state directory (watermarks, staged CSVs, run summaries) on a local disk, never the NAS mount. Back it up or accept re-pulling on loss (see §9) |
| `HL_WEIGHT_BUDGET` | `600` if anything else on this IP polls Hyperliquid, else up to `900` |
| `HL_RETENTION_DAYS` | `45` (≥ 31; the 30-day analytics windows read the staged CSV) |
| `HL_WALLETS_FILE` | `/etc/hypercore/wallets.txt` — the pins; an API-capped window on a pin is a hard problem, on a tracked wallet a warning |
| `HL_TRACKED_SOURCE` | `leaderboard` (the automatic set above); `HL_WHALE_TOP_N` / `HL_SMART_TOP_N` / `HL_SMART_MIN_*` mirror the TS constants, `HL_TRACKED_MAX_AGE_HOURS` (48) bounds how long a cached set is reused when the stats host is down |
| `HL_RUN_SOFT_DEADLINE_MIN` | `35` on the hourly cadence (service timeout 50 min): after it no new wallet is started and the rest of the list is deferred to the next run; `0` on the daily cadence |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | the new scoped key from §0 |
| `HL_CATALOG_URI` + `WAREHOUSE` | **standalone host**: PostgreSQL catalog URL + `file:///mnt/<nas>/hypercore/warehouse` (see "Standalone host" below). No Nessie, no object store |
| `NESSIE_URI`, `NESSIE_BRANCH`, `WAREHOUSE`, `S3_ENDPOINT`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_REGION` | **shared atlas lake** instead: the same values the atlas Rust services use (`services/rust/*/.env.example`). With neither set the pipeline falls back to a local SQLite catalog under `HL_OUT_DIR/iceberg` that is for dry runs only |
| `HL_WEB_BIND` / `HL_WEB_PORT` | where the web monitor listens (`0.0.0.0` / `8787`). No authentication: keep it on the LAN or a VPN |
| `HL_LOG_JSON` | `1` when journald/a log shipper collects stderr |

The `.env` loader never overrides variables already in the process environment,
so `EnvironmentFile=` (systemd) or `--env-file` (Docker) work unchanged.

## 4. Preflight

```bash
sudo -u hypercore HL_ENV_FILE=/etc/hypercore/hypercore.env \
  /opt/blueblocx-hyperliquid/.venv/bin/python -m blueblocx_hyperliquid.preflight --iceberg --s3 --wallets-file /etc/hypercore/wallets.txt
```

Expected: every line `OK` or `WARN`, exit 0. It never prints secret values.
Standalone host without an AWS key: drop `--s3` here and `--s3-backfill` in §5
(with the flag, a missing key is a FAIL); the units installed by
`deploy/bootstrap.sh` already omit both.

| check | FAIL means |
|---|---|
| `deps`, `tzdata`, `ddl` | the venv is incomplete (`pip install -r requirements.txt -c constraints.txt`) or `HL_DDL_PATH` is wrong |
| `out_dir` | `HL_OUT_DIR` is not writable by the `hypercore` user. `WARN … network filesystem` means it sits on NFS/CIFS: move it to a local disk |
| `weight_budget`, `retention` | out-of-range values (budget > 1200 is above HL's cap; 0 < retention < 31 would truncate the 30d windows) |
| `wallets` | a line is not a 40-hex address |
| `hl_api` | no route to `api.hyperliquid.xyz` (egress firewall / proxy) |
| `clock` (WARN) | host clock > 60 s off HL's — fix NTP before scheduling |
| `s3` | the IAM key is invalid/rotated, or the policy lacks `s3:GetObject` with requester-pays; `--s3` makes a missing key a FAIL |
| `iceberg` | the REST catalog is unreachable or rejects the credentials; the S3 endpoint it advertises cannot be reached from this host (fix Nessie `external-endpoint`); the effective FileIO needs a package that is not installed; `--iceberg` makes "no `NESSIE_URI`" a FAIL. `WARN missing […]` is fine on the first run (tables are created from the DDL) |
| `tracked` | only misconfiguration: unknown `HL_TRACKED_SOURCE` or a non-numeric tracked-set setting. An unreachable or odd-looking stats host is a **WARN** that says what the run will do (cached set within `HL_TRACKED_MAX_AGE_HOURS` → `tracked_stale`; else pins only → `tracked_unavailable`) — this check runs as `ExecStartPre`, so a FAIL would block exactly that fallback |
| `soft_deadline` | `HL_RUN_SOFT_DEADLINE_MIN` negative or non-numeric; WARN at ≥ 50 (the hourly drop-in kills a run at 50 min, so there it would never fire) |

## 5. First run — small, then full

Optional but recommended: run two or three wallets (any you care about, or the demo
wallets in the README) for a two-day window and read the evidence before scheduling
anything — the scheduled run itself needs no wallet input when the tracked set is on:

```bash
cd /opt/blueblocx-hyperliquid
sudo -u hypercore HL_ENV_FILE=/etc/hypercore/hypercore.env .venv/bin/python -m blueblocx_hyperliquid.extract \
  --wallet 0xA,0xB --days 2 --incremental --iceberg --s3-backfill
```

Then look at `$HL_OUT_DIR/hypercore/_reconciliation.json`:

- `natural_key_ok: true` — otherwise nothing was written and the run exited non-zero.
- per wallet: `fidelity_ok: true`, `oracle.account_value_matches: true`,
  `oracle.networth_matches: true`, `fills_truncated_at_10k_cap: false`.
- `iceberg`: per-table `incoming / written / table_total`. A second identical run
  must show `written: 0` for `l1_wallet_ledger` (insert-if-absent) — that is the
  idempotency check.

Then the whole list once, by hand, exactly as the scheduler will run it:

```bash
sudo -u hypercore HL_ENV_FILE=/etc/hypercore/hypercore.env .venv/bin/python -m blueblocx_hyperliquid.daily \
  --wallets-file /etc/hypercore/wallets.txt --iceberg --s3-backfill
echo $?            # 0 clean · 1 problems (see _run_summary.json) · 2 config · 3 lock held
```

Verify from the lake side too (Trino / Spark SQL):

```sql
SELECT count(*), min(block_date), max(block_date) FROM nessie.hypercore.l1_wallet_ledger;
SELECT * FROM nessie.control.table_watermarks WHERE namespace = 'hypercore';
```

### Standalone host: PostgreSQL catalog + plain files on the NAS (no Nessie, no MinIO)

When this pipeline is the only lake writer on the host (the case for
`server4090-2`), it needs **no object store and no Nessie**: pyiceberg writes
Parquet and Iceberg metadata as plain files, and a small PostgreSQL holds the
catalog rows. The engines that forced Nessie + S3 in the atlas lake (Trino 465,
iceberg-rust with S3 hard-coded, Nessie's own ObjectIO) are not on this host.

```bash
sudo apt install postgresql                               # local disk, a few hundred MB
sudo -u postgres psql -c "CREATE ROLE hl LOGIN PASSWORD '...';" -c "CREATE DATABASE hl_catalog OWNER hl;"
sudo -u hypercore mkdir -p /mnt/blueblocx/hypercore/warehouse   # the NAS: data + metadata files
```

In the env file: `HL_CATALOG_URI=postgresql+psycopg2://hl:...@localhost:5432/hl_catalog`,
`WAREHOUSE=file:///mnt/blueblocx/hypercore/warehouse`, `HL_WAREHOUSE_MOUNT=/mnt/blueblocx`,
`NESSIE_URI` empty. pyiceberg creates its two catalog tables on first use and the
`hypercore` tables from the DDL on the first lift. `preflight --iceberg`
checks the database, that the warehouse directory is writable **and on the
expected mount**, and reports the filesystem type (NFS is fine **for the
warehouse**; `HL_OUT_DIR` must stay on the local disk).

Verified against pyiceberg 0.12 source and the Linux NFS client (2026-10-12):

- **Volume.** Hourly lifts write a few MB of Parquet (a market-maker wallet is
  ~30 MB/day); a lift is at most one Iceberg commit per table that received rows
  (delete + append of a replaced table happen in ONE transaction since v1.4.0)
  plus one commit for all watermark rows, i.e. ~8 commits regardless of wallet
  count. On a synchronous NFS export that is a few seconds of NFS time per hour.
- **Atomicity.** Every file pyiceberg writes gets a new unique name and is
  closed (NFS flushes on close) before the catalog pointer is swapped with an
  optimistic `UPDATE … WHERE metadata_location = <previous>` in PostgreSQL.
  Nothing depends on atomic rename. A concurrent writer fails cleanly
  (`CommitFailedException`, no retry) — hence the single-flight lock.
- **Durability.** pyiceberg never fsyncs; with the Synology export set to
  "Enable asynchronous" a NAS power loss can leave the pointer at a file that
  never reached disk. Keep the export synchronous (or put the NAS on a UPS);
  recovery is `UPDATE iceberg_tables SET metadata_location = previous_metadata_location`.
- **Guards.** The systemd unit has `RequiresMountsFor=/mnt/blueblocx` and
  `ReadWritePaths=… /mnt/blueblocx/hypercore` (with `ProtectSystem=strict` the
  NAS would otherwise be read-only inside the unit); `HL_WAREHOUSE_MOUNT` makes
  the run refuse to start when the path is not on that filesystem instead of
  quietly creating the warehouse on the root disk. Inside the unit every
  `ReadWritePaths` entry is its own bind mount, so `/proc/mounts` shows the
  warehouse directory as a separate nfs4 mount below `/mnt/blueblocx`; the guard
  accepts a nested mount of the same filesystem type and rejects a bind mount of
  the root disk (what you get when the NAS is not mounted).
- **Readers.** `hypercore-sql` on this host (§7: pyiceberg resolves the committed
  snapshot through this catalog, DuckDB runs the SQL; ships with the pipeline, writes
  nothing), any pyiceberg or DuckDB on this host, and the ETL's DuckDB from the
  ETL host — see "Serving the lake to DuckDB readers" below. Alternatives that
  were considered and not chosen: iceberg-rust `iceberg-catalog-sql` + an fs
  storage factory (the atlas crates hard-code REST + S3), Trino ≥ 471 with
  `iceberg.catalog.type=jdbc` + `fs.local.enabled=true` (the atlas Trino is 465).
  Plain Parquet globs are unsafe for the replaced tables (old snapshot files
  coexist until expired).
- **If nothing cross-host ever reads the lake**, PostgreSQL is optional:
  `HL_CATALOG_URI=sqlite:////var/lib/hypercore/catalog.db` is a single-writer
  catalog on the local disk and this pipeline is exactly one writer (preflight
  still warns, as pyiceberg labels SQLite development-only).
- **Backup** = `pg_dump hl_catalog` **first**, then the warehouse directory
  (NAS snapshot); the dump then only references files that already exist.
- **Maintenance.** `python -m blueblocx_hyperliquid.maintenance --expire-days 7 --compact`
  nightly (`deploy/hypercore-maintenance.{service,timer}`, 00:05 UTC): first
  **compaction** (v1.8.0) — every closed partition with ≥ 8 data files (closed =
  its UTC day is ≥ 2 days old, 1 day for l4x) is rewritten into one file, one
  transaction per table so the pointer moves once, bounded by the next hourly tick
  minus a minute because maintenance holds the shared run lock — then snapshot
  expiry so `metadata.json` stays small; tables carry
  `write.metadata.delete-after-commit.enabled=true` and
  `write.metadata.previous-versions-max=100`. pyiceberg deletes no data files on
  expiry (the pre-compaction files become small orphans; a sweeper is a follow-up).

### Serving the lake to DuckDB readers

The dagster_blueblocx ETL reads hypercore with **DuckDB** through this catalog
(owner decision, 2026-10-14) — no Nessie, no Trino. Two things make that work:

- **Identical mount path.** pyiceberg stores absolute `file:///mnt/blueblocx/hypercore/warehouse/…`
  locations in every metadata and manifest file, so the reader host mounts the
  same NFS export **read-only at the same path** (Synology: an NFS rule for the
  ETL host with privilege *Read only*; mount options
  `ro,vers=4.1,hard,noatime,_netdev,lookupcache=positive`).
- **A read-only catalog role.** `READER_CIDR=<etl host ip>/32 bash deploy/bootstrap.sh`
  creates role `hl_ro` (SELECT on the catalog tables, `default_transaction_read_only`,
  default privileges for tables `hl` creates later), makes PostgreSQL listen on this
  host's LAN address, adds a `pg_hba` rule for exactly that CIDR (and a `ufw` rule
  when ufw is active), and writes `/etc/hypercore/reader.env` (mode 600, owned by
  you) with `HL_CATALOG_URI` for `hl_ro` and `WAREHOUSE`. Hand the credentials to
  the ETL team out of band — never in chat, commits or docs.

How a reader works (the two-step read; `iceberg_scan()` needs a constant path):

```sql
-- 1. one catalog read per ETL run (never cache a pointer across runs: metadata files
--    older than write.metadata.previous-versions-max commits are deleted)
SELECT table_name, metadata_location FROM iceberg_tables
WHERE catalog_name = 'hl' AND table_namespace = 'hypercore';
-- 2. per table, with the location from step 1
SET TimeZone = 'UTC';                      -- timestamptz columns then render as +00:00
SELECT count(*) FROM iceberg_scan('<metadata_location>');
-- 2'. scheme-independent form: the table root with the committed version pinned
--     (<metadata_location> = <root>/metadata/<version>.metadata.json). Reads exactly
--     the file the catalog points at and resolves data/manifest paths relative to the
--     root, so it never depends on how DuckDB treats the absolute file:// locations
--     stored inside the manifests.
SELECT count(*) FROM iceberg_scan('<root>', version = '<version>', allow_moved_paths = true);
```

Which form the ETL host should use (the raw `file:///…` location, the stripped
local path, or the pinned-version table root) is settled by the smoke test — run it
from a throwaway venv as your login user (duckdb is deliberately not a dependency
of the pipeline; never pip install into `/opt`; the smoke needs only `duckdb` and
`sqlalchemy`, not pyiceberg):

```bash
python3 -m venv ~/duckdb-venv && ~/duckdb-venv/bin/pip install duckdb sqlalchemy psycopg2-binary
cd ~/blueblocx-hyperliquid && HL_ENV_FILE=/etc/hypercore/reader.env ~/duckdb-venv/bin/python -m blueblocx_hyperliquid.duckdb_smoke
```

It reports the DuckDB / iceberg-extension versions, the scan form that worked (and
prints it), row counts and the `max(updated_at)` rendering per table. The same
command on the ETL host (through `hl_ro` and the read-only mount) is the ETL's
acceptance test. Verified 2026-10-15 with DuckDB 1.5.6 (iceberg extension
890b78a9c) against a development lake: DuckDB resolves standard three-slash
`file:///…` locations (the form production writes), rejects the two-slash
`file://C:/…` form the Windows dev catalog writes (pyiceberg on Windows cannot
write the other form), and the pinned-version table-root form read every table in
both cases; `SET TimeZone = 'UTC'` makes `max(updated_at)` render as `… +00`.
`preflight --duckdb` runs the identical check when `duckdb` happens to be
importable and FAILs otherwise; without the flag the online preflight runs it
only when duckdb is importable, and `--offline` never runs it (the extension
install needs egress to `extensions.duckdb.org` once per venv, and the smoke
reads the catalog). `l3_token_price_usd_1m` is reported as
*optional / absent* in production (development-only output). Concurrency is safe:
every file pyiceberg writes is immutable and uniquely named, the catalog pointer
moves at most once per table per lift, and snapshot expiry (7 days) deletes no
data files — a scan that started before a commit keeps reading its snapshot.

Hand-off notes for the ETL team (identifiers, Tier-A semantics, lift timing,
per-table SQL) live in [`docs/ETL_HANDOFF_HYPERCORE.md`](ETL_HANDOFF_HYPERCORE.md).

### Restatements and the ETL's full refresh

The lake is append-only for facts (`l1_wallet_ledger` is insert-if-absent: a
re-pulled row never changes its lake row) and replace-by-key for the dimension
tables. Therefore:

- A **correction** to already-landed ledger rows (a mapper fix, a hand re-pull
  with `--days N`) does **not** reach the lake by itself; it needs an explicit
  Iceberg delete of the affected keys followed by a re-lift. Treat that as a
  *restatement*: tell the ETL team before it runs (they stop their hourly
  schedule and run a full refresh afterwards), and run it as
  `python -m blueblocx_hyperliquid.iceberg_load --from $HL_OUT_DIR/hypercore --full`
  between timer ticks — that path records `status='restated'` in
  `control.table_watermarks` for every touched table.
- The ETL's own **full refresh** reads every table once while truncating its
  destination; the ETL team announces it, and the only coordination needed on
  this side is that the lift is already single-commit per table (a reader never
  sees a half-replaced table). Nothing has to be paused.
- Rows are stamped with the **run start** instant and committed at the end of the
  run (≤ 50 min later under the hourly unit), inside the ETL's 2-hour re-pull
  margin. The run logs a warning when that gap exceeds 60 minutes (a long hand
  run): announce hand runs longer than 90 minutes to the ETL team.

### Hourly cadence

`deploy/hypercore-hourly.timer` runs the same service every hour (minute 10)
with `deploy/hourly-override.conf` capping a run at 50 min. The run is
incremental (per-wallet watermark, 1 h overlap, bounded lift), the staged CSVs
are read once per run for all wallets, and retention pruning runs at most once
a day. Verified against the code (2026-10-12):

| wallet class | API weight per hourly run | wallets per 50-min run at budget 900 / 1200 |
|---|---|---|
| light (< 20 new fills) | ~165 (4 flat-20 account calls, 4 paged history calls, 2 state calls) | ~270 / ~360 |
| active (~300 fills/h) | ~200 | ~225 / ~300 |
| market maker (5–8k fills/h) | ~750–1,200 | ~40 / ~55 |

The TS platform-backend runs on Cloudflare Workers, not on this host's IP, so
`HL_WEIGHT_BUDGET=900` (default) or up to 1100 is safe unless something else here
calls the Info API. Hourly windows keep market makers under the ~10k fills cap
(cap coverage 60–120 min at 5–8k fills/h), so S3 backfill becomes a rare repair
path — fetch MM-class wallets first in the list, because fills made during the
run eat into the cap. Beyond ~270–360 wallets: a second egress IP, or fetching
the slow-changing pulls (staking, vault equity, open orders) once a day with a
lazy re-fetch when the net-worth oracle fails — a code change not made yet
(+60 % capacity).

**The price step runs first.** From v1.7.0 the first ≤ 3 minutes of every run
(`HL_PRICE_TIME_BUDGET_MIN`) fetch native 1h / 1d candles for the ETL's
`token_price_usd_1h` / `token_price_usd_1d` (`blueblocx_hyperliquid.price_step`):
tokens with ledger activity in the last 7 days every run, the rest rotated most-stale
first, closed candles only, 32 days on a token's first visit, stables pinned at 1.0,
no perp rows. A gap fetch weighs ~20, so ~130 tokens fit the budget each hour; the
two tables are lifted before any wallet is pulled. The wallet phase then has ≈ 32 of
the 35 soft-deadline minutes. The run summary reports `prices_deferred=N` (budget
reached) and `prices_history_pending=N` (tokens never fetched yet - the ETL's
onboarding gate is 0); `_price_state.json` holds the per-token cursors (delete it to
re-fetch everything).

Rules for hourly operation:

- **Capacity is set by the soft deadline, not by the kill cap.** Wallet starts
  share `HL_RUN_SOFT_DEADLINE_MIN × HL_WEIGHT_BUDGET` weight (35 × 900 ≈ 31,500;
  the ~45,000 of a 50-minute run is only the kill cap). Measured 2026-10-13: 100
  whales + 50 smart-money wallets = 149 addresses (1 overlap); the whales' median
  30-day volume is 0 (passive capital, light class) and 11 of the 149 trade ≥ $1B a
  month (market-maker class). Steady state ≈ 149 × 165–200 ≈ 25–30k, inside the
  window; a market-maker-class wallet costs ~1k only in its first hour (its 2-day
  window hits the 10k-fill cap once, the hole is accepted, and from then on it is
  light). An occasional `deferred=N` is normal; every hour means the list has
  outgrown the slot. One wallet's own pull is never interrupted, so the 15-minute
  margin covers the wallet in flight, phase 2 (its one unbounded call, the lazy
  full-ledger pull, is itself skipped once the deadline has passed and retried
  next run) and the lift.
- **Tracked wallets onboard themselves** (a wallet with no watermark gets a 2-day
  window) and are never S3-backfilled by the scheduled run. An API-capped window
  on a tracked wallet is **accepted as a hole**: `<w>:fills_truncated` is a
  warning for that run, coverage restarts at the oldest fill returned (the 7d/30d
  windows stay `*_complete=false` until genuinely covered), the watermark advances
  and the wallet is light from the next hour. **Pins keep the repair semantics**:
  the watermark is held until an S3 backfill completes the window, and the
  incomplete window is a hard problem — so pin a market-maker-class wallet by hand
  with a backfill first
  (`python -m blueblocx_hyperliquid.extract --wallet 0x… --days 2 --incremental --iceberg --s3-backfill`),
  because that backfill can outlast the 50-min cap inside a scheduled run.
- A killed or OOM-killed run can no longer block the next tick: the lock
  records the PID and is discarded when that process is gone (`HL_LOCK_STALE_HOURS=2`
  is the age fallback for a holder that cannot be observed), SIGTERM from the
  timeout unwinds cleanly, and a lock that is genuinely held is itself
  summarised (exit 3). A lock whose PID is **a live pipeline process is never
  stolen**, however old (identity from `/proc/<pid>/cmdline`; a PID recycled by an
  unrelated process after a reboot is discarded at once): `extract`,
  `iceberg_load`, `maintenance` and the scheduled run all take
  the same `.daily.lock`, so while a hand-run backfill of a market-maker wallet
  runs for hours, every hourly tick exits 3, the unit shows *failed* and the
  monitor says `lock_held` — expected, not an outage. The hand run's rows are
  still lifted by its own lift (the cut-off never passes the run's own stamp).
- `l4x_perp_position_snapshot` keeps the **last snapshot per UTC day** per
  wallet (its key is `as_of_date`); hourly runs replace the day's rows, so the
  lake holds no intra-day position history, and the PnL oracle (needs a 20–28 h
  old snapshot) fires in roughly 8–9 of 24 runs. Add an `as_of_hour` to the key
  if hourly mark-to-market history is wanted.
- `MemoryMax=8G` in the unit: the staged ledger is merged in memory on every
  upsert (~1 KB/row); 45 days of a market-maker wallet is several GB.

### Talking to a Nessie REST catalog from another host

Three things the first real lift against Nessie will hit unless prepared:

- **The endpoint Nessie advertises wins.** Nessie pushes its
  `nessie.catalog.service.s3.default-options.external-endpoint` to every client as
  the S3 endpoint, and REST overrides beat the client's `S3_ENDPOINT`. On olympus
  that value is the Docker gateway `172.17.0.1:9000`, unreachable from any other
  host: listing namespaces works, every read/write hangs. Set it to a LAN address
  (and open ports 19120 + 9000 to this host only). `preflight --iceberg` fails
  with the advertised endpoint in the message when it cannot connect to it.
- **FileIO.** Nessie's defaults name `pyiceberg.io.fsspec.FsspecFileIO`, which
  needs `s3fs`; this project pins `pyiceberg.io.pyarrow.PyArrowFileIO` instead
  (`HL_PY_IO_IMPL`), and defaults `S3_REGION` to `us-east-1` so PyArrow does not
  ask AWS for the bucket region. Preflight checks the effective class.
- **S3 store interop.** PyArrow's S3 client sends chunked uploads with trailing
  checksums. SeaweedFS releases up to 4.48 can reject those intermittently with
  `400 InvalidDigest` (fixed upstream on 2026-10-03, after 4.48); MinIO and AWS
  are fine. Before the first production lift against a new store, lift one small
  table into a scratch namespace a few dozen times (`--tables l1_token_metadata`)
  and watch for `InvalidDigest`. See `docs/MINIO_ASSESSMENT.md` §7.3.
- **Lifts are bounded.** `_lift_state.json` in the staging dir records the last
  successful lift; the next run reads only rows updated after it (1h margin) and
  checks only those against the lake. Row counts come from table metadata, never
  a scan. To re-lift everything (new lake, lost state): `python -m blueblocx_hyperliquid.iceberg_load --from $HL_OUT_DIR/hypercore --full`.
  To retry only the lift after a failed run: the same command without `--full`.

### Iceberg tables: who creates them

`docs/PROPOSED_SCHEMA.sql` is the DDL. Two valid orders:

1. **Create them first** with Spark SQL / Trino from that file (recommended: the
   engines that also own `nessie.ethereum.*` then own this namespace too). The
   loader casts every batch to the *existing* table's Arrow schema and fails
   fast on a type it cannot write.
2. **Let the first lift create them** — pyiceberg builds the same shapes
   (`timestamptz` columns, identity partitions) from the DDL.

Either way, `control.table_watermarks` rows are written with
python-ingestion's exact schema, so the atlas watermark readers see them.

## 6. Schedule

**Standalone host (hourly)** — `deploy/bootstrap.sh` has already installed
`hypercore-daily.service` with a drop-in (no S3 flags, 50-minute cap),
`hypercore-hourly.timer`, `hypercore-maintenance.timer` (00:05 UTC, before the
00:10 tick), the web monitor `hypercore-web.service` (already running on port 8787)
and `/usr/local/bin/hypercore-status` + `hypercore-sql`; after the first run by hand (§5) enable the timers:

```bash
sudo systemctl enable --now hypercore-hourly.timer hypercore-maintenance.timer
systemctl list-timers 'hypercore-*'
```

**systemd, daily cadence (manual install)** — the service runs preflight first,
then the daily run; timers never overlap a running service:

```bash
sudo cp deploy/hypercore-daily.service deploy/hypercore-daily.timer deploy/hypercore-web.service /etc/systemd/system/
sudo install -m 0755 deploy/hypercore-status deploy/hypercore-sql /usr/local/bin/   # §7's status + SQL commands
sudo systemctl daemon-reload
sudo systemctl enable --now hypercore-web.service hypercore-daily.timer
sudo systemctl start --no-block hypercore-daily.service   # run once now (without --no-block, start waits for the whole run)
sudo journalctl -u hypercore-daily -f
```

00:20 UTC is deliberate: Hyperliquid's midnight dust conversions have settled and
the 23:00 S3 hour file (published ~00:02) exists, so the previous day is complete.

**cron** — `deploy/run_daily.sh` + `deploy/crontab.example` (uses `flock`).

**Docker** — `docker build -t blueblocx-hyperliquid .`; state in a volume mounted at `/data`
(`wallets.txt` at `/data/wallets.txt`); schedule the `docker run` from host cron
or an orchestrator. `ENTRYPOINT` is `python -m`, so
`docker run --rm --env-file /etc/hypercore/hypercore.env -v hypercore-data:/data blueblocx-hyperliquid blueblocx_hyperliquid.preflight`
works too.

**Dagster** — `deploy/dagster_hypercore_assets.py` is a template asset that runs
the same command as a subprocess; register it in the atlas dagster project.

Add the shared L3 price job after the daily run if the product needs
`l3_token_price_usd_1m` for HyperCore tokens (`crontab.example` has the line).

## 7. Monitor

systemd is the controller; the monitoring is a read-only view of what the run
leaves behind — one small stdlib web server, no Dagster daemon/webserver/database
on this host (the atlas Dagster on olympus can still trigger the same command over
SSH later, see `deploy/dagster_hypercore_assets.py`). There is no push alerting:
you look at the page, or point any uptime checker you already have at `/healthz`.

| signal | answers | where |
|---|---|---|
| **web monitor** `hypercore-web.service` → `http://<host>:8787/` | the whole picture, refreshed every 30 s: **running?** phase, wallet i/N, progress bar, the wallet in flight, minutes since the last progress — `STUCK?` after `HL_STUCK_MINUTES` quiet minutes while the process is alive, "died without a summary" when the pid is gone; **last run** verdict, problems, warnings; **recent runs** table; **per-wallet** table (problems first); **timers**. `/api/status` (JSON verdict), `/api/summary`, `/api/progress`, `/api/history`, `/healthz` (200 / 503) | `python -m blueblocx_hyperliquid.web`, bound to `HL_WEB_BIND:HL_WEB_PORT`; read-only, no authentication — LAN/VPN only |
| `hypercore-status` (`python -m blueblocx_hyperliquid.status`, `--json` for scripts) | **running?** phase, wallet i/N, minutes since the last progress — `STUCK?` after `HL_STUCK_MINUTES` (15) quiet minutes while the process is alive (the clock also spans one first-time full-ledger lookup, ~9 min on an old whale); a progress file whose pid is gone = "died without a summary"; **last run** ok? how long ago — `STALE` after 2 h on the hourly timer, 26 h when only `hypercore-daily.timer` is active, or `HL_STATUS_MAX_AGE_HOURS` (set it for cron/Docker schedules); **timers** active? exit 0 healthy / 1 attention | reads `_run_progress.json`, `_run_summary.json`, `_run_history.jsonl` + `systemctl show` |
| `hypercore-sql` (`python -m blueblocx_hyperliquid.query`) | **what is in the lake**: `--list` (rows, partitioning, last commit per table), then any SQL over `l1_wallet_ledger`, `l4x_perp_position_snapshot`, `l1_token_metadata`, `l4_wallet_metadata`, `wallet_watchlist`, `token_whitelist`, `table_watermarks` — see "Query the lake" below | pyiceberg → DuckDB in memory; read-only; any user in the `hypercore` group |
| journald | the detail: one line per wallet while pulling (`[12/149] 0xeadc152a pulled in 61.3s: 6159 fills …`), per-table lift lines, tracebacks | `journalctl -u hypercore-daily -f` |

### Query the lake (SQL)

```bash
hypercore-sql --list                                   # tables, rows, partitioning, last commit, this session's bound
hypercore-sql "select block_date, event_type, count(*) n, sum(amount_signed) usdc
               from l1_wallet_ledger where token_address = 'hlcore:usdc' group by 1, 2 order by 1, 2"
hypercore-sql --days 90 --format csv -f report.sql > report.csv
hypercore-sql                                          # interactive: end statements with ';', .tables / .schema NAME / .load NAME / .rows N / .quit
```

`hypercore-sql` is `deploy/hypercore-sql`: the production checkout's venv with the
production env file, running `blueblocx_hyperliquid.query`. pyiceberg opens the
catalog **read-only** (`init_catalog_tables=false`, no directory created) and
resolves the committed snapshot of each table a statement names; the rows are
scanned once and registered in an in-memory DuckDB, which runs the SQL. Reading
never blocks or breaks the hourly lift (no reader locks; the lift commits with one
optimistic pointer `UPDATE`), and nothing is written anywhere: DuckDB runs with
file access disabled (no `COPY … TO` a file, `read_csv`, `ATTACH`, `INSTALL` — export
with `--format csv > file`), and an oversized query spills under the system temp dir,
never into the checkout. Error messages are scrubbed of secret values; never `echo`
the env file yourself. A relative `-f FILE` is resolved from where you run the command.

- **Bounded by default.** The fact / snapshot tables are read for the last 30 days
  (`block_date`, `as_of_date`, `bucket_date` — identity partitions, so the scan reads
  only those files); `--days N`, `--since` / `--until` (`--until` alone ends the N-day
  window there), or `--days 0` for everything.
  The whole filtered table sits in memory (~250 bytes per ledger row), so bound a
  year of market-maker rows before you `select *` it. Dimension tables and
  `control.table_watermarks` are read whole.
- **Names.** Bare (`l1_wallet_ledger`) or qualified (`hypercore.l1_wallet_ledger`,
  `control.table_watermarks`). `l3_token_price_usd_1m` appears only once the price
  job has run. `--namespace hyperevm` for the EVM namespace.
- **Semantics** (`docs/HYPERCORE_LEDGER_SPEC.md` §3, `docs/PROPOSED_SCHEMA.sql` rollup
  note): `amount_signed` is in the row's token, not USD — sum stablecoin rows (`join
  l1_token_metadata … where is_stablecoin`) directly and price the rest (spot-buy fee
  legs are in the base token; HIP-3 PnL/funding/fee rows are in that dex's collateral
  token). Realized PnL, lake part = Σ usd over `event_type in ('perp_realized_pnl',
  'funding','fee','interest','staking_reward')` (the last is reserved, not emitted yet;
  spot FIFO realized PnL is not in the lake); `net_flow` = Σ usd over
  `('deposit','withdraw','transfer')` only, and `transfer` includes net-zero internal
  legs. One spot fill is several rows sharing `tx_hash` (`count(distinct tx_hash)`).
  `l4x_perp_position_snapshot` holds the last snapshot per UTC day per wallet;
  `account_value_usd` is the margin account of one `(wallet, dex)` repeated on each of
  its position rows: `max()` per `(as_of_date, wallet_address, dex)`, then `sum()` across
  dexes for the wallet's perp equity (spec §3: main + Σ dex). Timestamps are UTC
  (`timestamptz`; `table_watermarks` naive UTC).
- **From the working copy** (development): `.venv/bin/python -m blueblocx_hyperliquid.query`
  reads whatever catalog the env selects — the local SQLite catalog under `HL_OUT_DIR`
  by default, or production with `HL_ENV_FILE=/etc/hypercore/hypercore.env` (read-only,
  the one module that may be pointed at the production env from there).
- **Heavier engines** (Spark SQL, Trino ≥ 471 with `iceberg.catalog.type=jdbc` on the
  same PostgreSQL, catalog name `hl`) are possible but are new JVM daemons on this
  host: an owner decision, not part of this repo.

Every run writes `$HL_OUT_DIR/hypercore/_run_summary.json` (full detail) and
appends one line to `_run_history.jsonl`. Exit code and summary agree:

| `problems` entry | meaning | action |
|---|---|---|
| `natural_key` | chain-wide `(tx_hash, event_index, direction)` collision — **nothing was written** | a mapping bug; open an issue with `_reconciliation.json` |
| `<w>:skipped` | a core pull (fills/TWAP/funding/ledger/state) failed for that wallet | transient HL error → next run re-pulls (watermark not advanced). Persistent → check `fetch_errors` |
| `<w>:fidelity` | mapped rows ≠ raw API input | mapping bug or a new fill shape; keep the raw JSON |
| `<w>:perp_oracle` | perp equity ≠ HL `perpAllTime` (or the oracle could not be read) | usually transient (position opened minutes before); persistent → a HIP-3 dex or collateral we do not know |
| `<w>:networth_oracle` | net worth ≠ HL `allTime` by > 1 % / $2 | a component we do not value (new token, new lending market) — see `unvalued_*` in the reconciliation |
| `<w>:fills_truncated` | window incomplete: API cap hit and S3 backfill missing/incomplete | ensure `--s3-backfill` + valid key; hour files publish ~1 h late — the next run completes it |
| `<w>:fetch_errors=…` | a non-core pull (staking / vaults / open orders / a dex) failed | net worth may be understated for that run |
| `tracked_unavailable` | the leaderboard could not be fetched and no cached set was within `HL_TRACKED_MAX_AGE_HOURS`; the run covered the pins only | check that `stats-data.hyperliquid.xyz` is reachable; the next run retries |
| `error:…` | the run raised | `journalctl -u hypercore-daily` has the traceback |

`warnings` do not fail the run but are shown in the monitor: `pnl_oracle`,
`unmapped_ledger_types`, `unpriced`; `<w>:networth_pending` (net worth differs
from HL but the full-ledger lending lookup that could explain it was skipped by the
soft deadline — the next run pulls it, and the wallet is then either clean or a
real `networth_oracle` problem); `<w>:fills_truncated` for an **auto-tracked**
wallet (its API-capped window stays incomplete without S3 backfill — for a pin the
same thing is a hard problem); `tracked_stale=Xh` (the cached tracked set was used);
`deferred=N` (the soft deadline left N wallets for the next run — fine now and
then; every hour means the list has outgrown the slot: raise `HL_WEIGHT_BUDGET`
toward 1100 if nothing else uses the IP, lower `HL_WHALE_TOP_N`, or add an egress
IP). The summary's `tracked` object records the source (`fresh` / `cache` / `none`),
the pin / auto / planned counts and the rotation cursor. Promote `pnl_oracle` to
hard once it has been stable for a few weeks on your wallet set.

Logs go to stderr (journald under systemd); `HL_LOG_JSON=1` for one JSON object
per line.

## 8. Upgrade (release flow when code is written on the server)

Develop and verify in the working copy, release with a tag, then move the
production checkout to that tag between timer ticks:

```bash
# 1) in the working copy (as hatsadin): edit, test, commit, push, tag
cd ~/blueblocx-hyperliquid
.venv/bin/python -m unittest discover -s tests -t . && .venv/bin/python -m blueblocx_hyperliquid.preflight --offline
git add -A && git commit -m "..." && git push
git tag -a v1.0.1 -m "..." && git push --tags            # CI runs on GitHub (tests + image)

# 2) move production to the tag while no run is active - scheduled OR hand-run: bootstrap
#    checks both; by hand, also confirm nothing holds the lock
sudo systemctl stop hypercore-hourly.timer hypercore-daily.timer 2>/dev/null
while systemctl is-active --quiet hypercore-daily.service; do sleep 10; done
pgrep -af 'blueblocx_hyperliquid\.' ; sudo cat /var/lib/hypercore/hypercore/.daily.lock 2>/dev/null   # both must show nothing live
# if the release notes list a state-file change (staged CSVs, _watermarks.json): tar HL_OUT_DIR first
sudo tar -C /var/lib -czf ~/hypercore-state-$(date -u +%Y%m%dT%H%M).tgz hypercore
cd /opt/blueblocx-hyperliquid && git fetch --tags && git checkout --detach v1.0.1
.venv/bin/pip install -r requirements.txt -c constraints.txt
.venv/bin/python -m unittest discover -s tests -t .
# lake schema migrations shipped by the release (idempotent; the lift refuses a drifted table)
sudo -u hypercore env HL_ENV_FILE=/etc/hypercore/hypercore.env .venv/bin/python -m blueblocx_hyperliquid.migrate --apply
HL_ENV_FILE=/etc/hypercore/hypercore.env .venv/bin/python -m blueblocx_hyperliquid.preflight --iceberg
# only when the release changed deploy/ (units, the web monitor, hypercore-status, hypercore-sql): refresh the
# installed copies; the drop-in hypercore-daily.service.d/local.conf is kept
sudo cp deploy/hypercore-daily.service deploy/hypercore-maintenance.service deploy/hypercore-web.service /etc/systemd/system/ \
  && sudo install -m 0755 deploy/hypercore-status deploy/hypercore-sql /usr/local/bin/ && sudo systemctl daemon-reload \
  && sudo systemctl restart hypercore-web.service
sudo systemctl start hypercore-daily.service && sudo journalctl -u hypercore-daily -n 50   # one real run (start waits for it)
sudo systemctl start hypercore-hourly.timer
```

`RELEASE_TAG=v1.8.0 bash ~/blueblocx-hyperliquid/deploy/bootstrap.sh` does all of
step 2 for you (checkout, pip, tests, units + web monitor restart + wrapper +
daemon-reload, env keys new in the release, lake schema migrations via
`migrate --apply`, preflight), refuses to run while a
timer, a run or a hand-run pipeline process is active, and is the simpler
choice whenever a release touches `deploy/` or the NAS is not at `/mnt/blueblocx`
(the shipped units hard-code that path; bootstrap rewrites it); re-enable the
timers afterwards. **Rollback** is the same bootstrap with the previous tag —
use bootstrap rather than a manual checkout whenever the release changed
`deploy/` (a unit file left behind by the newer release may pass flags the older
code does not know). State files (`_watermarks.json`, `_lift_state.json`, staged
CSVs) are forward- and backward-compatible across versions of this repo so far;
the release notes below say when a release changes them or the lake schema (then
the rollback order is given there too). Never run `git pull` or an editor inside
`/opt`: the hourly timer executes whatever is on disk at minute 10.

Release notes (state-file and lake-schema changes only):

| release | state files | lake | rollback |
|---|---|---|---|
| v1.4.0 | none | replaced tables are rewritten in one commit per lift; `control.table_watermarks` rows gain the statuses `failed` / `restated` and `watermark_ts` becomes the max **write** stamp (was business time); lift order parents → facts → derived | `RELEASE_TAG=v1.3.0 bash deploy/bootstrap.sh` (v1.3.0 reports `kept_newer` on a few watermark rows for an hour — harmless) |
| v1.5.0 | staged `l4_wallet_metadata.csv` rewritten by `migrate --apply` (tx-hash values in `first_tx` / `latest_tx` replaced by the block timestamps) — **tar HL_OUT_DIR first** | `hypercore.l4_wallet_metadata.first_tx` / `latest_tx`: STRING → TIMESTAMPTZ by in-place schema evolution (drop + re-add, DDL order, one backfill commit from `first_block_timestamp` / `latest_block_timestamp`); the lift raises `SchemaDriftError` and preflight FAILs on a table whose types differ from the DDL | **first** `cd /opt/blueblocx-hyperliquid && sudo -u hypercore env HL_ENV_FILE=/etc/hypercore/hypercore.env .venv/bin/python -m blueblocx_hyperliquid.migrate --revert --apply` (columns back to STRING, values NULL until the next run), **then** `RELEASE_TAG=v1.4.0 bash deploy/bootstrap.sh`; without the revert v1.4.0's lift of that table fails every hour (string → timestamptz cast) |
| v1.6.0 | `_watermarks.json[wallet].meta` added (the running `l4_wallet_metadata` aggregate; deleting a wallet's key re-seeds it from staging on the next run); staged dimension CSVs keep their stamps when content is unchanged; **one-time** `cd /opt/blueblocx-hyperliquid && sudo -u hypercore env HL_ENV_FILE=/etc/hypercore/hypercore.env .venv/bin/python -m blueblocx_hyperliquid.maintenance --fix-first-seen` between ticks after the deploy and **before 2026-11-26** (while staging still holds the full history since go-live) — tar HL_OUT_DIR first | none (same schema); the first lift after the deploy re-stamps every wallet's `l4_wallet_metadata` row once and the repaired token rows once | `RELEASE_TAG=v1.5.0 bash deploy/bootstrap.sh` (older code ignores the `meta` key and recomputes window-based rows; dimension rows go back to being re-stamped every run) |
| v1.7.0 | `_price_state.json` added (per-token candle cursors + ledger-seen stamps; delete it to re-fetch 32 days for every token); new staged `l3_token_price_usd_1h.csv` / `_1d.csv` (pruned by retention like the ledger); two new env keys `HL_PRICE_TIME_BUDGET_MIN=3` / `HL_PRICE_HISTORY_DAYS=32` (bootstrap appends them) | two new tables `hypercore.l3_token_price_usd_1h` / `_1d`, created from the DDL on the first lift (same shape as the 1m table), lifted FIRST in every run | `RELEASE_TAG=v1.6.0 bash deploy/bootstrap.sh` — the new tables, CSVs and state file are ignored by older code; v1.6.0's preflight only WARNs `env_unknown` about the two HL_PRICE_* keys (remove them from the env file or ignore) |
| v1.8.0 | none | nightly **compaction** (`maintenance --compact`, in `deploy/hypercore-maintenance.service`): closed, fragmented partitions (≥ 8 files; a partition is closed 2 UTC days after its date, 1 day for l4x) are rewritten into one file each, one commit per table per night, bounded by the next hourly tick minus a minute; `write.metadata.previous-versions-max` 20 → 100 on every table (set by the nightly run) so a pointer the ETL fetched stays readable through commit bursts | `RELEASE_TAG=v1.7.0 bash deploy/bootstrap.sh` (bootstrap rewrites the maintenance unit without `--compact`; compacted partitions stay compacted — they are ordinary data files) |

Several releases at once (e.g. v1.3.0 → v1.8.0 in one bootstrap) is supported: every
step is cumulative and idempotent (env keys are appended for all releases, `migrate
--apply` applies every pending migration, the units are those of the target tag).
Afterwards run the one-time repairs the skipped release notes list (v1.6.0's
`--fix-first-seen`). Rolling back below v1.5.0 still needs `migrate --revert --apply`
first.

## 9. Backup / recovery

The lake is the durable store. `HL_OUT_DIR` holds per-wallet watermarks, the
staged CSVs (last `HL_RETENTION_DAYS`), run summaries and the lending-position
cache. Losing it costs one re-pull: the next run treats every wallet as new
(`--days` window; API-capped wallets then need `--s3-backfill`) and the
insert-if-absent lift writes nothing that already exists. A nightly `tar` of
`HL_OUT_DIR` is enough.

## 10. Costs and limits to keep in mind

- **Hyperliquid API**: free; the only cost is the shared per-IP budget.
- **S3 node data**: Requester Pays egress (~1–1.5 GB per MM wallet-day of backfill). Only wallets whose window the API cannot serve trigger it, and one pass serves all of them.
- **Memory**: S3 hour files are parsed one at a time (≤ ~330 MB decompressed); `MemoryMax=3G` in the unit is a reasonable guard.
- **Time**: budget-bound. A 24h S3 window for an MM wallet ≈ 35 min; a whale with a first-time lending lookup adds up to ~10 min. `TimeoutStartSec=4h` is generous.

## 11. What this repo does NOT do

Still required in `atlas-project` / the platform before any HyperCore number is
user-visible (tracked in `docs/HYPERCORE_LEDGER_SPEC.md` §7):

1. Rust `wallet-analytics` / `fifo-pnl` / `control-jobs` arms for `{"chain":"hypercore"}` (watchlist gate counts only `event_type=='transfer'` today).
2. Trino view `all_chain.vw_wallet_analytics_live` UNION of `hypercore` (it hard-codes ethereum + arbitrum).
3. The etl export / `cockroachdb_sync` and a `chains` row in core-api's Postgres.
4. Registering the dagster asset (or keeping the systemd timer as the scheduler of record).
5. A 30-day calibration period on the chosen wallet segment before showing numbers to users.
