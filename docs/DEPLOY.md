# Deploying the HyperCore pipeline

Runbook for putting `hyperliquid-poc` on a Linux server as a scheduled batch job.
It is a **batch job, not a service**: once a day it pulls each watched wallet's
Hyperliquid history, validates it against Hyperliquid's own portfolio numbers,
and lifts the rows into Iceberg (`nessie.hypercore.*`). Serving those rows to the
product (Rust L4 crates, Trino view, Postgres export, core-api) is work in the
`atlas-project` repo — see [§11](#11-what-this-repo-does-not-do).

Validated on 39 live wallets (6 account shapes); the code and its 100 unit tests
run on Linux (Ubuntu 24.04 / Python 3.12.3) and Windows (Python 3.14); CI covers
Python 3.12 + 3.13 and the Docker image build.

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

```bash
sudo useradd --system --create-home --home-dir /var/lib/hypercore --shell /usr/sbin/nologin hypercore
sudo git clone <your-remote> /opt/hyperliquid-poc
sudo chown -R hypercore:hypercore /opt/hyperliquid-poc
sudo -u hypercore bash -c '
  cd /opt/hyperliquid-poc &&
  python3 -m venv .venv &&
  .venv/bin/pip install -r requirements.txt -c constraints.txt &&
  .venv/bin/python -m unittest discover -s tests -t .'
```

`constraints.txt` pins the full transitive closure the pipeline was validated
with; `requirements.txt` alone would float the indirect packages.

## 3. Configure

```bash
sudo mkdir -p /etc/hypercore
sudo cp /opt/hyperliquid-poc/deploy/.env.example /etc/hypercore/hypercore.env
sudo chown root:hypercore /etc/hypercore/hypercore.env && sudo chmod 640 /etc/hypercore/hypercore.env
sudoedit /etc/hypercore/hypercore.env            # fill in the values
printf '%s\n' 0x... 0x... | sudo tee /etc/hypercore/wallets.txt     # one address per line, # comments ok
```

Every variable is documented in `hl_poc/config.py` (`ENV_VARS`); the important ones:

| variable | set it to |
|---|---|
| `HL_OUT_DIR` | `/var/lib/hypercore` — the state directory (watermarks, staged CSVs, run summaries) on a local disk, never the NAS mount. Back it up or accept re-pulling on loss (see §9) |
| `HL_WEIGHT_BUDGET` | `600` if anything else on this IP polls Hyperliquid, else up to `900` |
| `HL_RETENTION_DAYS` | `45` (≥ 31; the 30-day analytics windows read the staged CSV) |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | the new scoped key from §0 |
| `NESSIE_URI`, `NESSIE_BRANCH`, `WAREHOUSE`, `S3_ENDPOINT`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_REGION` | the same values the atlas Rust services use (`services/rust/*/.env.example`). Leave `NESSIE_URI` empty **only** for a dry run: it switches to a local SQLite catalog under `HL_OUT_DIR/iceberg` that nothing else can read |
| `HL_ALERT_WEBHOOK_URL` | a Slack or Discord incoming webhook. Problems are POSTed after every failing run |
| `HL_LOG_JSON` | `1` when journald/a log shipper collects stderr |

The `.env` loader never overrides variables already in the process environment,
so `EnvironmentFile=` (systemd) or `--env-file` (Docker) work unchanged.

## 4. Preflight

```bash
sudo -u hypercore HL_ENV_FILE=/etc/hypercore/hypercore.env \
  /opt/hyperliquid-poc/.venv/bin/python -m hl_poc.preflight --iceberg --s3 --wallets-file /etc/hypercore/wallets.txt
```

Expected: every line `OK` or `WARN`, exit 0. It never prints secret values.

| check | FAIL means |
|---|---|
| `deps`, `tzdata`, `ddl` | the venv is incomplete (`pip install -r requirements.txt -c constraints.txt`) or `HL_DDL_PATH` is wrong |
| `out_dir` | `HL_OUT_DIR` is not writable by the `hypercore` user. `WARN … network filesystem` means it sits on NFS/CIFS: move it to a local disk |
| `weight_budget`, `retention` | out-of-range values (budget > 1200 is above HL's cap; 0 < retention < 31 would truncate the 30d windows) |
| `wallets` | a line is not a 40-hex address |
| `hl_api` | no route to `api.hyperliquid.xyz` (egress firewall / proxy) |
| `clock` (WARN) | host clock > 60 s off HL's — fix NTP before scheduling |
| `s3` | the IAM key is invalid/rotated, or the policy lacks `s3:GetObject` with requester-pays; `--s3` makes a missing key a FAIL |
| `iceberg` | the REST catalog is unreachable or rejects the credentials; `--iceberg` makes "no `NESSIE_URI`" a FAIL. `WARN missing […]` is fine on the first run (tables are created from the DDL) |

## 5. First run — small, then full

Run two or three wallets for a two-day window and read the evidence before
scheduling anything:

```bash
cd /opt/hyperliquid-poc
sudo -u hypercore HL_ENV_FILE=/etc/hypercore/hypercore.env .venv/bin/python -m hl_poc.extract \
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
sudo -u hypercore HL_ENV_FILE=/etc/hypercore/hypercore.env .venv/bin/python -m hl_poc.daily \
  --wallets-file /etc/hypercore/wallets.txt --iceberg --s3-backfill
echo $?            # 0 clean · 1 problems (see _run_summary.json) · 2 config · 3 lock held
```

Verify from the lake side too (Trino / Spark SQL):

```sql
SELECT count(*), min(block_date), max(block_date) FROM nessie.hypercore.l1_wallet_ledger;
SELECT * FROM nessie.control.table_watermarks WHERE namespace = 'hypercore';
```

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

**systemd (preferred)** — the service runs preflight first, then the daily run;
timers never overlap a running service:

```bash
sudo cp deploy/hypercore-daily.service deploy/hypercore-daily.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now hypercore-daily.timer
sudo systemctl start hypercore-daily.service          # run once now
journalctl -u hypercore-daily -f
```

00:20 UTC is deliberate: Hyperliquid's midnight dust conversions have settled and
the 23:00 S3 hour file (published ~00:02) exists, so the previous day is complete.

**cron** — `deploy/run_daily.sh` + `deploy/crontab.example` (uses `flock`).

**Docker** — `docker build -t hl-poc .`; state in a volume mounted at `/data`
(`wallets.txt` at `/data/wallets.txt`); schedule the `docker run` from host cron
or an orchestrator. `ENTRYPOINT` is `python -m`, so
`docker run --rm --env-file /etc/hypercore/hypercore.env -v hypercore-data:/data hl-poc hl_poc.preflight`
works too.

**Dagster** — `deploy/dagster_hypercore_assets.py` is a template asset that runs
the same command as a subprocess; register it in the atlas dagster project.

Add the shared L3 price job after the daily run if the product needs
`l3_token_price_usd_1m` for HyperCore tokens (`crontab.example` has the line).

## 7. Monitor

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
| `error:…` | the run raised | `journalctl -u hypercore-daily` has the traceback |

`warnings` (`pnl_oracle`, `unmapped_ledger_types`, `unpriced`) do not fail the
run but are in the alert text; promote `pnl_oracle` to hard once it has been
stable for a few weeks on your wallet set.

Logs go to stderr (journald under systemd); `HL_LOG_JSON=1` for one JSON object
per line.

## 8. Upgrade

```bash
cd /opt/hyperliquid-poc && sudo -u hypercore git pull --ff-only
sudo -u hypercore .venv/bin/pip install -r requirements.txt -c constraints.txt
sudo -u hypercore .venv/bin/python -m unittest discover -s tests -t .
sudo -u hypercore HL_ENV_FILE=/etc/hypercore/hypercore.env .venv/bin/python -m hl_poc.preflight --iceberg --s3
sudo systemctl start hypercore-daily.service && journalctl -u hypercore-daily -n 50
```

Rollback is `git checkout <previous tag>` + the same pip/preflight steps. State
files (`_watermarks.json`, staged CSVs) are forward- and backward-compatible
across versions of this repo so far; a release note will say if that changes.

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
