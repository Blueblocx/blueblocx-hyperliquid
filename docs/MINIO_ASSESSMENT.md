# Should MinIO be removed to relieve the bottleneck?

Assessment date: 2026-10-09. Scope: the BlueBlocx data stack in `atlas-project`
(production host `olympus`) as read from its configs, docs and git history. No
new measurements were taken — that is the first recommendation below.

## Verdict

**Not now.** Nothing in the repo measures MinIO as the bottleneck; the documented
pain points trace to memory oversubscription and to everything sharing one NVMe
(swap, Spark shuffle, MinIO data), plus a small-file problem that no scheduled
compaction addresses. Removing MinIO is also not a config change: the Nessie REST
catalog needs an object store, every engine is wired to the S3 API, and ~95
tables carry absolute `s3://warehouse/...` paths in their metadata. Measure for a
day, fix the three cheap things, then revisit with numbers.

## 1. What MinIO is in this stack

| fact | source |
|---|---|
| Single-node, **single-drive** MinIO (`minio server /data/blueblocx/minio`) as a systemd unit on `olympus`: 24 cores, 30.8 GB RAM, 96 GB swapfile on NVMe; `/data` 1.3 TB (nvme0n1), `/data2` 3.6 TB (nvme1n1) | `setup/production/minio-olympus/01-prerequisites.md`, `02-minio.md`, `.claude/commands/infra.md` |
| It is the **only** store for the Iceberg warehouse `s3://warehouse/` — ~95 tables across `ethereum`, `arbitrum`, `all_chain`, `control`; `l1_wallet_ledger` ≈ 12 B (ethereum) + 6 B (arbitrum) rows | `TRINO_SCHEMA.sql`, `TODO.md` |
| Clients: Nessie (writes table metadata), Trino 465 (reads, `DELETE`, view metadata), Spark 3.5 (ETL writes + the only compaction/expiry tooling), 13 Rust crates (iceberg-rust 0.9 + OpenDAL **S3, hard-coded**), python-ingestion (pyiceberg: `raw_logs` append **every minute**), a boto3 dashboard (`list_objects_v2` / `delete_objects` for orphan cleanup) | `services/rust/*/src/catalog.rs`, `services/python-ingestion/src/app.py`, `services/spark-jobs/shared/catalog.py`, Trino catalog template |
| Co-located on the same box with Trino (`-Xmx29G`, `query.max-memory=20GB`), Spark worker (20 GB / 16 cores), Nessie, PostgreSQL, Dagster, python-ingestion (4 GB) → planned ~61 GB on 31 GB RAM, i.e. ~30 GB of swap by design | `infra.md` memory budget, Trino `jvm.config` |
| Not on AWS. No Terraform/CI. Containers reach MinIO through the Docker bridge (`172.17.0.1:9000`), host processes via `localhost:9000` | `docker-olympus/*/.env.example`, `03-nessie.md` |

## 2. Evidence in the repo of where time actually goes

- "running both chains' L1 re-parse in parallel **doubles I/O contention on the same MinIO/Nessie host**" — `TODO.md`.
- Rust source scans capped at 4 concurrent data files "to limit memory pressure and **avoid overloading MinIO**" — `fifo-pnl/README.md`.
- Spark S3 connection pool exhausted at 50 under 20 concurrent tasks → raised to 200 (commits 2026-04-01). Fixed.
- **Small files**: AQE produced ~22 MB files; `rewrite_data_files` (binpack 512 MB) exists but is **manual** (dashboard button / one-off scripts), no schedule. `raw_logs` gets one pyiceberg commit per minute → ≥1,440 snapshots and ≥1,440 small Parquet files per day until someone compacts.
- A **MinIO 507 (disk full)** incident on 2026-04-28 led to direct S3-API orphan deletion tooling. `/data` (1.3 TB) filled while `/data2` (3.6 TB) sits idle.
- The dominant theme in git history is **memory**: Trino heap 6 → 16 → 24 → 29 GB, 96 GB swap added, repeated OOM fixes in Spark jobs and Rust crates ("25 GB peak on a 30 GB host").
- **No benchmark anywhere**: no MinIO latency/throughput figures, no Trino `physical input` rates, no document comparing MinIO with S3 or a local filesystem. The only alternative ever set up (SeaweedFS) was for NFS compatibility, not speed, and was abandoned.

So the recommendation to remove MinIO is a hypothesis, not a finding.

## 3. Why "remove MinIO" is a migration, not a config change

1. **The catalog needs an object store.** Nessie's Iceberg REST catalog is configured per warehouse with S3/GCS/ADLS options (it also writes table metadata files itself). A `file://` warehouse means replacing Nessie (JDBC/SQL catalog), losing branches, and reconfiguring every engine's catalog — a different project.
2. **Absolute paths in metadata.** Iceberg manifests store the full `s3://warehouse/...` path of every data file. A scheme change needs `rewrite_table_path` / re-registration for ~95 tables, or a rewrite of the data (the ledger re-parse is estimated at 40–80 h per chain in `TODO.md`).
3. **Hard-wired S3 clients.** `OpenDalStorageFactory::S3` in 13 Rust crates, `S3FileIO` + `s3a` in Spark, `fs.native-s3` in Trino, `s3.*` props in pyiceberg, boto3 in the dashboard.
4. **Network reachability.** Trino, Dagster and python-ingestion run in containers; a filesystem warehouse must be bind-mounted at the identical path in every container and host process (and across hosts in the two-node variant).
5. **Same disk, same bytes.** On a single NVMe the Parquet bytes are read from the same device either way. What disappears is the loopback HTTP hop and MinIO's per-object layout (each object = a directory with `xl.meta` + a part file → extra metadata ops, which matter for thousands of small manifest/metadata files, not for 512 MB data files). That is CPU and small-request overhead, not the I/O ceiling the TODO complains about.

## 4. What to do instead (ordered by expected gain / cost)

1. **Measure for one heavy day** (a few hours of work, no risk). During the daily Spark/Rust jobs and a slow Trino query, record:
   - `iostat -x 1` on nvme0n1 / nvme1n1 (`%util`, `r_await`, `w_await`) and `vmstat 1` (`si`/`so`). Swap-in/out > 0 while jobs are slow = memory, not storage.
   - MinIO: `mc admin trace --stats` or its Prometheus endpoint (request rate, TTFB, 5xx/503 "slow down").
   - Trino: `physical input bytes/s`, `blocked time`, `peak memory` per slow query (`system.runtime.queries`); Spark stage input/shuffle sizes and GC time.
   - Iceberg: `SELECT count(*) FROM "<table>$files"`, `$snapshots` for `raw_logs` and `l1_wallet_ledger`; average file size.
2. **Stop swapping.** 61 GB planned on 31 GB RAM is the single most likely cause of "I/O contention": the swapfile competes with MinIO data and Spark shuffle for the same NVMe. Either cap Trino + Spark to fit (e.g. 12 GB + 12 GB, jobs serialized — Dagster already runs one heavy Spark job at a time) or add RAM; RAM is cheaper than any storage migration.
3. **Separate the I/O paths.** Move the swapfile and `spark-tmp` (or the MinIO data directory) to the idle 3.6 TB nvme1n1. This also ends the 507 disk-full exposure.
4. **Schedule maintenance.** A weekly Dagster job per namespace: `rewrite_data_files` (binpack, 512 MB), `expire_snapshots`, `remove_orphan_files`; set `write.target-file-size-bytes` explicitly; batch `raw_logs` commits to every 5–10 minutes (or compact hourly). Fewer files → fewer GETs, faster planning in Nessie/Trino/Rust, smaller MinIO metadata footprint.
5. **Only if MinIO still shows up** after 1–4 (high TTFB on small GET/PUT, request queueing): the cheap lateral move is another S3-compatible store with the **same bucket layout** (`mc mirror` → change the endpoint; no engine or metadata changes). A `file://` warehouse only makes sense together with a catalog change, planned as its own project with a measured target.

## 5. For the HyperCore pipeline specifically

This repo appends to 7 tables once a day (megabytes). Its bottlenecks are the
Hyperliquid per-IP weight budget (1200/min, shared with the TS backend) and
parsing S3 node-data hour files. Nothing here gets faster by removing MinIO; the
pipeline reads the same `NESSIE_URI` / `S3_ENDPOINT` variables as the Rust
services, so whatever the warehouse is, it follows.
