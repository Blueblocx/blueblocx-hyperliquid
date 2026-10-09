# Should MinIO be removed to relieve the bottleneck?

Assessment date: 2026-10-09. Scope: the BlueBlocx data stack in `atlas-project`
(production host `olympus`) as read from its configs, docs and git history. No
new measurements were taken — that is the first recommendation below.

> **Update, same day:** the stack is moving to a new host whose only bulk storage
> is an NFS-mounted HDD NAS. Sections 1–5 describe olympus and still hold for it;
> §6 below re-assesses the question for the new host with the measured NFS
> numbers, corrects two statements made above, and records what was verified
> against primary sources.

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

## 6. Update: the new host has only an NFS HDD (2026-10-09)

New facts from the user: the stack moves to `server4090-2`; its only storage able
to hold bulk data is the NAS mounted at `/mnt/blueblocx` over NFS; the host has
126 GB RAM (olympus had 31 GB). Measured on the mount with `dd`:

| test | result | what it is |
|---|---|---|
| `bs=1M oflag=direct` write, 4 GiB | **18.0 MB/s** (238.6 s = 58 ms per 1 MiB request) | per-request *stable-write latency*: each O_DIRECT write becomes an NFS `WRITE stable=FILE_SYNC` (or `WRITE`+`COMMIT`) that the NAS must flush to HDD before replying; ~9 ms wire + ~49 ms NAS commit, queue depth 1 |
| `bs=1M iflag=direct` read, 4 GiB | **110 MB/s** (38.95 s) | ≈ 93 % of the ~118 MB/s TCP goodput of 1 GbE: the *network link*, not the disk (the file had just been written and was likely served from NAS RAM) |

Neither number involves MinIO: `dd` on an NFS path exercises NFS client → NIC →
NAS → HDD and nothing else. Buffered write throughput (no `oflag=direct`),
`iperf3`, NIC speed and the NFS mount options have **not** been measured yet.

### 6.1 Corrections (verified against primary sources, 2026-10-09)

1. **"MinIO 2023+ explicitly rejects NFS at startup" is wrong** (atlas
   `setup/production/02-minio-NOT-SUPPORTED-NFS.md`, `minio/02-minio.md`,
   `setup/README.md`). MinIO's source has no filesystem-type gate: `drive access
   denied` is MinIO's mapping of `EACCES/EPERM` while reading or migrating
   `.minio.sys/format.json` (`cmd/xl-storage.go`, checked at
   `RELEASE.2024-11-07T00-52-20Z`; `NFS` appears only as a display label in
   `internal/disk/type_linux.go`; the O_DIRECT probe is skipped in single-drive
   mode). The 2026-03 failure on `/mnt/blueblocx/minio` was a uid/`root_squash`
   permission problem on the export (same symptom as minio/minio#16163 and
   #20810, where `chmod -R 777` "fixed" it). MinIO *would* start with
   permissions aligned — which is exactly why the ban must rest on the real
   reasons: MinIO's 2024 docs said "MinIO cannot provide consistency guarantees
   if the underlying storage volumes are NFS", maintainers answer NFS reports
   with "Do not use NFS" (#19924) / "For distributed setups we do not support
   NFS" (#18337), and the NAS's 18 MB/s synchronous write path is what MinIO's
   fsync-per-object write pattern would see. Reword the atlas docs; keep the
   verdict.
2. **MinIO lifecycle.** The `minio/minio` GitHub repository shows "archived by
   the owner on Apr 25, 2026 — THIS REPOSITORY IS NO LONGER MAINTAINED"; the
   community edition is source-only and the last binary is
   `RELEASE.2025-10-15T17-29-55Z`; the successor is AIStor. The binary the atlas
   runbooks download from `dl.min.io` is therefore frozen. This is a separate
   decision from the NAS question, but it removes "keep MinIO" as the long-term
   default anywhere.
3. §3.1 above said a `file://` warehouse "means replacing Nessie". Confirmed at
   tag `nessie-0.107.4`: `ResolvingObjectIO.resolve()` accepts only `s3/s3a/s3n`,
   `gs`, `abfs/abfss` and throws for anything else; `LocalObjectIO` is javadoc'd
   "purely for unit tests – and nothing else"; Nessie itself writes
   `metadata.json` through that ObjectIO, so a `file://` warehouse fails at the
   first commit. The Rust cost was **overstated**: the 13 writers' hand-rolled
   `CommitTableRequest` POSTs are storage-agnostic, and `iceberg-storage-opendal
   0.9` ships an `Fs` variant (feature `opendal-fs`, already compiled in), so the
   storage change is one enum swap per `catalog.rs`. The catalog change is the
   expensive part: if Nessie is replaced by a JDBC catalog the REST commit paths
   must be rewritten to `catalog.update_table` (`iceberg-catalog-sql 0.9`
   implements it); keeping REST needs a REST server that serves `file://`
   (`apache/iceberg-rest-fixture` = JdbcCatalog behind REST does, but is
   labelled a test fixture).

### 6.2 What "remove MinIO and SeaweedFS, read the files directly" requires

| component | change | verified detail |
|---|---|---|
| Catalog | replace Nessie | JDBC/SQL catalog on the existing PostgreSQL (Trino `iceberg.catalog.type=jdbc`, Spark `type=jdbc`, pyiceberg `type=sql`, iceberg-rust `iceberg-catalog-sql`), or a `file://`-capable Iceberg REST server in front of it. Nessie branches are unused (`NESSIE_BRANCH=main` everywhere), so nothing functional is lost |
| Trino | upgrade 465 → ≥ 471 | local filesystem support arrived in 471 (`fs.local.enabled=true`, `local.location=local:///…`); on 465 only the legacy `fs.hadoop` path reaches `file://`, which Trino says is "not recommended and will be removed". The implementing class is still javadoc'd "for testing"; root/absolute-path composition needs a 10-minute test. Same bind-mount path inside the container |
| Spark 3.5 + Iceberg 1.8.1 | config | HadoopFileIO handles `file://`; set `fs.file.impl=org.apache.hadoop.fs.RawLocalFileSystem`, otherwise `LocalFileSystem` (a `ChecksumFileSystem`) writes a `.crc` sidecar next to every file — orphans to Iceberg, double the inodes on NFS |
| 13 Rust crates | `OpenDalStorageFactory::S3` → `::Fs` in each `catalog.rs`; drop `s3.*` props; catalog builder per the catalog choice above | `opendal-fs` is in the default feature set; OpenDAL 0.55 normalises the `file:/` prefix quirk |
| python-ingestion | 10 `load_catalog` sites + the boto3 dashboard (list/delete objects → filesystem walks) | — |
| hyperliquid-poc | one more catalog branch in `iceberg_load.py` (`type=sql`) | small |
| Existing tables | rewrite absolute `s3://warehouse/…` paths in ~95 tables | `rewrite_table_path` exists in Iceberg 1.8.0+ (not 1.7.1; unsupported when partition-statistics files exist), then copy data, then `register_table` in the new catalog; the same `metadata.json` must never be registered in two catalogs |
| NFS hygiene | every engine host/container mounts the identical path | `hard`, `nconnect`, 1 MiB `rsize/wsize`, `lookupcache=positive` if more than one host writes; Spark shuffle, Trino spill, Postgres, SQLite and `HL_OUT_DIR` stay on local disk |

"Fastest" is true only relative to an S3 gateway on the same NFS path: it
removes the gateway's per-object metadata operations (MinIO: a directory +
`xl.meta` + part file per object) and one HTTP hop. It does not change the
physics: ~110 MB/s bulk reads (100 GB scan ≈ 15 min, 1 TB ≈ 2.5 h), ~50 ms per
stable write, 0.3–2 ms per NFS metadata operation (10–50× local) for every
Iceberg commit, planning step and `expire_snapshots`/`remove_orphan_files`.

### 6.3 If the S3 API is kept instead

Stores that tolerate an NFS directory and keep Nessie, Trino, Spark, Rust and
pyiceberg unchanged (endpoint + credential swap, bucket name `warehouse`
preserved, no metadata rewrite): **SeaweedFS** (append-only ~30 GB volume files
on the NAS, filer/master metadata on the local SSD, atomic conditional writes,
official Trino/Spark Iceberg guides, and already run by this team on the same
NAS in 2026-03) and **versitygw** (POSIX backend, objects are plain files 1:1,
explicitly NFS-aware; multipart completion copies the data, i.e. a second write).
Garage splits objects into 1 MiB blocks (bad on NFS); RustFS, Ceph RGW, the
JuiceFS gateway and Zenko are poor fits. None changes the NAS physics.

### 6.4 Recommendation for the new host (verified option analysis, 2026-10-09)

Nine options from three independent designs were scored and the recommendation's
seven load-bearing claims were each attacked against primary sources (Linux NFS
client/server source, SeaweedFS 4.48 source, Nessie 0.107.4 source, Trino docs,
atlas repo). Verdicts: 3 confirmed, 2 refuted (corrections folded in below),
2 uncertain (the unmeasured numbers).

**On "remove MinIO and SeaweedFS, read the files directly, that is fastest":**

- *True*: MinIO must go, and the NAS path is the new ceiling for every engine.
- *False*: direct files are not faster on this hardware and not a configuration
  change. Bulk scans are link-bound either way (~110 MB/s). For the small-file
  work that dominates Iceberg (3–4 files per commit, thousands of manifests per
  planning step, `expire_snapshots` / `remove_orphan_files` listings) a gateway
  whose index lives on the local SSD answers HEAD/LIST with **zero NFS round
  trips** and writes an object as an append to an already-open volume file,
  while a plain file costs CREATE + WRITE + COMMIT + GETATTR over the wire at
  0.3–2 ms each. And the stack cannot read `file://` without replacing Nessie,
  upgrading Trino to ≥ 471, editing 13 Rust crates, 10 pyiceberg sites, Spark
  and the dashboard, and rewriting ~95 tables' paths (§6.2) — 1–3 weeks of work
  to land on the same wire with D-state hangs instead of HTTP errors when the
  NAS blips.

**First question to settle — it can make this whole section moot:** is "the NAS
is the only bulk storage" a physical fact or a policy? olympus has a 3.6 TB NVMe
(`nvme1n1`, `/data2`, not in the LVM group) and the warehouse is bounded by the
1.3 TB `/data` that filled on 2026-04-28. If that drive, or any ≥ 2 TB NVMe, can
go into `server4090-2`, the lake stays on NVMe with **zero software change**,
the NAS becomes the backup tier, and every number below improves 20–30×. Check
`dmidecode -t slot`, `ls /sys/class/nvme`, `lspci | grep -i nvme` on the new
host and `lsblk /dev/nvme1n1` on olympus before anything else.

**Recommended shape if the lake must live on the NAS (scored 7.6/10; nothing
scores higher because the NAS caps everything):**

| element | decision | why / verified detail |
|---|---|---|
| API + catalog | keep the S3 API and Nessie 0.107.4 exactly as they are | zero code change, no table/path rewrite. Host/endpoint values (`nessie.properties` endpoint + external-endpoint, Trino `BLUEBLOCX1_HOST`, both compose `AWS_ENDPOINT_URL`, Spark `AWS_*`) are rewritten on a new host anyway |
| store | **SeaweedFS ≥ 4.45** (4.48 current), one `weed server -filer -s3` process on the host, bucket `warehouse` on `:9000`, same S3 key values as olympus MinIO | append-only ~GB volume files fit an HDD NFS export; HEAD/LIST served from the local index; multipart completion is metadata-only; official Trino/Spark Iceberg guides; this team ran it on this NAS in 2026-03. **≥ 4.45 matters**: the S3 path only honours the fsync rule from that release; aws-chunked CRC uploads from Nessie's AWS SDK 2.42 are handled from the Dec-2025 fixes |
| data placement | volume `.dat` files on `/mnt/blueblocx/seaweedfs`; master dir and `.idx` on the local SSD | SSD placement is a latency optimisation, not a correctness requirement (`.idx` is rebuilt by `weed fix`; master state is soft) |
| namespace durability | **filer store = `postgres2` in the same PostgreSQL that holds Nessie** | the filer store is the only irreplaceable SeaweedFS state (S3 chunks are stored nameless); the default leveldb2 store writes with `Sync=false`, so a host crash can lose the key→chunk map for recent objects while Nessie's pointer is already durable. PostgreSQL closes that gap and one `pg_dump` captures catalog + namespace in one transaction snapshot |
| data durability | `fs.configure -locationPrefix=/buckets/warehouse/ -fsync -apply` before the first production write | with it, chunk bytes are fsynced before the S3 200 (source-verified at 4.48: filer `rule.Fsync` → `?fsync=true` → volume `DataBackend.Sync()`); default is no fsync. Cost: one ~50 ms stable write per chunk, so a 512 MiB Parquet takes ~11 s instead of ~5 s for a single sequential writer |
| volumes | `-master.volumeSizeLimitMB=8000`, `-volume.compactionMBps=40`, `-volume.max=0` | an 8 GB volume rewrites in ~73 s read + ~73 s write at link rate; vacuum after compaction/expire moves whole volumes across the wire, so keep them small and throttled |
| ports | pick a free volume port and record the map | the plan's first choice (8082) collides: 8080 Spark master UI, 8081+ Spark worker UIs, 8083 Trino, 8000 Python API, 8001 ingestion, 3000 Dagster, 19120/9002 Nessie, 5432 PG, 9000 S3, 9333 weed master, 8888 weed filer |
| fallbacks | versitygw (plain files 1:1, ~2× write cost, explicitly NFS-aware) if SeaweedFS fails the Trino/iceberg-rust smoke test; iSCSI LUN + XFS under the store if NFS semantics misbehave (single-initiator only) | both keep the zero-code property |
| upgrade path | when a ≥ 1 TB local NVMe appears: `-dir=/nvme,/mnt/... -volume.disk=ssd,hdd` + `fs.configure … -disk=ssd` + scheduled `volume.tier.move` | a restart, not a migration |

**Measurement gate (run before installing or copying anything; every row must pass):**

| test | command | threshold |
|---|---|---|
| host identity, local disk, kernel | `hostnamectl; lsblk -d -o NAME,ROTA,SIZE,MODEL,TRAN; df -h / /var/lib; free -g; nproc; uname -r` | **≥ 300–400 GB free local SSD** (Spark shuffle alone used 600 GB+ on a full pass, ~50 GB per monthly chunk; a 363 GB root was too small on olympus); kernel ≥ 5.3 for `nconnect` |
| mount options | `grep blueblocx /proc/mounts; nfsstat -m` | `vers` ≥ 4.1, `rsize=wsize=1048576`, `hard`, **no `sync`** |
| link | `ethtool <if>`; `iperf3 -c <nas> -t 20`, `-R`, `-P 4` (`iperf3 -s` on the NAS) | ~940 Mbit/s both ways = 1 GbE confirmed and 110 MB/s is the wire; 2.3/9.4 Gbit/s = the NAS disks/CPU are the ceiling, examine RAID first |
| buffered write, incompressible, larger than NAS RAM | `fio --name=bufw --directory=/mnt/blueblocx --size=20G --rw=write --bs=1M --ioengine=psync --direct=0 --end_fsync=1 --buffer_pattern=random --refill_buffers --group_reporting` | ≥ 90 MB/s go; 30–90 investigate export/mount; **≤ 30–40 stop** (client `sync`, SMR drives in steady state, degraded RAID): fix the NAS before moving anything |
| latency vs bandwidth | `dd … bs=16M count=256 oflag=direct`; 8 parallel `dd … bs=1M count=512 oflag=direct` | ~85–90 MB/s / aggregate 60–110 = per-commit latency only (benign); still ~18–25 = the array itself is slow |
| fsync IOPS, metadata tax | `fio … --rw=randwrite --bs=4k --direct=1 --fsync=1 --iodepth=1`; 5 000 small-file create/ls/rm vs `/tmp` | 15–40 IOPS expected (= cost of every SeaweedFS chunk fsync); 10–50× slower than local expected |
| permissions as the service uid | `sudo -u <weed-user> sh -c 'mkdir -p /mnt/blueblocx/seaweedfs && touch /mnt/blueblocx/seaweedfs/.w && ls -ln /mnt/blueblocx/seaweedfs'` | file owned by the service uid, not `nobody` — fix on the export (`anonuid`/`no_root_squash`), never `chmod -R 777` (the 2026-03 "drive access denied" was exactly this) |
| NAS side (ssh) | `cat /proc/mdstat; smartctl -i /dev/sd?; mount; cat /etc/exports; free -g; df -h; which docker iscsiadm` | drive models CMR vs SMR; RAID health; `sync`/`async`; NFS 4.2; iSCSI/Docker availability (opens the on-NAS store option) |
| source size | on olympus `mc du local/warehouse; mc ls --recursive local/warehouse \| wc -l; df -h /data /data2` | copy time = bytes / 105 MB/s (buffered) … bytes / 18 MB/s (sync) + objects / 2 000 s |
| real commit rate | Trino `SELECT count(*) FROM blueblocx.ethereum."raw_logs$snapshots" WHERE committed_at > current_timestamp - interval '1' day` | the 1-minute QuickNode writer is `default_status=STOPPED` in code; the running writer is the 6-hourly BQ ingest. ~4/day means the small-file pressure is far lower than assumed |

**RAM plan for 126 GB (peak = Trino + Spark + one Rust job):** OS/Docker 4 ·
Trino 42 (`-Xmx40G`, `query.max-memory-per-node=28GB`; was `-Xmx29G` on a 30.8 GB
host) · Spark 26 while a job runs (executor 20–24 g, driver 4 g; raise
`SPARK_WORKER_MEMORY` with it) · Nessie 3 · PostgreSQL 4 · SeaweedFS 4 · Dagster +
python-ingestion + python-api 8 · Rust 8 transient → ≥ 27 GB of page cache at
peak, 50–60 GB when Spark is idle. The page cache is the NFS client cache shared
by every engine on the one host: all Iceberg metadata and the current day's
Parquet stay resident after first touch (each cached GB saves ~9 s of wire time
per re-read), and it is what turns application writes into pipelined
`UNSTABLE` writes instead of the 18 MB/s synchronous path. Guard it with
`vm.dirty_background_bytes=268435456`, `vm.dirty_bytes=1073741824` (bounds a
COMMIT stall to ~10 s), `vm.swappiness=10`, `vm.vfs_cache_pressure=50`; no
swapfile on the NAS. RAM does nothing for cold scans, first-touch reads or
per-commit latency. Trino's own repeat-read cache (`fs.cache`) wants a local
SSD, which competes with Spark shuffle for the same scarce disk.

**Migration (condensed):**

1. *Prepare (T-7d):* build the host from the `minio-olympus` runbooks with
   SeaweedFS in place of `02-minio.md`; identical Nessie 0.107.4 with
   `nessie.version.store.type=JDBC` (the current docs recommend JDBC2, whose
   tables `refs2/objs2` differ — a mismatch yields an empty catalog on top of the
   restored rows); PostgreSQL, Spark shuffle, Trino spill, Docker, `HL_OUT_DIR`
   on the local SSD; `fstab`: `vers=4.2,hard,proto=tcp,nconnect=8,rsize=1048576,wsize=1048576,noatime,_netdev`;
   smoke test Trino / Spark / one Rust crate (the only test of iceberg-rust 0.9
   OpenDAL list semantics against SeaweedFS) / pyiceberg / the HL loader on a
   scratch namespace; backup routine (`pg_dump`, `fs.meta.save`) working.
2. *Migrate:* bulk `mc mirror` while olympus is live; then freeze the **Dagster
   schedules** (ethereum 02:00, arbitrum 04:00, cross-chain 06:00 UTC, weekly
   Sunday jobs, BQ ingest every 6 h — "Rust timers" do not exist, Rust runs via
   Dagster → Python API) and the HL timer; delta mirror; `pg_dump`/`pg_restore`
   of Nessie (state is entirely in `refs`/`objs`, every location is
   `s3://warehouse/…`, 82/82 in `TRINO_SCHEMA.sql`); validate with
   `$snapshots` ids and small counts, never a full scan; keep olympus
   read-only for 14 days. Because the product website reads CockroachDB, a
   24–48 h lake freeze costs only data freshness — take the time instead of a
   risky 1–2 h delta under pressure.
3. *Operate:* daily `rewrite_data_files` (binpack 512 MB) on hot tables, daily
   `expire_snapshots`, weekly `remove_orphan_files` (LIST is filer-local);
   nightly `pg_dump` (catalog + namespace); monitor `nfsiostat` WRITE/COMMIT RTT,
   writable volumes, NAS free space ≥ 3 volume sizes, local SSD usage.
4. *Decide before moving:* the pending ~18 B-row L1 re-parse (4–6 days on NVMe
   per `TODO.md`) runs 2–3× longer on the NAS and churns SeaweedFS vacuum — run
   it on olympus first or accept the cost. Move all secrets and verify outbound
   reach (CockroachDB, BigQuery, QuickNode, Moralis, Hyperliquid) from the new
   host.

**Numbers to plan with (ranges until the gate is run):** copy of a 1.3 TB
warehouse 3.4 h (buffered) to 20 h (sync path), ~9 h on SMR-class 40 MB/s;
daily 50 GB Spark write 8–46 min; 512 MiB Parquet 5–30 s (+~6 s with per-chunk
fsync); cold scan 100 GB ≈ 15 min, 1 TB ≈ 2.5 h; `expire`/orphan listings
seconds (filer-local) vs minutes on plain NFS files.

**Hyperliquid pipeline on this host:** `NESSIE_URI=http://localhost:19120/iceberg`,
`S3_ENDPOINT=http://localhost:9000`, scoped S3 key, `HL_OUT_DIR` on the local
SSD; the two blockers the verification found (FileIO needing `s3fs`, full-table
count scan on every run) are fixed in commit `9c505d3` — an older checkout
would reintroduce them.
