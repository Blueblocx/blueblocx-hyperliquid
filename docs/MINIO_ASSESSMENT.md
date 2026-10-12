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
| blueblocx-hyperliquid | one more catalog branch in `iceberg_load.py` (`type=sql`) | small |
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
| store | **SeaweedFS ≥ 4.45** (4.48 current), one `weed server -filer -s3` process on the host, bucket `warehouse` on `:9000`, same S3 key values as olympus MinIO | append-only ~GB volume files fit an HDD NFS export; HEAD/LIST served from the local index; multipart completion is metadata-only; official Trino/Spark Iceberg guides; this team ran it on this NAS in 2026-03. **≥ 4.45 matters**: the S3 path only honours the fsync rule from that release. **4.48 still lacks the trailing-checksum fix #11583** (see §7.3): set `chunked-encoding-enabled=false` in `nessie.properties` or use a weed build after 2026-10-03 |
| data placement | volume `.dat` files on `/mnt/blueblocx/seaweedfs`; master dir and `.idx` on the local SSD | SSD placement is a latency optimisation, not a correctness requirement (`.idx` is rebuilt by `weed fix`; master state is soft) |
| namespace durability | **filer store = `postgres2` in the same PostgreSQL *database* that holds Nessie** (a separate schema is fine; `pg_dump` snapshots are per database) | the filer store is the only irreplaceable SeaweedFS state (S3 chunks are stored nameless); the default leveldb2 store writes with `Sync=false`, so a host crash can lose the key→chunk map for recent objects while Nessie's pointer is already durable. PostgreSQL closes that gap and one `pg_dump` captures catalog + namespace in one transaction snapshot |
| data durability | `fs.configure -locationPrefix=/buckets/warehouse/ -fsync -apply` before the first production write | with it, chunk bytes are fsynced before the S3 200 (source-verified at 4.48: filer `rule.Fsync` → `?fsync=true` → volume `DataBackend.Sync()`); default is no fsync. Cost: one ~50 ms stable write per chunk, so a 512 MiB Parquet takes ~11 s instead of ~5 s for a single sequential writer |
| volumes | `-master.volumeSizeLimitMB=8000`, `-volume.compactionMBps=40`, `-volume.max=0` | an 8 GB volume rewrites in ~73 s read + ~73 s write at link rate; vacuum after compaction/expire moves whole volumes across the wire, so keep them small and throttled |
| ports | volume port **8380**; record the map | 8080 Spark master UI, 8081+ Spark worker UIs, 8083 Trino, 8000 Python API, 8001 ingestion, 3000 Dagster, 19120/9002 Nessie, 5432 PG, 9000 S3, 9333 weed master, 8888 weed filer, 9327 metrics |
| fallbacks | versitygw (plain files 1:1, ~2× write cost, explicitly NFS-aware) if SeaweedFS fails the Trino/iceberg-rust smoke test; iSCSI LUN + XFS under the store if NFS semantics misbehave (single-initiator only) | both keep the zero-code property |
| upgrade path | when a ≥ 1 TB local NVMe appears: `-dir=/nvme,/mnt/... -volume.disk=ssd,hdd` + `fs.configure … -disk=ssd` + scheduled `volume.tier.move` | a restart, not a migration |

**Measurement gate (run before installing or copying anything; every row must pass):**

| test | command | threshold |
|---|---|---|
| host identity, local disk, kernel | `hostnamectl; lsblk -d -o NAME,ROTA,SIZE,MODEL,TRAN; df -h / /var/lib; free -g; nproc; uname -r` | **≥ 300–400 GB free local SSD** (Spark shuffle alone used 600 GB+ on a full pass, ~50 GB per monthly chunk; a 363 GB root was too small on olympus); kernel ≥ 5.3 for `nconnect` |
| mount options | `grep blueblocx /proc/mounts; nfsstat -m` | `vers=4.1` (DSM's maximum), `hard`, **no `sync`**; `rsize/wsize` as high as the model negotiates (1 MiB; a 2 GB Synology gives 512 KiB) |
| sync vs async export | `sudo nfsiostat 5 /mnt/blueblocx` **while** `dd if=/dev/zero of=/mnt/blueblocx/d1.bin bs=1M count=1024 oflag=direct` runs | `COMMIT` avg RTT ~50 ms = sync export (latency-bound, benign); sub-ms `COMMIT` with still ~18 MB/s = DSM "Enable asynchronous" is on **and the array itself is slow** (stop; see §7.3) |
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
   on the local SSD; `fstab`: `vers=4.1,hard,proto=tcp,nconnect=8,rsize=1048576,wsize=1048576,noatime,_netdev`
   (4.1 is DSM's maximum; `rsize/wsize` negotiate down on small-RAM models);
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

## 7. An hourly pipeline on server4090-2 (verified analysis, 2026-10-09)

Goal stated by the owner: the pipeline must work and be as fast as possible,
updating every hour, on `server4090-2`: 126 GB RAM, one 421 GB NVMe with
**29 GB free**, the Synology NAS (35 TB, 31 TB free) over NFS, link speed not
yet confirmed. Method: three fact-finders (atlas hourly readiness read from
source, Synology DSM capabilities, host disk/RAM budget), three designs (no new
hardware / one NVMe / network and NAS upgrades), a judge, six adversarial
verifications (1 confirmed, 3 refuted with corrections, 2 unmeasured) and a
completeness critic. Corrections are folded in below.

### 7.1 Verdict

**Hourly is not achievable with the atlas code as it is, on any storage.** The
number of bytes each cycle moves is set by the code, not by the disk. Verified
in source:

- ~15 unconditional "find the table's min/max date" scans that read a whole
  column of a whole table with no filter (`load_extreme_date`: `token_supply.rs:130`,
  `wallet-statement-watchlist core.rs:275-277` — the 18 B-row ledger twice per
  chain — `wallet-networth core.rs:451-463`, `wallet-statement-usd core.rs:300-307`,
  `wallet-trades-usd core.rs:191-203`, `token_price_1m.rs:192-193`). Cost per scan
  ≈ data-file count × (0.5 MiB footer prefetch + 1–3 GETs at 10–20 ms cold) / 4
  concurrency, so it scales with file count, not bytes.
- Full-table rewrites every run: `l4_wallet_balance_1d_dense`
  (`wallet-balance-1d core.rs:69-90`), `l4_wallet_networth_1d` and `_dense`
  (`wallet-networth core.rs:55-88`), `l4_token_wallet_sharpe_1d`
  (`token-wallet-sharpe core.rs:164-176`) — the heaviest bytes of a cycle.
- `wallet_metadata` rebuilds from the full history of `l3_wallet_statement_watchlist`
  every run (`wallet_metadata.rs:77-118`; "521 7-day chunks" per commit abde695).
- 7-day lookbacks everywhere (`parse-raw core.rs:253`, `dex/core.rs:255`,
  `wallet-networth core.rs:465`, `SPARK_AUTO_LOOKBACK_DAYS=7`).
- Daily jobs still read the full `l3_token_price_usd_1m` with no date filter
  (`wallet-analytics core.rs:472-476`) and the serving views are full-table
  (`vw_wallet_token_stats`, `vw_wallet_transactions`); the Trino → CockroachDB
  sync that feeds the website is in neither repo (removed in e4e18bb) and must be
  found before "hourly" can include the product.

Magnitude is **unmeasured** — no incremental run duration or `$files` size exists
anywhere in the repo. The one measured datapoint (commit 1c28ed3: 2 s per 5-day
single-column ledger chunk on NVMe) alone puts a full ledger scan at 10–25 min
on NVMe. So the code work is a precondition at any storage speed, including the
NVMe olympus has today.

**Size of the fix: days to ~2 weeks for one engineer**, not weeks: window
resolution from `control.table_watermarks` or from partition values in
`plan_files()` (~20 lines per crate; 4 crates already do it); the range-replace
writer already exists (`control-jobs/src/writer.rs:389-612`) and needs porting to
3 crates; 409 retry on the whole-table watermark overwrite (3 sites); gate the
weekly full passes (`wallet-balance core.rs:38-56`, `statement-watchlist
core.rs:126-183`) behind a weekly-only flag; two-tier windows (1-day hourly run,
7-day nightly repair run — the same binaries, env-driven) so BigQuery
corrections to older hours still propagate; switch `parse_dex` to the existing
Rust twin; make `wallet_metadata` and `load_latest_prices` incremental; port the
two remaining Spark price jobs (123 and 294 lines) to Rust so Spark leaves the
hourly path.

**Storage is the second question.** After the code fix, a NAS-resident lake is
estimated at 35–60 min per cycle (a hypothesis: it closes only if the hot set
stays in ~40–60 GB of page cache and nothing evicts it); a local-NVMe hot tier at
20–35 min, independent of the wire and of cache eviction. RAM makes repeat reads
free and turns application writes into pipelined NFS writes; it does nothing for
cold reads, first-touch reads or per-commit latency.

**Best sequencing (critic):** do the code project and the hourly schedule on
olympus first — the lake is already on NVMe there and Dagster run history is
there — then move storage. Coupling the two hides which one is slow.

### 7.2 Stages (each a strict superset of the previous)

| stage | what | cost | hourly cycle (hypothesis until Dagster history exists) | bottleneck left |
|---|---|---|---|---|
| **0 — this week, no purchase** | diagnostics (§7.4); reclaim the root NVMe to ≥ 135 GB free (≥ 172 with Spark local dirs: PostgreSQL 20–30, SeaweedFS meta 2, Docker 15, `HL_OUT_DIR` 20, Spark 40–60, logs 5, floor 30–40; zram instead of any swapfile); SeaweedFS as in §6.4 with the §7.3 corrections and the single `-dir` tagged `-volume.disk=hdd` now (makes Stage 1 a flag change); Synology: NFSv4.1 max, one NFS rule for the host IP with squash matching the service uid, data checksum off, no snapshot schedule on the SeaweedFS folder; Dagster: `QueuedRunCoordinator` (there is none today) and a decision on the eager auto-materialize sensor **before** any cron change, BQ ingest hourly but reconciling only buckets older than ~90 min (today it re-deletes the still-filling hour), per-chain hourly jobs serialized until the 409 fix, a 00:15 daily slot (FIFO, wallet_metadata, sharpe, wallet_analytics, insider, Moralis, compaction, expire) and a Sunday window that pauses the hourly schedules; the code project above; migration = bulk `mc mirror` while olympus is live, freeze the Dagster schedules (there are no "Rust timers"; Rust runs via Dagster → Python API) and the HL timer, delta mirror, `pg_dump`/`pg_restore` of Nessie (keep `nessie.version.store.type=JDBC`), fsync rule applied **after** the copy (before it adds ~4 h), 24–48 h freeze is acceptable because the website reads CockroachDB; run the pending 18 B-row L1 re-parse on olympus before the move | $0; 2–3 h diagnostics, 1–2 days build, days–2 weeks code | code as-is: hours (NAS) / 60–120 min (NVMe) — not hourly. After the fixes: ~45 min (35–60) serialized chains; 25–35 min warm with parallel chains and Rust prices | the 1 GbE wire on first-touch and evicted reads (9.1 s per GB); the whole-table `control.table_watermarks` overwrite until the 409 retry lands |
| **1 — cheapest big gain** | one **≥ 4 TB** TLC NVMe with DRAM in the host (2 TB fails the headroom arithmetic once shuffle and the re-parse transient are counted) — or olympus `nvme1n1` (4 TB nominal) if it is an M.2 drive holding nothing needed (infra.md calls it "Data disk", not idle); SeaweedFS `-dir=/nvme/...,/mnt/blueblocx/... -volume.disk=ssd,hdd` + `fs.configure … -disk=ssd -fsync`; hourly `volume.tier.move` ssd→hdd as capacity insurance; Spark local dirs, PostgreSQL, Docker, `HL_OUT_DIR`, optional Trino `fs.cache` on the NVMe; hourly `pg_dump` + `weed backup` to the NAS; UPS for host and NAS | $0 (transplant) or roughly $250–400 for 4 TB TLC (knowledge-based; check a Thai retailer), UPS $120–250 | 20–35 min with 25–40 min of slack, independent of the wire; the root-disk reclaim becomes optional | Rust CPU time, ~20 process launches per cycle, Dagster overhead (3–7 min), BigQuery lag (or validate the QuickNode minute writer); a single NVMe copy between hourly backups |
| **2 — network / NAS** (only if diagnostics say the link, not the array, is the ceiling) | 10 GbE direct host↔NAS link (Synology E10G22-T1-Mini on DS723+/DS923+/DS1522+/RS422+, PCIe cards on DS1621+/DS1821+/xs models; 2.5 GbE only on 25-series models; no slot on DS9x0+/DS7x0+/DS4x0+/DS2x4+); Synology RW M.2 cache (only lever for the ~49 ms commit; disable "skip sequential I/O"); `async` export only with UPS + Nessie rollback runbook; SeaweedFS volume server on the NAS only together with 10 GbE or Stage 1 (it removes the host page cache from the read path) | $250–450 (10 GbE pair), $130–300 (cache SSDs) | unchanged 20–30 min once Stage 1 exists; pays in nightly compaction (15–30 → 3–6 min), weekly ledger-wide passes (2+ h → ~30 min), the 1.3 TB copy (3.4 h → < 1 h) | nothing storage-bound in the hourly path |

### 7.3 Corrections to §6.4 found by verification

- **fstab `vers=4.1`**, not 4.2 — DSM offers NFSv3/4/4.1 only, and only on some
  CPU platforms. `rsize/wsize` negotiate down to 512 KiB on a 2 GB Synology
  (knfsd scales its maximum with RAM; DSM's "packet size" setting affects UDP
  only). Use "as high as the model allows".
- **The DSM "Enable asynchronous" checkbox is exports(5) `async`**, and every
  Synology exports line found in the wild has it on. If it is on, knfsd never
  flushes before replying and `COMMIT` is a no-op: the measured 18 MB/s is then
  the array's **sustained write rate** (the gate's STOP case: resync, scrubbing,
  SMR drives, encrypted folder), not a benign commit latency. Discriminator: run
  `sudo nfsiostat 5 /mnt/blueblocx` during `dd … bs=1M oflag=direct`: `COMMIT`
  average RTT ~50 ms = sync export; sub-millisecond `COMMIT` with still ~18 MB/s
  = async export and a slow array. Under async the SeaweedFS fsync rule still
  pushes each chunk to NAS RAM before the S3 200 (closes the host-crash window
  at ~1–2 ms per chunk) but not to disk; a UPS on the NAS is mandatory.
- **SeaweedFS 4.48 vs Nessie 0.107.4.** 4.48 (2026-09-28) contains the Dec-2025
  aws-chunked fixes but not #11583 (merged 2026-10-03): Nessie's AWS SDK 2.42
  sends `metadata.json` as chunked uploads with a trailing CRC32 and can be
  rejected intermittently with `400 InvalidDigest`. One config line fixes it:
  `nessie.catalog.service.s3.default-options.chunked-encoding-enabled=false`, or
  `AWS_REQUEST_CHECKSUM_CALCULATION=WHEN_REQUIRED` on the Nessie JVM, or a weed
  build after 2026-10-03. Smoke test ≥ 40 sequential Trino `CREATE TABLE`s
  before trusting it. PyArrow's S3 client (the HL loader) sends the same trailers
  and is exposed until the fix ships.
- **`.idx` is never fsynced.** After a host crash a stale (not missing) `.idx`
  loads the volume read-only and already-acknowledged chunks are unreadable
  until the volume server is stopped and `weed fix` rebuilds the index. Put it
  in the runbook; Stage 1 does not remove it.
- **postgres2 filer store in the same PostgreSQL database as Nessie** (a separate
  schema is fine): `pg_dump` snapshots are per database, so "one dump = catalog +
  namespace" holds only then.
- **Volume port 8380.** 8080 is the Spark master UI, 8081+ the worker UIs, 8083
  Trino, 8000 Python API, 8001 ingestion, 3000 Dagster, 19120/9002 Nessie, 5432
  PostgreSQL, 9000 S3, 9333 weed master, 8888 weed filer, 9327 metrics.
- **Nessie sets `gc.enabled=false`**, so the planned `expire_snapshots` /
  `remove_orphan_files` silently do nothing unless each table gets
  `gc.enabled=true` — and once they run they delete the files a "roll Nessie
  back one commit" recovery would need. Choose one cleanup tool (Nessie GC,
  Iceberg expire + orphan, or the dashboard endpoint) and a rollback window
  shorter than its cutoff; run maintenance only with the hourly schedules paused.
- **The eager auto-materialize sensor is probably live** (every L1–L4 asset is
  `eager()`), so the chain likely already re-runs after each 6-hourly BQ ingest;
  Dagster has no run coordinator and the Python API has no lock — changing the
  BQ cron to hourly without `QueuedRunCoordinator` produces 24 uncoordinated
  chain runs a day.
- **"warehouse ≤ 1.3 TB" is stale**: that was the size of the `/data` LV on
  2026-04-28, shared with spark-tmp and backfill; growth since is ~1.5–3 GB/day
  (low confidence) and the bound is void if MinIO was moved to `/data2`. Measure
  with `mc du`. "~5 M objects" was an assumption (the minute writer is STOPPED;
  likely < 1 M).
- Live partitioning differs from `ICEBERG_SCHEMA.sql`: ethereum
  `l1_wallet_ledger` is partitioned by `day(block_timestamp)` only
  (`TRINO_SCHEMA.sql:186`); any "max partition value" fix must read that field.
- BigQuery ingest is **unfiltered** (no topic whitelist on the BQ path) — raw
  reads are 2–3× larger than the pipeline needs; applying the QuickNode whitelist
  on the hourly reload shrinks ingest, delete churn and the L1 read set.
- iceberg-rust 0.9 does apply positional and equality delete files
  (`delete_filter.rs`), so Trino `DELETE`s on `raw_logs` are honoured by the Rust
  readers — previously assumed, now verified.

### 7.4 First measurements (read-only, ~2–3 h; they decide the stage)

| where | command | decides |
|---|---|---|
| olympus | per table in both chains: `SELECT count(*) files, round(sum(file_size_in_bytes)/1e9,1) gb FROM blueblocx.ethereum."<t>$files"` for `l1_wallet_ledger`, `raw_logs`, `l3_wallet_statement_watchlist`, `l3_wallet_statement_usd_watchlist_1m`, `l3_token_price_usd_1m`, `l4_wallet_balance_1d_dense`, `l4_wallet_networth_1d(_dense)`, `l4_wallet_balance_state`, `l4_wallet_pnl_realized_per_sell_tx` | order of code fixes: ledger files > 50k → window fix first; any L4 table > 20 GB → range-replace first; avg file < 50 MB → compaction first |
| olympus | Dagster UI → Sensors (is `default_automation_condition_sensor` RUNNING?) and Schedules; run durations of the last 14 days (`runs` table in the Dagster PostgreSQL); `SELECT date(committed_at), count(*) FROM blueblocx.ethereum."l1_wallet_ledger$snapshots" GROUP BY 1 ORDER BY 1 DESC LIMIT 7` | today's real cadence and per-chain duration; > 50 min per chain on NVMe = hourly impossible without the code project anywhere |
| olympus | `SELECT key, value FROM blueblocx.ethereum."l1_wallet_ledger$properties" WHERE key IN ('gc.enabled','write.target-file-size-bytes')`; `psql -d nessie -c '\dt'` | whether expire will no-op; JDBC (`refs/objs`) vs JDBC2 |
| olympus | `mc du local/warehouse; mc ls --recursive local/warehouse \| wc -l; df -h /data /data2; sudo du -xsh /data2/*; lsblk -o NAME,SIZE,MODEL,TRAN /dev/nvme1n1` | copy time; 2 vs 4 TB; transplant candidate |
| BigQuery | `SELECT TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(block_timestamp), MINUTE) FROM bigquery-public-data.goog_blockchain_ethereum_mainnet_us.logs WHERE block_timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)` (3–4 times a day; same for arbitrum) | ≤ 15 min → hourly BQ is fine; > 60 min → QuickNode must be the hourly source |
| host | `sudo dmidecode -t baseboard`; `sudo dmidecode -t slot`; `ls /sys/class/nvme`; `lspci -tv \| grep -iE 'nvme\|root port'`; `sudo blkid /dev/nvme0n1p*` | free M.2/PCIe slot (dmidecode often omits M.2 — read the board manual); NTFS on p2–p4 = a Windows dual boot, i.e. a shared, rebootable host |
| host | `sudo du -xh --max-depth=2 / \| sort -rh \| head -50; docker system df -v; journalctl --disk-usage; swapon --show; du -xsh ~/.cache /home/* /var/lib/docker` | what fills 371 GB; reclaim target ≥ 135 GB (≥ 15 GB if Stage 1) |
| host | `ip -br link; grep blueblocx /proc/mounts; nfsstat -m; ethtool <if>; iperf3 -c 192.168.1.121 -t 20` (and `-R`, `-P 4`; `iperf3 -s` on the NAS via Container Manager) | one NIC or two; vers/wsize/`sync`; 1 GbE or the array as the ceiling |
| host | `sudo nfsiostat 5 /mnt/blueblocx` **during** `dd if=/dev/zero of=/mnt/blueblocx/d1.bin bs=1M count=1024 oflag=direct`; then `fio --name=bufw --directory=/mnt/blueblocx --size=20G --rw=write --bs=1M --ioengine=psync --direct=0 --end_fsync=1 --buffer_pattern=random --refill_buffers --group_reporting`; `dd … bs=16M count=256 oflag=direct`; 8 parallel `dd … bs=1M count=512 oflag=direct` | sync vs async export; buffered ≥ 90 MB/s go, ≤ 30–40 stop; latency- vs array-bound |
| host | `sudo -u <weed-user> sh -c 'mkdir -p /mnt/blueblocx/seaweedfs && touch /mnt/blueblocx/seaweedfs/.w && ls -ln /mnt/blueblocx/seaweedfs'` | the uid trap (never `chmod -R 777`) |
| NAS (SSH, `sudo -i`) | `cat /proc/sys/kernel/syno_hw_version; cat /etc/VERSION; free -m; cat /proc/mdstat; mount \| grep /volume1; cat /etc/exports; cat /proc/fs/nfsd/versions; for d in /dev/sd?; do smartctl -i $d \| grep -iE 'Model\|Rotation'; done; which docker iscsiadm; synopkg list \| grep -iE 'container\|san'` | model → 2.5/10 GbE, M.2, Container Manager; `async`; CMR vs SMR; RAID health; NAS RAM |
| new host | with Nessie up and one checksum mitigation applied: `for i in $(seq 40); do trino --execute "CREATE TABLE blueblocx.scratch.t$i AS SELECT $i x" \|\| echo FAIL $i; done`; `cargo run -p control-jobs` against scratch; one pyiceberg append; `python -m blueblocx_hyperliquid.iceberg_load --from <staging> --tables l1_token_metadata` into scratch; fsync rule check with `strace -f -e trace=fsync -p $(pidof weed)` + `nfsiostat` during `mc cp` | Nessie↔SeaweedFS interop; iceberg-rust list semantics; the fsync chain |

### 7.5 Do not do

Schedule hourly with the code as-is · build a `file://` warehouse · run MinIO
on the NAS · `vers=4.2`, `soft`, `sync` or `noac` on the mount · leave "Enable
asynchronous" on without a UPS and a rollback runbook · put shuffle, spill,
PostgreSQL, SQLite, Docker, SeaweedFS metadata or `HL_OUT_DIR` on the NAS · any
hourly cadence without scheduled compaction + expire (and `gc.enabled=true`) ·
parallel eth/arb Rust chains before the 409 retry · let the Sunday weekly jobs
fire while hourly schedules are active (the next `wallet_balance` run flips to a
6–10 h full recompute) · run the 18 B-row re-parse or a full-dataset Spark pass
at NAS speed · apply the fsync rule before the bulk copy · install Nessie with
JDBC2 · cachefilesd for NFS (caches read-only opens only) · the SeaweedFS volume
server on the NAS at 1 GbE · stream Rust stdout into the Dagster event log at
hourly cadence without retention · download backfills to `/mnt/blueblocx/backfill`
during pipeline hours · iSCSI with multiple sessions or a thin LUN.

### 7.6 Questions the owner must answer

1. Why is the stack leaving olympus, what happens to olympus, and may the code
   project + hourly schedule run there first?
2. Is "the NAS is the only bulk storage" physical or policy: a free M.2/PCIe slot
   on the 4090 board? May `nvme1n1` move (what is on it)? Budget for a 4 TB NVMe
   + UPS, and for Stage 2?
3. Is the host dedicated: what fills the 371 GB, who owns it, is there a Windows
   dual boot, who reboots it, is it (and the NAS) on a UPS?
4. Product definition of "hourly": balances, prices, statements, net worth hourly
   with realized FIFO PnL / analytics / insider / sharpe at T+1 (FIFO is
   day-granular by construction, `fifo.rs:471-479`), or realized PnL too? Does it
   include the website, i.e. where does the Trino → CockroachDB sync run today?
5. Freshness source: BigQuery (observed lag?) or the QuickNode minute writer (has
   the parity verifier ever passed for a week; cost at ~7,100 eth + ~345k arb
   blocks/day)?
6. Which Dagster schedules and sensors are RUNNING on olympus right now, and the
   last 14 days of run durations?
7. NAS facts (§7.4) and the durability/cleanup stance: async + UPS + rollback
   runbook vs sync; which one cleanup tool; SeaweedFS (no official NFS support,
   data unreadable without the filer store) vs versitygw plain files at ~2× write
   cost.
8. Who does the Rust/Python/Dagster work, and is an interim cadence (every 2–6 h)
   acceptable until it lands?
9. Which secrets move, and does the new host reach BigQuery, CockroachDB,
   QuickNode and api.hyperliquid.xyz? One NIC or two?
10. Do the pending 18 B-row re-parse and the Balance/FIFO rebuild run on olympus
    before the move?

## 8. Scope correction: only the Hyperliquid pipeline moves to server4090-2 (2026-10-12)

The owner clarified that `server4090-2` hosts **this repository's pipeline**, not
the atlas ethereum/arbitrum lake (which stays on olympus unless stated
otherwise). §6–§7 analysed the wrong scope; they remain valid for the day the
atlas lake moves, and §6.1–§6.3 are verified facts either way. For the
Hyperliquid pipeline alone the picture is simpler, and the owner's original
instinct — no MinIO, no SeaweedFS, read and write plain files — is **right**:

- The only lake writer on the host is pyiceberg, which supports `file://`
  natively. None of the constraints that forced an S3 gateway in the atlas lake
  (Nessie's ObjectIO, Trino 465, iceberg-rust with S3 hard-coded) exists here.
- Data volume is a few MB per hourly lift (a market-maker wallet ≈ 30 MB/day);
  NFS throughput is irrelevant at that scale. A lift is ~14 commits and ~5–10 s
  of NFS time on a synchronous export.
- Design (implemented in commits `04be05a` and the follow-up): pyiceberg
  `SqlCatalog` on PostgreSQL (local disk; SQLite is acceptable when nothing
  cross-host reads the lake) + `WAREHOUSE=file:///mnt/blueblocx/hypercore/warehouse`,
  `HL_WAREHOUSE_MOUNT=/mnt/blueblocx` guard, `HL_OUT_DIR` on the local NVMe,
  hourly systemd timer, nightly snapshot expiry. Details and the verified
  behaviour (commit atomicity via the PostgreSQL pointer swap, no dependence on
  atomic rename, NFS close-to-open sufficiency, durability caveat under an async
  export) are in `docs/DEPLOY.md`, "Standalone host" and "Hourly cadence".
- The 29 GB free on the root NVMe is enough for this pipeline's state
  (`HL_OUT_DIR` ≈ 2 MB per wallet-day of staging at 45-day retention, a few GB
  for a market-maker wallet) but leaves no room to grow; reclaim or cut
  `HL_RETENTION_DAYS` to 31 beyond ~150–200 light wallets.
- Hourly capacity on one egress IP: ~270 light wallets per 50-minute run at
  `HL_WEIGHT_BUDGET=900`, ~360 at 1200 (the TS platform-backend is on Cloudflare
  Workers, not this IP); ~40–55 market-maker-class wallets. Beyond that: a second
  IP or daily-only fetching of the slow-changing pulls (+60 %).
- Open decisions for the owner: who consumes the hypercore numbers (the fastest
  user-visible path is writing the per-wallet analytics the pipeline already
  computes straight into the serving PostgreSQL; the lake stays as history), how
  many wallets and of which class, whether atlas on olympus must read these
  tables (then olympus mounts the same export at the identical path and uses the
  same PostgreSQL catalog), the Synology export mode and UPS, and who rotates
  the exposed AWS key (only needed if S3 backfill stays in use).
