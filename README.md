# Astropipe

Astropipe watches a local source directory for satellite images named
`SAT_IMG_<yyyyMMddHHmmssSSS>.raw`, moves each complete file into a
`Archive/<Year>/<Month>/<Day>` hierarchy, and records every operation in SQLite
and in a log file.

## Build and run

Requirements: Linux, Java 21 or newer, Maven 3.8 or newer.

```bash
mvn clean package
./scripts/run.sh
```

`scripts/run.sh` builds the jar if needed, creates `data/source`, `data/archive`
and `logs`, and starts the application. To run it by hand:

```bash
java -jar target/astropipe.jar [path/to/config.json]
```

The configuration file defaults to `./config.json`.

## Configuration

All settings live in one JSON file. Only `sourceDir` and `archiveDir` are
required; anything else falls back to the default shown here.

```json
{
  "sourceDir": "./data/source",
  "archiveDir": "./data/archive",
  "databaseUrl": "jdbc:sqlite:./data/archive.db",

  "pollIntervalMillis": 250,
  "quietPeriodMillis": 250,
  "resumeOverlapBytes": 65536,
  "reconciliationIntervalSeconds": 30,

  "workerPoolSize": 2,
  "workerQueueCapacity": 10000
}
```

| Setting | Meaning |
| --- | --- |
| `sourceDir` | Watched directory. Must already exist and be readable and writable. |
| `archiveDir` | Archive root. Created with its parents if missing. |
| `databaseUrl` | JDBC URL of the SQLite ledger. |
| `pollIntervalMillis` | How often a tracked file is re-sampled for changes. |
| `quietPeriodMillis` | How long size and modification time must hold still before publication. Must exceed any normal pause between producer writes. |
| `resumeOverlapBytes` | Bytes re-validated against staging before appending more. |
| `reconciliationIntervalSeconds` | Interval of the directory listing that backs up `WatchService`. |
| `workerPoolSize` / `workerQueueCapacity` | Copy and publication thread pool. |

Startup fails with a specific message on a blank required directory, a source
and archive that resolve to the same path, a negative tuning value, an unknown
setting, or an inaccessible directory.

Logs go to the console and to `logs/astropipe.log`, rotated daily and kept for
14 days.

## Input data

Test images come from a separate generator application, which has its own build
and release lifecycle; Astropipe neither packages nor invokes it. Its producer
contract is append-only: it writes into `<out>/.staging` and atomically renames
the finished file into the source directory, and existing byte ranges are never
rewritten.

Anything that honours that contract works. The simplest manual equivalent is:

```bash
mkdir -p data/source/.staging
dd if=/dev/urandom of=data/source/.staging/img bs=1M count=100 status=none
mv data/source/.staging/img data/source/SAT_IMG_20210714102730046.raw
```

## Tests

```bash
mvn test     # 33 functional tests, no database connection
mvn verify   # adds 16 SQLite-backed integration tests under src/it/java
```

See [VALIDATION.md](VALIDATION.md) for the test cases and observed results.

## Design decisions

### Incremental archive-side staging

Astropipe is optimized for very large append-only files whose source and archive
normally live on different filesystems. Copying begins as soon as a file is
detected instead of waiting for the producer to finish. Available bytes are
written to `<destination>.tmp` on the archive filesystem and the durable byte
count is checkpointed in SQLite after `FileChannel.force(true)`.

When the source grows, Astropipe validates the last `resumeOverlapBytes` against
staging byte-for-byte before appending new bytes. This detects a broken resume
boundary without rereading the entire large file. A different overlap, a source
shrink, or a replacement violates the producer contract; the row becomes
`FAILED` and both artifacts are preserved for inspection.

### Detection and complete files

`SourceDetector` owns this and nothing else: it reports every regular file in the
source directory, with how it was found, and leaves interpreting the name to the
pipeline. `WatchService` supplies low-latency `ENTRY_CREATE` and `ENTRY_MODIFY`
events. It cannot recover events missed while the process was stopped and may
overflow, so a startup listing and a periodic reconciliation listing run
alongside it. An explicit `OVERFLOW` schedules an immediate listing. If
`WatchService` is unavailable altogether, detection falls back to listing alone.

Keeping detection separate means it is testable on its own: `SourceDetectorTest`
drives it with a plain callback and never opens a database.

Creation does not mean a producer has closed the file. Source size and
last-modified time are sampled on the scheduled thread. Growth submits another
overlap-validated append pass; an unchanged source starts the quiet period. Only
after the complete quiet period does Astropipe atomically rename staging to the
final destination and remove the source.

### Correctness under failure

The SQLite primary key on `file_name` is the authoritative deduplication guard:
a name that is already `ARCHIVED` or `FAILED` is never processed again.
Processing cycles between `COPYING` and `STAGING` — `COPYING` means overlap
validation or transfer is active, `STAGING` means the durable copy has caught up
and is waiting for quiet. Publication sets `ARCHIVED` along with the saved time
and saved folder; contract violations and unrecoverable I/O errors set `FAILED`.

`copied_bytes` advances only after staging is forced. On restart,
`ArchiveReconciler` compares every in-progress row against the filesystem:
bytes beyond the checkpoint are truncated and transfer resumes after validating
the overlap, and a published destination is finalized or returned to staging
according to its size relative to the append-only source. Temporary files are
durable recovery state and are never removed by a blanket startup cleanup.

Schema version 2 is required. A database created by the earlier `PENDING` state
model is rejected with an explicit request to create a fresh database.

SQLite runs in WAL mode with `synchronous=NORMAL` on a single connection, which
serialises the handful of tiny statements the pipeline issues — all SQLite would
allow concurrently anyway.

### Concurrency and backpressure

One thread blocks in the watch loop and one scheduled thread handles
quiet-period sampling and reconciliation. Copy and publication work uses a fixed
pool with a bounded `ArrayBlockingQueue` and `CallerRunsPolicy`. When the queue
fills, the submitting thread performs the work and naturally slows intake
instead of growing the heap without limit.

Two workers are the default because copy parallelism depends on the source and
archive devices; both pool size and queue capacity are configurable.

## What I deliberately did not build

- Distributed coordination: the exercise targets one local process and disk.
- Full-file checksums: resume validation intentionally reads only the configured overlap.
- Automatic retries: failed rows stay explicit rather than hiding policy in a backoff loop.
- A metrics endpoint: logs and `VALIDATION.md` are sufficient here.
- A framework: the application is plain Java over `java.nio`, JDBC and Jackson.
- NFS/CIFS specialization: periodic reconciliation limits missed-event risk, but remote storage needs separate operational testing.
