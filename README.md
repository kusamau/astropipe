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
detected instead of waiting for the producer to finish: available bytes are
written to `<destination>.tmp` on the archive filesystem, and each time the
source grows the new bytes are appended. A slow producer therefore overlaps its
writing with Astropipe's copying rather than being copied only after it stops.

**Staging is valid only for the run that created it.** A restart discards
whatever a previous attempt left behind and copies again from the beginning.
That is the deliberate trade behind this design: it removes the persisted
checkpoint, the byte-for-byte resume-boundary validation and the two-phase copy
state, at the cost of re-copying a large file that was interrupted mid-transfer.
The source is the only authority, and it is never removed until its image has
been published.

A source that loses bytes it has already handed over violates the append-only
contract; the row becomes `FAILED` and both artifacts are preserved.

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
a name that is already `ARCHIVED` or `FAILED` is never processed again. A row is
`IN_PROGRESS` from the moment the file is claimed until publication sets
`ARCHIVED` along with the saved time and saved folder, or a contract violation or
unrecoverable I/O error sets `FAILED`. Copy progress is deliberately not
recorded, because no staging file outlives the process that wrote it.

On restart, `ArchiveReconciler` compares every in-progress row against the
filesystem. A surviving source is the authority: any staging file is deleted, and
a published destination shorter than its source is deleted too rather than leave
a truncated image under a complete name. The row stays in progress and the image
is copied again. Only a fully published destination, a source that shrank, or the
loss of both copies is terminal.

Schema version 3 is required. A database created by an earlier state model is
rejected with an explicit request to create a fresh database.

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
- Resuming an interrupted copy across a restart: recopying from the source is simpler and the source is still there.
- Full-file checksums: the append-only contract plus an atomic publish is the integrity story.
- Automatic retries: failed rows stay explicit rather than hiding policy in a backoff loop.
- A metrics endpoint: logs and `VALIDATION.md` are sufficient here.
- A framework: the application is plain Java over `java.nio`, JDBC and Jackson.
- NFS/CIFS specialization: periodic reconciliation limits missed-event risk, but remote storage needs separate operational testing.
