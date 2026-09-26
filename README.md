# Astropipe

Astropipe watches a local source directory for satellite images named
`SAT_IMG_<yyyyMMddHHmmssSSS>.raw`, moves each complete file into a
`Archive/<Year>/<Month>/<Day>` hierarchy, and records every operation in SQLite
and in a log file.

## Build and run

Requirements: Linux, Java 21 or newer, Maven 3.8 or newer.

`sqlite-jdbc` carries a native library for each of 24 platforms, which is most of
the packaged jar. The build keeps only 64-bit Linux and macOS — every machine this
is built, reviewed or deployed on — which takes the jar from 17 MB to under 6 MB.
Running anywhere else means widening the `org.xerial:sqlite-jdbc` filter in
`pom.xml`.

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

An entry the pipeline refuses — a name that is not `SAT_IMG_<timestamp>.raw`, or
one whose 17 digits are not a real instant — is left where it is and never
claimed, so every later listing finds it again. It is reported at `WARN` when it
could be a first sighting, meaning a watch event or the startup listing, and at
`DEBUG` on the periodic re-scans. An operator therefore hears about a stray file
once per run rather than once per sweep for as long as the process lives.

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
mvn test     # 36 functional tests, no database connection
mvn verify   # adds 16 SQLite-backed integration tests under src/it/java
```

See [VALIDATION.md](VALIDATION.md) for the test cases and observed results.

## The code

Eleven classes in one package. They are kept package-private wherever possible —
only `Main` and `Config` are public — so the compiler enforces that nothing here
is reachable from outside. That is worth more at this size than a directory per
layer would be, but it does mean an alphabetical file listing scatters related
things, so here is the map:

| Role | Class |
| --- | --- |
| Entry point and wiring | `Main` |
| Configuration | `Config` |
| Noticing files | `SourceDetector` |
| Deciding what happens to one | `SourceWatcher` |
| Startup recovery | `ArchiveReconciler` |
| Naming and placement | `SatelliteImageName` (a parsed name), `ArchivePathResolver` |
| Knowing a file is finished | `StabilityDetector` |
| Moving bytes | `FileMover` |
| The archive ledger | `ArchiveRepository` |
| JDBC, with no domain knowledge | `Database` |

An image travels through them in that order: `SourceDetector` reports every
regular file it sees, `SourceWatcher` decides whether it is an image worth
claiming and drives it from staging to publication, and `ArchiveReconciler`
picks up whatever a previous run left behind.

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

The only index is partial and covering: it holds in-progress rows alone, which are
transient, so the startup lookup stays instant however large the archive grows.

`Database` owns the single SQLite connection and the JDBC ceremony; it knows
nothing about archiving. It opens in WAL mode with `synchronous=NORMAL` and
serialises the handful of tiny statements the pipeline issues, which is all
SQLite would allow concurrently anyway. `ArchiveRepository` holds only the ledger
itself, and each state change is one statement whose `WHERE` clause carries the
rule it enforces, so the guard is applied by the database rather than by a read
followed by a write.

### Concurrency and backpressure

One thread blocks in the watch loop and one scheduled thread owns the pipeline.
Copy and publication work uses a fixed pool with a bounded `ArrayBlockingQueue`
and `CallerRunsPolicy`. When the queue fills, the submitting thread performs the
work and naturally slows intake instead of growing the heap without limit.

Two workers are the default because copy parallelism depends on the source and
archive devices; both pool size and queue capacity are configurable.

**All per-file state is confined to the scheduler thread.** It alone decides what
happens to a tracked image and mutates its record; workers run blocking I/O,
return a value, and touch nothing shared. Handing work to an executor and posting
the result back supplies the happens-before edges in both directions, so
`SourceWatcher` needs no locks, no atomics and no `volatile` — a plain `HashMap`
and plain fields. The invariant that makes it safe is that a tracked file has at
most one outstanding action, which holds structurally because only one method
chooses the next one.

One consequence worth knowing: because the scheduler submits the work, a full
worker queue makes `CallerRunsPolicy` run a copy on the scheduler thread, pausing
quiet-period polling until it finishes. Publication is delayed, never incorrect —
the quiet period is a floor, not a ceiling — and the default queue holds 10,000
entries.

## What I deliberately did not build

- Distributed coordination: the exercise targets one local process and disk.
- Resuming an interrupted copy across a restart: recopying from the source is simpler and the source is still there.
- Full-file checksums: the append-only contract plus an atomic publish is the integrity story.
- Automatic retries: failed rows stay explicit rather than hiding policy in a backoff loop.
- A metrics endpoint: logs and `VALIDATION.md` are sufficient here.
- A framework: the application is plain Java over `java.nio`, JDBC and Jackson.
- NFS/CIFS specialization: periodic reconciliation limits missed-event risk, but remote storage needs separate operational testing.
