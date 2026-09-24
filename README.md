# Astropipe

Astropipe watches a local source directory for satellite images named
`SAT_IMG_<yyyyMMddHHmmssSSS>.raw`, moves complete files into a year/month/day
archive, and records each operation in SQLite.

## Build and run

Requirements: Linux, Java 21 or newer, and Maven 3.8 or newer.

```bash
mvn clean package
mkdir -p data/source
java -jar target/astropipe.jar
```

Configuration comes from `src/main/resources/application.yml`. Astropipe settings
are bound into the `AppConfig` record, while the database uses Spring Boot's
standard `spring.datasource` settings. External configuration can override them,
for example with `--spring.config.additional-location=file:./application.yml` or
environment variables such as `ASTROPIPE_SOURCE_DIR` and `SPRING_DATASOURCE_URL`.
The source directory must already exist and be readable and writable. By default,
Astropipe creates the archive directory and any missing parents. Set
`astropipe.create-archive-dir` to `false` to require it to exist instead. Invalid
or inaccessible paths fail at startup with a specific error.

Test data is produced by the independent `AstropipeGenerator` application. From
a sibling checkout, build it and point its required `--out` option at this
application's source directory:

```bash
cd ../AstropipeGenerator
mvn clean package

# Five 100 MB images, safely published through staging and atomic rename
java -jar target/astropipe-generator.jar \
  --out ../Astropipe/data/source --count 5 --size 100M --rate 2

# Write slowly in place to exercise incremental staging
java -jar target/astropipe-generator.jar \
  --out ../Astropipe/data/source --count 1 --size 10M --unsafe-write
```

The generator has its own build and release lifecycle; Astropipe neither
packages nor invokes it. Its default producer contract writes into
`<out>/.staging` and atomically renames the finished file into the source
directory.

Run the fast functional suite with:

```bash
mvn test
```

Functional tests live under `src/test/java` and exercise application logic
without opening a database connection. Run the complete suite, including the
SQLite integration tests under `src/it/java`, with:

```bash
mvn verify
```

## Design decisions

### Incremental archive-side staging

Astropipe is optimized for very large append-only files whose source and archive
normally live on different filesystems. Copying begins as soon as a file is
detected instead of waiting for the producer to finish. Available bytes are
written to `<destination>.tmp` on the archive filesystem and the durable byte
count is checkpointed in SQLite after `FileChannel.force(true)`.

When the source grows, Astropipe validates the last configured overlap (64 KiB
by default) byte-for-byte against staging before appending new bytes. This
detects a broken resume boundary without rereading the entire large file. A
different overlap, source shrink, or replacement violates the producer contract;
the row becomes `FAILED` and both artifacts are preserved.

The relevant settings are `astropipe.stability.poll-interval-millis`,
`quiet-period-millis`, and `resume-overlap-bytes`. Incremental copying is always
used, including when both paths happen to share a filesystem.

### Detection and complete files

`WatchService` supplies low-latency `ENTRY_CREATE` and `ENTRY_MODIFY` events.
It cannot recover events missed while the process was stopped and may overflow,
so Astropipe also performs a startup listing and a periodic reconciliation
listing. An explicit `OVERFLOW` schedules an immediate listing.

Creation does not mean a producer has closed the file. Source size and
last-modified time are sampled on the scheduled thread. Growth submits another
overlap-validated append pass; an unchanged source starts the quiet period.
Only after the complete quiet period does Astropipe atomically rename staging
to the final destination and remove the source.

The producer contract is append-only: existing byte ranges must never change.
The quiet period is the completion signal, so it must be longer than any normal
pause between producer writes.

### Correctness under failure

The SQLite primary key on `file_name` is the authoritative deduplication guard.
Processing cycles between `COPYING` and `STAGING`: `COPYING` means overlap
validation or transfer is active, while `STAGING` means the durable copy has
caught up and is waiting for quiet. Publication changes the row to `ARCHIVED`;
contract violations and unrecoverable I/O errors change it to `FAILED`.

`copied_bytes` advances only after staging is forced. On restart, bytes beyond
that checkpoint are truncated and transfer resumes after validating the
overlap. A published destination is finalized or returned to staging according
to its size relative to the append-only source. Temporary files are durable
recovery state and are never removed by a blanket startup cleanup.

Schema version 2 is required. A database created by the previous `PENDING`
state model is rejected with an explicit request to create a fresh database.

SQLite runs in WAL mode with `synchronous=NORMAL`. Spring Boot manages a
two-connection Hikari pool for the two default workers, avoiding a connection
open per file while keeping connection lifecycle out of the repository.

### Concurrency and backpressure

One thread blocks in the watch loop and one scheduled thread handles quiet-period
sampling and reconciliation. Copy and publication work use a fixed pool, default size two,
with a bounded `ArrayBlockingQueue` and `CallerRunsPolicy`. When the queue fills,
the submitting thread performs work and naturally slows intake instead of
growing the heap without limit.

Two workers are deliberate because copy parallelism depends on the source and
archive devices. Both pool size and queue capacity remain configurable.

## What I deliberately did not build

- Distributed coordination: the exercise targets one local process and disk.
- Full-file checksums: resume validation intentionally reads only the configured overlap.
- Automatic retries: failed rows remain explicit rather than hiding policy in a backoff loop.
- A metrics endpoint: logs and `VALIDATION.md` are sufficient for this exercise.
- NFS/CIFS specialization: periodic reconciliation limits missed-event risk, but remote storage needs separate operational testing.
- Docker, brokers, and plugin abstractions: none help the stated local-file workflow.

See [VALIDATION.md](VALIDATION.md) for test cases and observed results.
