# Astropipe

Astropipe watches a local source directory for satellite images named
`SAT_IMG_<yyyyMMddHHmmssSSS>.raw`, moves complete files into a year/month/day
archive, and records each operation in SQLite.

## Build and run

Requirements: Linux, Java 21 or newer, and Maven 3.8 or newer.

```bash
mvn clean package
mkdir -p data/source data/archive
java -jar target/astropipe.jar
```

Configuration comes from `src/main/resources/application.yml`. Astropipe settings
are bound into the `AppConfig` record, while the database uses Spring Boot's
standard `spring.datasource` settings. External configuration can override them,
for example with `--spring.config.additional-location=file:./application.yml` or
environment variables such as `ASTROPIPE_SOURCE_DIR` and `SPRING_DATASOURCE_URL`.
Source and archive directories must already exist and be readable and writable;
invalid configuration fails at startup with a specific error.

Test data is produced by the independent `AstropipeGenerator` application. From
a sibling checkout, build it and point its required `--out` option at this
application's source directory:

```bash
cd ../AstropipeGenerator
mvn clean package

# Five 100 MB images, safely published through staging and atomic rename
java -jar target/astropipe-generator.jar \
  --out ../Astropipe/data/source --count 5 --size 100M --rate 2

# Write slowly in place to exercise stability detection
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

### Moving is normally independent of file size

The brief's suggested bottleneck only exists when source and archive are on
different filesystems. On one filesystem, `Files.move(..., ATOMIC_MOVE)` maps
to a metadata rename, so moving 1 GB costs roughly the same as moving 1 MB.
Astropipe compares both `FileStore` values at startup and logs either
`SAME_FILESTORE (rename fast path)` or `CROSS_FILESTORE (copy fallback)`.

Measurements on the development machine's ext4 filesystem confirm this. Each
file contained real bytes; timings are the duration reported by `FileMover`.

| File size | Atomic rename |
|---:|---:|
| 1 MB | 2 ms |
| 100 MB | 0 ms |
| 1 GB | 0 ms |

A second writable filestore was unavailable, so the cross-filesystem behavior
was measured with the hidden validation setting `astropipe.force-copy-path: true`. This
uses the exact fallback implementation without claiming to measure a different
physical device.

| File size | Forced copy fallback |
|---:|---:|
| 1 MB | 18 ms |
| 100 MB | 135 ms |
| 1 GB | 1,325 ms |

The fallback copies to `<destination>.tmp` with `FileChannel.transferTo()` in a
loop, forces the destination channel, atomically renames the temporary file in
its destination directory, and deletes the source last. Startup recursively
removes only Astropipe-shaped `SAT_IMG_*.raw.tmp` files.

### Detection and complete files

`WatchService` supplies low-latency `ENTRY_CREATE` and `ENTRY_MODIFY` events.
It cannot recover events missed while the process was stopped and may overflow,
so Astropipe also performs a startup listing and a periodic reconciliation
listing. An explicit `OVERFLOW` schedules an immediate listing.

Creation does not mean a producer has closed the file. With stability detection
enabled, a candidate must have the same size and last-modified time for the
configured number of samples. Sampling runs on the scheduled thread and never
blocks the watcher thread. The preferred producer contract is still staging
plus atomic rename: it is unambiguous and avoids the detection delay. Stability
detection can be disabled when every producer follows that contract.

### Correctness under failure

The SQLite primary key on `file_name` is the authoritative deduplication guard;
the in-memory set only avoids unnecessary database work. Processing follows
this order:

1. Insert `PENDING`.
2. Move or safely copy the file.
3. Update the row to `ARCHIVED` with its directory and save time.
4. If movement fails, mark it `FAILED` and preserve the source.

On restart, a `PENDING` row with a destination becomes `ARCHIVED`; a row whose
source still exists is deleted so the file can be submitted again; and a row
with neither file becomes `FAILED`. If a crash interrupts a fallback copy, the
original remains in source and startup removes the partial `.tmp` before the
row is reset. If movement completes before the database update, the destination
causes the row to be finalized as `ARCHIVED`.

SQLite runs in WAL mode with `synchronous=NORMAL`. Spring Boot manages a
two-connection Hikari pool for the two default workers, avoiding a connection
open per file while keeping connection lifecycle out of the repository.

### Concurrency and backpressure

One thread blocks in the watch loop and one scheduled thread handles stability
sampling and reconciliation. Archive work uses a fixed pool, default size two,
with a bounded `ArrayBlockingQueue` and `CallerRunsPolicy`. When the queue fills,
the submitting thread performs work and naturally slows intake instead of
growing the heap without limit.

Two workers are deliberate. Extra threads only contend on directory and SQLite
metadata for the rename path. Copy-path parallelism depends on the storage
device, so both pool size and queue capacity remain configurable.

## What I deliberately did not build

- Distributed coordination: the exercise targets one local process and disk.
- Checksums: they would add a full read and change the performance question.
- Automatic retries: failed rows remain explicit rather than hiding policy in a backoff loop.
- A metrics endpoint: logs and `VALIDATION.md` are sufficient for this exercise.
- NFS/CIFS specialization: periodic reconciliation limits missed-event risk, but remote storage needs separate operational testing.
- Docker, brokers, and plugin abstractions: none help the stated local-file workflow.

See [VALIDATION.md](VALIDATION.md) for test cases and observed results.
