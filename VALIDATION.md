# Validation

## Strategy

Three layers, each answering a different question.

| Layer | Question | Where | Command |
| --- | --- | --- | --- |
| Functional tests | Is each unit correct in isolation? | `src/test/java` | `mvn test` |
| Integration tests | Do the ledger, the filesystem and the watcher agree? | `src/it/java` | `mvn verify` |
| Manual end-to-end | Does the shipped jar satisfy the requirements? | below | `./scripts/run.sh` |

Timing is never asserted with a fixed sleep. Integration tests poll a condition
until a deadline, and the pipeline is configured with short poll and quiet
periods so the suite stays fast without becoming timing-dependent.

The suites were also checked against deliberately broken production code, so that
passing means something:
- breaking the deduplication guard was caught by `ArchiveRepositoryIT`;
- keeping a truncated publication instead of discarding it was caught by `ArchiveReconcilerIT`;
- publishing without waiting for the quiet period, and recording a transfer stopped by
  shutdown as `FAILED`, were both caught by `ArchiverIT`.

The same-day directory race in `ArchivePathResolver` also failed its test on every run
before the fix.

Nor may a test depend on how quickly `WatchService` reports a new file. That is a
property of the platform — instant on Linux, polled on macOS, and unreliable over
bind mounts and network filesystems — and the periodic listing exists precisely
because it cannot be relied on. A test that asserts on detection latency is
testing the operating system. `doesNotMoveFileWhileUnsafeGeneratorIsStillWriting`
therefore checks its invariant after every chunk the producer writes, rather than
once at a fixed moment.

## Requirements coverage

| Requirement | Verified by |
| --- | --- |
| Monitor the source folder | `SourceDetectorTest` (5 tests), `EventKindHandlerTest` (3 tests), `ArchiverIT.archivesStartupAndWatchEventsAndIgnoresMalformedFiles`, `ArchiverIT.fallsBackToReconciliationWhenWatchServiceIsUnavailable`, M1 |
| Move images to the archive as fast as possible | `ArchiverIT.doesNotMoveFileWhileUnsafeGeneratorIsStillWriting`, `ArchiverIT.archivesManyImagesOfTheSameDayArrivingTogether`, M2, M9, M11 |
| Log all file operations to a log file | M4 |
| Use a JSON configuration file | `ConfigTest` (7 tests), M5 |
| Source folder configurable | `ConfigTest.loadsTheShippedConfigurationFile`, M5 |
| Archive folder configurable | `ConfigTest.loadsTheShippedConfigurationFile`, M5 |
| Create `<Year>/<Month>/<Day>` if missing, zero-padded | `ArchivePathResolverTest` (3 tests), M2 |
| Record file name, time saved and saved folder | `ArchiveRepositoryIT.recordsNameSavedTimeAndSavedFolderOnArchive`, M3 |
| Never process the same name twice | `ArchiveRepositoryIT.primaryKeyIsAuthoritativeDedupeGuard`, M6 |

## Automated results

`mvn verify` on Linux, Java 25, run 2026-09-27:

```
Tests run: 41, Failures: 0, Errors: 0, Skipped: 0   (surefire)
Tests run: 17, Failures: 0, Errors: 0, Skipped: 0   (failsafe)
BUILD SUCCESS
```

| Suite | Tests | Covers |
| --- | ---: | --- |
| `ConfigTest` | 7 | JSON loading, defaults, required fields, same-directory rejection, negative values, unknown settings |
| `SatelliteImageNameTest` | 10 | Valid names, leap day, wrong extension, wrong digit count, `.tmp` suffix, impossible dates |
| `ArchivePathResolverTest` | 3 | Zero-padded hierarchy, creation, year boundary, many callers racing for the same new day |
| `FileMoverTest` | 5 | Ranged append, a range spanning several transfer calls, atomic publish, staging always starting empty, offset mismatch refused, publish-over-existing refused with rollback |
| `MainTest` | 3 | Archive directory creation with parents, missing source, non-directory path |
| `Await` | – | Shared polling helper; no test of its own |
| `StabilityDetectorTest` | 3 | Size and mtime comparison, absent previous snapshot, quiet-period boundary |
| `SourceDetectorTest` | 7 | Startup listing, files appearing later, every regular file reported, directories skipped, silence after close, first sightings vs repeat listings |
| `EventKindHandlerTest` | 3 | Created files reported with their path resolved against the watched directory, overflow answered with a listing on the scheduler rather than inline, unregistered kinds ignored |
| `ArchiveRepositoryIT` | 6 | Dedupe key, terminal-name reporting, archive bookkeeping, refusal of absent/terminal transitions, in-progress listing, WAL and `synchronous` |
| `ArchiveReconcilerIT` | 4 | Published file adopted, identical leftover source removed, incomplete publication discarded, stale staging discarded, unrecoverable shapes failed |
| `ArchiverIT` | 7 | Startup and watch detection, malformed files ignored, no move while a producer writes, listing-only fallback, source shrink failed, stale staging discarded, many same-day images at once, a transfer stopped midway left in progress |

## Manual end-to-end test cases

Executed against `target/astropipe.jar` with the shipped `config.json`, on
Linux, Java 25, 2026-09-27, in a scratch directory; paths are shortened to `…`.
Images were 8 MB of `/dev/urandom` unless a case says otherwise.

### M1 — Startup and detection

1. `rm -rf data logs && mkdir -p data/source`
2. `java -jar target/astropipe.jar`

**Expected:** archive directory created, both file stores logged, reconciliation
reports zeroes, monitoring begins.
**Observed:** pass.

```
INFO  [main] io.astropipe.Main - Created archiveDir directory=…/data/archive
INFO  [main] io.astropipe.Main - Startup reconciliation: archived=0 resumable=0 failed=0
INFO  [main] io.astropipe.Main - Monitoring source directory=…/data/source
```

### M2 — Archive placement and integrity

1. Copy an 8 MB image to `data/source/SAT_IMG_20210714102730046.raw`.
2. Copy the same bytes to `data/source/SAT_IMG_20240229235959999.raw` (leap day).
3. Write `data/source/not-an-image.txt`.

**Expected:** both images land under the zero-padded date of their own
timestamp, byte-identical; sources removed; no `.tmp` left behind; the malformed
file is left in place.
**Observed:** pass.

```
data/archive/2021/07/14/SAT_IMG_20210714102730046.raw
data/archive/2024/02/29/SAT_IMG_20240229235959999.raw
```

MD5 of source and of both destinations: `893fb4e0b78017fe1e5f3c7f42301d58`.
No `.tmp` was left behind.
`data/source` retained only `not-an-image.txt`.

### M3 — Database entry

**Expected:** one row per image carrying the name, the time saved and the saved
folder.
**Observed:** pass.

`file_name, status, archive_dir, saved_at`:

```
('SAT_IMG_20210714102730046.raw', 'ARCHIVED', '…/data/archive/2021/07/14', '2026-09-27T15:25:37.549951086Z')
('SAT_IMG_20240229235959999.raw', 'ARCHIVED', '…/data/archive/2024/02/29', '2026-09-27T15:25:37.549770815Z')
```

### M4 — Logging

**Expected:** every operation recorded in `logs/astropipe.log` as well as on the
console.
**Observed:** pass. Detection, staging, archival with byte count and duration,
and rejection of the malformed entry were all present.

```
WARN  [source-watcher] i.a.SourceWatcher - Ignoring malformed source entry=…/data/source/not-an-image.txt
      detectedBy=watch ENTRY_CREATE reason=File name does not match SAT_IMG_<timestamp>.raw: not-an-image.txt
INFO  [transfer-1] i.a.ImageTransfer - Detected file=SAT_IMG_20210714102730046.raw detectedBy=watch ENTRY_CREATE
INFO  [transfer-1] i.a.ImageTransfer - Staged file=SAT_IMG_20210714102730046.raw copiedBytes=8388608 appendedBytes=8388608
INFO  [transfer-1] i.a.ImageTransfer - Archived file=SAT_IMG_20210714102730046.raw
      destination=…/data/archive/2021/07/14/SAT_IMG_20210714102730046.raw bytes=8388608 durationMs=2
```

Each image's lines carry the name of its own transfer thread; the other image ran
on `transfer-2`. The malformed entry is reported once.

### M5 — JSON configuration

**Expected:** source, archive and database paths are all taken from
`config.json`; a bad file fails at startup with a specific message.
**Observed:** pass. Paths in the run above match the shipped file. Rejection
messages are asserted by `ConfigTest`: `sourceDir must not be blank`,
`sourceDir and archiveDir must be different directories`,
`pollIntervalMillis must be greater than zero`, and an unknown setting by name.

### M6 — Duplicate name refused

1. Copy the same bytes back to `data/source/SAT_IMG_20210714102730046.raw`,
   a name already `ARCHIVED`.

**Expected:** the file is not moved, the archived copy is untouched, and no row
is added or changed.
**Observed:** pass. The duplicate remained in `data/source`, the archive still
held exactly two files with no `.tmp`, and the ledger still held two rows with
their original `saved_at` values.

### M7 — Clean shutdown

1. `kill -TERM <pid>`

**Expected:** the watcher stops, transfers stop, the database connection closes,
and the WAL is checkpointed into the database file.
**Observed:** pass.

```
INFO  [main] io.astropipe.Main - Shutdown started
INFO  [main] io.astropipe.Main - Shutdown complete
```

`data/archive.db-wal` and `data/archive.db-shm` were removed on exit, leaving
only `data/archive.db`.

### M8 — Crash mid-copy recovers by copying again

1. Write a 40 MB image into `data/source` in place, one megabyte at a time.
2. Once staging passes 5 MB, `kill -9` the JVM: a crash with no clean shutdown.
3. Let the writer finish, then restart Astropipe.

**Expected:** the incremental copy runs while the producer writes; after the
crash the stale staging file is discarded, the image is copied again from the
start, and the archived bytes match the source exactly.
**Observed:** pass.

```
Staged file=SAT_IMG_20240302120000000.raw copiedBytes=1048576 appendedBytes=1048576
Staged file=SAT_IMG_20240302120000000.raw copiedBytes=3145728 appendedBytes=2097152
Staged file=SAT_IMG_20240302120000000.raw copiedBytes=6291456 appendedBytes=3145728
        <SIGKILL; ledger row left IN_PROGRESS, staging left on disk>
ArchiveReconciler - Source survives and will be copied again from the start: SAT_IMG_20240302120000000.raw
Startup reconciliation: archived=0 resumable=1 failed=0
Staged file=SAT_IMG_20240302120000000.raw copiedBytes=41943040 appendedBytes=41943040
Archived file=SAT_IMG_20240302120000000.raw bytes=41943040 durationMs=4
```

A poll that finds several megabytes written since the last one copies them in one
pass. MD5 of source and of the archived copy: `3e0baa6eeb10201f986a459e782c74c2`.

### M9 — Concurrent burst keeps every image intact

1. Prepare 200 distinct 256 KB images with known MD5s, 50 for each of four days.
2. Drop them into the source directory from eight concurrent writers, each using
   the staging-plus-atomic-rename producer contract: copy into a sibling directory
   on the same filesystem, then `mv` into place.

**Expected:** all 200 archived under the date of their own timestamp, byte-exact,
no duplicates, no losses, no leftover `.tmp`, and one `ARCHIVED` ledger row each
carrying a saved folder and saved time.
**Observed:** pass.

```
expected: 200   archived files: 200
missing: 0      unexpected: 0     checksum mismatches: 0
wrong date folder: 0
ledger rows: [('ARCHIVED', 200)]
rows missing archive_dir or saved_at: 0
leftover .tmp: 0   errors in log: 0
```

The run also confirms the threading model: each image was followed from detection
to archive by one thread of its own.

```
events: {'Detected': 200, 'Staged': 200, 'Archived': 200}
images whose events all came from one thread: 200 of 200
distinct transfer threads: 200   (all named transfer-N: True)
```

### M10 — Shutdown mid-copy stops without failing, and the next start resumes

1. Place a 1 GB image in `data/source` and start Astropipe; the startup listing
   begins copying it.
2. While staging holds only part of it, `kill -TERM <pid>`.
3. Restart Astropipe.

**Expected:** the process exits promptly, without waiting for the copy; the
ledger row stays `IN_PROGRESS`, not `FAILED`; on restart the partial staging is
discarded and the image is copied again from the start and archived byte-exact.
**Observed:** pass.

```
SIGTERM with staging at 2424832 of 1073741824 bytes; process exited 347 ms later; staging left at 67108864 bytes
INFO  [transfer-2] i.a.ImageTransfer - Detected file=SAT_IMG_20240304120000000.raw detectedBy=startup listing
INFO  [main] io.astropipe.Main - Shutdown started
INFO  [transfer-2] i.a.ImageTransfer - Stopped before archiving file=SAT_IMG_20240304120000000.raw; it stays in progress for the next start
INFO  [main] io.astropipe.Main - Shutdown complete
ledger status after stop: ('IN_PROGRESS',)
        <restart>
INFO  [main] i.a.ArchiveReconciler - Source survives and will be copied again from the start: SAT_IMG_20240304120000000.raw
INFO  [main] io.astropipe.Main - Startup reconciliation: archived=0 resumable=1 failed=0
INFO  [transfer-2] i.a.ImageTransfer - Archived file=SAT_IMG_20240304120000000.raw … bytes=1073741824 durationMs=55
ledger status after restart: ('ARCHIVED',)
```

MD5 of source and of the archived copy: `fe471c8475a5597706cd11cdc7135998`. The
copy stopped at one 64 MiB transfer call: a file copy already in the kernel ignores
an interrupt, so `FileMover` caps each call. Before that cap, the same stop waited
8.2 s for the whole gigabyte to be copied and then discarded it.

### M11 — Copies are bounded by `workerPoolSize`

1. Drop six 256 MB images into `data/source` at once (default `workerPoolSize` of 2).
2. While they copy, take thread dumps (`jcmd <pid> Thread.dump_to_file`) about every
   300 ms.

**Expected:** never more than two transfers copying at once; the others wait for
a permit; all six archived.
**Observed:** pass. Copying / waiting for a permit, per dump:

```
2/4  2/4  2/4  2/4  2/4  2/4  2/4  2/2  2/4  2/4  2/4  0/0  0/0
archived: 6 of 6; errors in log: 0
```

## Known gaps

- The ledger is never pruned. That is deliberate — old names are what reject
  duplicates — but it grows without bound and has no documented retention story.
- The packaged jar carries native SQLite libraries for 64-bit Linux and macOS only.
- An image interrupted mid-copy, by a crash or by shutdown, is copied again from the beginning on restart.
  This is deliberate (see README), but it is worth measuring before pointing
  Astropipe at files where a re-copy would be expensive.
- Performance has not been measured against multi-gigabyte images or a saturated
  device; `durationMs` is logged per file so a campaign can be run without code
  changes.
- Remote storage (NFS, CIFS) is untested. Periodic reconciliation limits the risk
  of missed `WatchService` events, but it needs separate operational testing.
