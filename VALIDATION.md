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
passing means something: breaking the deduplication guard, keeping a truncated
publication instead of discarding it, and publishing without waiting for the quiet
period were each caught, by `ArchiveRepositoryIT`, `ArchiveReconcilerIT` and
`ArchiverIT` respectively.

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
| Monitor the source folder | `SourceDetectorTest` (5 tests), `ArchiverIT.archivesStartupAndWatchEventsAndIgnoresMalformedFiles`, `ArchiverIT.fallsBackToReconciliationWhenWatchServiceIsUnavailable`, M1 |
| Move images to the archive as fast as possible | `ArchiverIT.doesNotMoveFileWhileUnsafeGeneratorIsStillWriting`, M2 |
| Log all file operations to a log file | M4 |
| Use a JSON configuration file | `ConfigTest` (7 tests), M5 |
| Source folder configurable | `ConfigTest.loadsTheShippedConfigurationFile`, M5 |
| Archive folder configurable | `ConfigTest.loadsTheShippedConfigurationFile`, M5 |
| Create `<Year>/<Month>/<Day>` if missing, zero-padded | `ArchivePathResolverTest` (2 tests), M2 |
| Record file name, time saved and saved folder | `ArchiveRepositoryIT.recordsNameSavedTimeAndSavedFolderOnArchive`, M3 |
| Never process the same name twice | `ArchiveRepositoryIT.primaryKeyIsAuthoritativeDedupeGuard`, M6 |

## Automated results

`mvn verify` on Linux, Java 21, run 2026-09-24:

```
Tests run: 33, Failures: 0, Errors: 0, Skipped: 0   (surefire)
Tests run: 16, Failures: 0, Errors: 0, Skipped: 0   (failsafe)
BUILD SUCCESS
```

| Suite | Tests | Covers |
| --- | ---: | --- |
| `ConfigTest` | 7 | JSON loading, defaults, required fields, same-directory rejection, negative values, unknown settings |
| `SatelliteImageNameTest` | 9 | Valid names, leap day, wrong extension, wrong digit count, `.tmp` suffix, impossible dates |
| `ArchivePathResolverTest` | 2 | Zero-padded hierarchy, creation, year boundary |
| `FileMoverTest` | 4 | Ranged append, atomic publish, staging always starting empty, offset mismatch refused, publish-over-existing refused with rollback |
| `MainTest` | 3 | Archive directory creation with parents, missing source, non-directory path |
| `Await` | – | Shared polling helper; no test of its own |
| `StabilityDetectorTest` | 3 | Size and mtime comparison, absent previous snapshot, quiet-period boundary |
| `SourceDetectorTest` | 5 | Startup listing, files appearing later, every regular file reported, directories skipped, silence after close |
| `ArchiveRepositoryIT` | 7 | Dedupe key, terminal-name reporting, archive bookkeeping, refusal of absent/terminal transitions, in-progress listing, WAL and `synchronous`, legacy schema rejection |
| `ArchiveReconcilerIT` | 4 | Published file adopted, identical leftover source removed, incomplete publication discarded, stale staging discarded, unrecoverable shapes failed |
| `ArchiverIT` | 5 | Startup and watch detection, malformed files ignored, no move while a producer writes, listing-only fallback, source shrink failed, stale staging discarded |

## Manual end-to-end test cases

Executed against `target/astropipe.jar` with the shipped `config.json`, on
Linux, Java 21, 2026-09-24. Images were 8 MB of `/dev/urandom`.

### M1 — Startup and detection

1. `rm -rf data logs && mkdir -p data/source`
2. `java -jar target/astropipe.jar`

**Expected:** archive directory created, both file stores logged, reconciliation
reports zeroes, monitoring begins.
**Observed:** pass.

```
Created archiveDir directory=/home/user/astropipe/data/archive
Startup reconciliation: archived=0 resumable=0 failed=0
Monitoring source directory=... archive directory=...
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

MD5 of source and of both destinations: `27ea8eb8b6d93238665cd2ccc8a43d76`.
`data/source` retained only `not-an-image.txt`.

### M3 — Database entry

**Expected:** one row per image carrying the name, the time saved and the saved
folder.
**Observed:** pass.

```
('SAT_IMG_20210714102730046.raw', 'ARCHIVED', 8388608,
 '/home/user/astropipe/data/archive/2021/07/14', '2026-09-24T12:18:44.544832526Z')
('SAT_IMG_20240229235959999.raw', 'ARCHIVED', 8388608,
 '/home/user/astropipe/data/archive/2024/02/29', '2026-09-24T12:18:44.544734012Z')
```

### M4 — Logging

**Expected:** every operation recorded in `logs/astropipe.log` as well as on the
console.
**Observed:** pass. Detection, staging, archival with byte count and duration,
and rejection of the malformed entry were all present.

```
INFO  [archive-worker-1] i.a.SourceWatcher - Archived file=SAT_IMG_20210714102730046.raw
      destination=.../2021/07/14/SAT_IMG_20210714102730046.raw bytes=8388608 durationMs=1
WARN  [reconciliation-1] i.a.SourceWatcher - Ignoring malformed source entry=.../not-an-image.txt
      reason=File name does not match SAT_IMG_<timestamp>.raw: not-an-image.txt
```

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

**Expected:** the watcher stops, workers drain, the database connection closes,
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
Staged file=SAT_IMG_...raw copiedBytes=3145728  appendedBytes=3145728
Staged file=SAT_IMG_...raw copiedBytes=4194304  appendedBytes=1048576
Staged file=SAT_IMG_...raw copiedBytes=5242880  appendedBytes=1048576
        <SIGKILL; ledger row left IN_PROGRESS, staging left on disk>
ArchiveReconciler - Source survives and will be copied again from the start: SAT_IMG_...raw
Startup reconciliation: archived=0 resumable=1 failed=0
Staged file=SAT_IMG_...raw copiedBytes=41943040 appendedBytes=41943040
Archived file=SAT_IMG_...raw bytes=41943040 durationMs=4
```

MD5 of source and of the archived copy: `4502163cc345866f84ac377adc3526e6`.

### M9 — Concurrent burst keeps every image intact

1. Prepare 200 distinct images with known MD5s.
2. Drop them into the source directory from eight concurrent writers, each using
   the staging-plus-atomic-rename producer contract.

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

The run also confirms the threading contract. Every state-changing event was
logged by the single scheduler thread:

```
Detected file=   [reconciliation-1]  (count: 200)
Staged file=     [reconciliation-1]  (count: 200)
Archived file=   [reconciliation-1]  (count: 200)
threads in log:  1000 x [reconciliation-1], 7 x [main]
```

## Known gaps

- The ledger is never pruned. That is deliberate — old names are what reject
  duplicates — but it grows without bound and has no documented retention story.
- The packaged jar carries native SQLite libraries for 64-bit Linux and macOS only.
- An image interrupted mid-copy is copied again from the beginning on restart.
  This is deliberate (see README), but it is worth measuring before pointing
  Astropipe at files where a re-copy would be expensive.
- Performance has not been measured against multi-gigabyte images or a saturated
  device; `durationMs` is logged per file so a campaign can be run without code
  changes.
- Remote storage (NFS, CIFS) is untested. Periodic reconciliation limits the risk
  of missed `WatchService` events, but it needs separate operational testing.
- A malformed entry left in the source directory is re-reported by every
  reconciliation sweep, so the log repeats that warning until the file is removed.
