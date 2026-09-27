# Validation

## Automated tests

```bash
mvn verify
```

This runs 43 unit tests (`src/test/java`) and 18 integration tests (`src/it/java`), which use the
real filesystem, `WatchService` and SQLite. Last run: Linux, Java 25, 2026-09-27, all passing.

Tests never sleep for a fixed time: they wait for a condition up to a deadline, with short poll and
quiet periods. None depends on how fast `WatchService` reports a new file, since that differs
between platforms.

| Suite | Tests | Covers |
| --- | ---: | --- |
| `ConfigTest` | 7 | Loading, defaults, rejection of invalid settings |
| `SatelliteImageNameTest` | 12 | Name parsing and date folder, including leap day and year boundary |
| `FileMoverTest` | 6 | Incremental copy into staging, atomic publish, rollback |
| `StabilityDetectorTest` | 3 | Size and modification-time checks, quiet period |
| `SourceDetectorTest` | 8 | Startup and periodic listings, new files, stopping |
| `EventKindHandlerTest` | 4 | Created files (directories ignored), overflow, other event kinds |
| `MainTest` | 3 | Source and archive directory checks |
| `ArchiveRepositoryIT` | 6 | The ledger: duplicate names, status changes, what is recorded |
| `ArchiveReconcilerIT` | 4 | Startup recovery of images left in progress |
| `ArchiverIT` | 8 | End to end: detection, a producer still writing, no `WatchService`, failures, a stop mid-copy, many images at once |

## Requirements coverage

| Requirement | Automated | Manual |
| --- | --- | --- |
| Monitor the source folder | `SourceDetectorTest`, `EventKindHandlerTest`, `ArchiverIT.archivesStartupAndWatchEventsAndIgnoresMalformedFiles`, `ArchiverIT.fallsBackToReconciliationWhenWatchServiceIsUnavailable` | M1, M2 |
| Move images to the archive as fast as possible | `ArchiverIT.doesNotMoveFileWhileUnsafeGeneratorIsStillWriting`, `ArchiverIT.archivesManyImagesOfTheSameDayArrivingTogether` | M5, M8 |
| Log all file operations to a log file | | M3 |
| JSON configuration with source and archive folders | `ConfigTest` | M1 |
| Create `<Year>/<Month>/<Day>` if missing | `SatelliteImageNameTest.placesTheImageUnderItsZeroPaddedDate`, `FileMoverTest.createsTheArchiveDirectoryOfANewStagingFile` | M2 |
| Record file name, time saved and saved folder | `ArchiveRepositoryIT.recordsNameSavedTimeAndSavedFolderOnArchive` | M2 |
| Never process the same name twice | `ArchiveRepositoryIT.primaryKeyIsAuthoritativeDedupeGuard` | M4 |
| Recover after a crash or a stop (beyond the brief) | `ArchiveReconcilerIT`, `ArchiverIT.leavesATransferStoppedMidwayInProgressForTheNextStart` | M6, M7 |

## Manual checks

Run with `target/astropipe.jar` and the shipped `config.json` on Linux, Java 25, 2026-09-27.
Images were random bytes.

| # | Action | Expected | Result |
| --- | --- | --- | --- |
| M1 | Start with only `data/source` present | Archive folder created, startup reconciliation reports zeroes, monitoring starts | Pass |
| M2 | Copy `SAT_IMG_20210714102730046.raw`, `SAT_IMG_20240229235959999.raw` (8 MB each) and `not-an-image.txt` into the source | Images under `2021/07/14` and `2024/02/29` with the source's MD5; sources removed; no `.tmp` left; one ledger row each; the `.txt` stays, with one warning | Pass |
| M3 | Read `logs/astropipe.log` | Detection, each copied range, and archival with size and duration, for every image | Pass |
| M4 | Copy an already archived name into the source again | Left in the source; archive and ledger unchanged | Pass |
| M5 | Eight writers move in 200 images of 256 KB, 50 for each of four days | All 200 under their own date with matching MD5, 200 `ARCHIVED` rows, no errors | Pass |
| M6 | `kill -9` while a 40 MB image is still being written and copied, then restart | Row left `IN_PROGRESS`; on restart the partial copy is discarded and the image copied again with matching MD5 | Pass |
| M7 | SIGTERM while a 1 GB image is being copied, then restart | Exits in under half a second; row left `IN_PROGRESS`, not `FAILED`; restart archives it with matching MD5 | Pass |
| M8 | Six 256 MB images at once, `workerPoolSize` 2, thread dumps every 300 ms | Never more than two copies at a time; each image published within 0.3 s of its own copy | Pass |
| M9 | SIGTERM while idle | `Shutdown complete`; `archive.db-wal` and `archive.db-shm` removed on exit | Pass |

Output of M2 and M3, paths shortened:

```
data/archive/2021/07/14/SAT_IMG_20210714102730046.raw
data/archive/2024/02/29/SAT_IMG_20240229235959999.raw

('SAT_IMG_20210714102730046.raw', 'ARCHIVED', '…/data/archive/2021/07/14', '2026-09-27T19:24:15.662484188Z')
('SAT_IMG_20240229235959999.raw', 'ARCHIVED', '…/data/archive/2024/02/29', '2026-09-27T19:24:15.696634475Z')

INFO  [transfer-1] i.a.ImageTransfer - Detected file=SAT_IMG_20210714102730046.raw detectedBy=watch ENTRY_CREATE
INFO  [transfer-1] i.a.ImageTransfer - Staged file=SAT_IMG_20210714102730046.raw copiedBytes=3997696 appendedBytes=3997696
INFO  [transfer-1] i.a.ImageTransfer - Staged file=SAT_IMG_20210714102730046.raw copiedBytes=8388608 appendedBytes=4390912
WARN  [source-watcher] i.a.SourceWatcher - Ignoring malformed source entry=…/data/source/not-an-image.txt detectedBy=watch ENTRY_CREATE reason=File name does not match SAT_IMG_<timestamp>.raw: not-an-image.txt
INFO  [transfer-1] i.a.ImageTransfer - Archived file=SAT_IMG_20210714102730046.raw destination=…/data/archive/2021/07/14/SAT_IMG_20210714102730046.raw bytes=8388608 durationMs=1
```

## Not covered

- Only Linux was tried. The jar also carries the macOS SQLite library, and no other platform's.
- Network filesystems (NFS, CIFS) are untested.
- Performance was not measured with many multi-gigabyte images or a saturated disk; each archival logs
  `durationMs` for that.
- The ledger is never pruned, because its rows are what reject duplicate names.
