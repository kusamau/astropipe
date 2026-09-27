package io.astropipe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.concurrent.Semaphore;

/**
 * Follows one image from first detection to the archive on a thread of its own: claim it,
 * stage what is there, append while the producer writes, publish once the source has held
 * still, record the outcome. An interrupt stops it without failing the image: the row stays
 * in progress, and startup reconciliation copies it again from the start.
 */
final class ImageTransfer {
    private static final Logger logger = LoggerFactory.getLogger(ImageTransfer.class);

    private final Config config;
    private final StabilityDetector stabilityDetector;
    private final Path archiveRoot;
    private final ArchiveRepository repository;
    /** How many images may copy or publish at the same time. */
    private final Semaphore ioPermits;

    ImageTransfer(Config config, Path archiveRoot, ArchiveRepository repository) {
        this.config = config;
        this.stabilityDetector = new StabilityDetector(config.quietPeriodMillis());
        this.archiveRoot = archiveRoot;
        this.repository = repository;
        this.ioPermits = new Semaphore(config.workerPoolSize());
    }

    /** Returns once the image is archived, has failed, or the thread was interrupted. */
    void archive(Path source, SatelliteImageName image, SourceDetector.Detection detection) {
        String fileName = image.fileName();
        ArchiveRepository.Status status;
        try {
            status = repository.claim(fileName, Instant.now());
        } catch (RuntimeException e) {
            logger.error("Could not claim file={} in the archive ledger", fileName, e);
            return;
        }
        if (status.terminal()) {
            logger.debug("Ignoring terminal file={} status={} detectedBy={}", fileName, status, detection);
            return;
        }

        // The ledger row is in progress from here on: a failure has to close it out, a stop must not.
        Path staging = null;
        try {
            Path destination = image.archivePath(archiveRoot);
            staging = FileMover.stagingPath(destination);
            transfer(source, fileName, destination, staging, detection);
        } catch (ClosedByInterruptException | InterruptedException stopped) {
            Thread.currentThread().interrupt();
            logger.info("Stopped before archiving file={}; it stays in progress for the next start", fileName);
        } catch (Exception failure) {
            markFailed(fileName, failure);
            logger.error("Archive operation failed for file={} staging={}", source, staging, failure);
        }
    }

    private void transfer(Path source, String fileName, Path destination, Path staging,
                          SourceDetector.Detection detection) throws IOException, InterruptedException {
        FileMover.createStaging(staging);
        logger.info("Detected file={} detectedBy={}", fileName, detection);

        long copied = 0;
        StabilityDetector.Snapshot quietFrom = null;
        long quietSince = 0;
        while (true) {
            StabilityDetector.Snapshot current = stabilityDetector.sample(source);
            requireNoShrink(source, current.size(), copied);
            if (current.size() > copied) {
                long from = copied;
                copied = copy(source, staging, from, current.size());
                logger.info("Staged file={} copiedBytes={} appendedBytes={}", fileName, copied, copied - from);
                continue;
            }
            if (!stabilityDetector.unchanged(quietFrom, current)) {
                quietFrom = current;
                quietSince = System.nanoTime();
            }
            if (stabilityDetector.quietFor(quietSince, System.nanoTime())) {
                long started = System.nanoTime();
                if (publish(source, staging, destination, copied, quietFrom) == Publication.DONE) {
                    recordArchived(fileName, destination);
                    logger.info("Archived file={} destination={} bytes={} durationMs={}",
                            fileName, destination, copied, (System.nanoTime() - started) / 1_000_000);
                    return;
                }
                logger.debug("Source changed while publishing file={}; waiting for quiet again", fileName);
                continue;
            }
            Thread.sleep(config.pollIntervalMillis());
        }
    }

    private long copy(Path source, Path staging, long from, long to) throws IOException, InterruptedException {
        ioPermits.acquire();
        try {
            return FileMover.appendRange(source, staging, from, to);
        } finally {
            ioPermits.release();
        }
    }

    /** Publishes staging, or reports that the source moved and the quiet period must restart. */
    private Publication publish(Path source, Path staging, Path destination, long copied,
                                StabilityDetector.Snapshot quietFrom) throws IOException, InterruptedException {
        ioPermits.acquire();
        try {
            StabilityDetector.Snapshot before = stabilityDetector.sample(source);
            if (before.size() != copied || !stabilityDetector.unchanged(quietFrom, before)) {
                return Publication.RETRY;
            }

            FileMover.publish(staging, destination);
            try {
                StabilityDetector.Snapshot after = stabilityDetector.sample(source);
                if (!stabilityDetector.unchanged(before, after)) {
                    FileMover.rollbackPublication(destination, staging);
                    return Publication.RETRY;
                }
                Files.delete(source);
                return Publication.DONE;
            } catch (IOException failure) {
                if (Files.exists(destination) && !Files.exists(staging)) {
                    try {
                        FileMover.rollbackPublication(destination, staging);
                    } catch (IOException rollbackFailure) {
                        failure.addSuppressed(rollbackFailure);
                    }
                }
                throw failure;
            }
        } finally {
            ioPermits.release();
        }
    }

    private void recordArchived(String fileName, Path destination) {
        try {
            repository.markArchived(fileName, destination.getParent(), Instant.now());
        } catch (RuntimeException databaseFailure) {
            logger.error("Published file but could not mark it ARCHIVED; startup recovery will reconcile file={}",
                    fileName, databaseFailure);
        }
    }

    private void markFailed(String fileName, Exception failure) {
        try {
            repository.markFailed(fileName);
        } catch (RuntimeException updateFailure) {
            failure.addSuppressed(updateFailure);
        }
    }

    private static void requireNoShrink(Path source, long sourceSize, long copiedBytes) throws IOException {
        if (sourceSize < copiedBytes) {
            throw new IOException("Append-only source shrank below what was already copied for " + source
                    + ": sourceSize=" + sourceSize + " copiedBytes=" + copiedBytes);
        }
    }

    private enum Publication { DONE, RETRY }
}
