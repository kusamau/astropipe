package io.astropipe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Drives each detected image through staging to the archive.
 *
 * <p>Finding files is {@link SourceDetector}'s job; this class decides what happens to
 * one once it has been found. A detected file is only published after its size and
 * modification time have held still for the configured quiet period.
 */
final class SourceWatcher implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(SourceWatcher.class);

    private final Config config;
    private final StabilityDetector stabilityDetector;
    private final ArchivePathResolver pathResolver;
    private final FileMover fileMover;
    private final ArchiveRepository repository;
    private final Executor workers;
    private final ScheduledExecutorService scheduler;
    private final SourceDetector detector;
    private final Set<Path> inFlight = ConcurrentHashMap.newKeySet();
    private final ConcurrentHashMap<Path, Candidate> candidates = new ConcurrentHashMap<>();
    private final AtomicBoolean running = new AtomicBoolean();

    SourceWatcher(
            Path sourceDirectory,
            Config config,
            ArchivePathResolver pathResolver,
            FileMover fileMover,
            ArchiveRepository repository,
            Executor workers,
            ScheduledExecutorService scheduler) {
        this.config = config;
        this.stabilityDetector = new StabilityDetector(config.quietPeriodMillis());
        this.pathResolver = pathResolver;
        this.fileMover = fileMover;
        this.repository = repository;
        this.workers = workers;
        this.scheduler = scheduler;
        this.detector = new SourceDetector(
                sourceDirectory, scheduler, config.reconciliationIntervalSeconds(), this::accept);
    }

    void start() {
        running.set(true);
        detector.start();
    }

    void awaitTermination() throws InterruptedException {
        detector.awaitTermination();
    }

    @Override
    public void close() {
        if (!running.getAndSet(false)) return;
        detector.close();
    }

    /** Takes one detected file and either starts tracking it or explains why not. */
    private void accept(Path source, String reason) {
        String fileName = source.getFileName().toString();
        LocalDateTime timestamp;
        try {
            timestamp = SatelliteImageName.parse(fileName);
        } catch (SatelliteImageName.InvalidNameException invalid) {
            logger.warn("Ignoring malformed source entry={} detectedBy={} reason={}",
                    source, reason, invalid.getMessage());
            return;
        }

        Candidate tracked = candidates.get(source);
        if (tracked != null) {
            scheduleCheck(tracked, 0);
            return;
        }
        if (!inFlight.add(source)) return;

        ArchiveRepository.Status status;
        try {
            status = repository.claim(fileName, Instant.now());
        } catch (RuntimeException e) {
            inFlight.remove(source);
            logger.error("Could not claim file={} in the archive ledger", fileName, e);
            return;
        }
        if (status.terminal()) {
            inFlight.remove(source);
            logger.debug("Ignoring terminal file={} status={} detectedBy={}", fileName, status, reason);
            return;
        }

        // The ledger row is active from here on, so every failure below has to close it out.
        try {
            Path destination = pathResolver.resolve(fileName, timestamp);
            Path staging = fileMover.stagingPath(destination);
            fileMover.createStaging(staging);

            Candidate candidate = new Candidate(source, fileName, destination, staging);
            candidates.put(source, candidate);
            logger.info("Detected file={} detectedBy={}", fileName, reason);
            StabilityDetector.Snapshot snapshot = stabilityDetector.sample(source);
            if (snapshot.size() > 0) {
                submit(candidate, this::copyAvailable);
            } else {
                candidate.markQuietFrom(snapshot);
                scheduleCheck(candidate, config.pollIntervalMillis());
            }
        } catch (Exception failure) {
            candidates.remove(source);
            inFlight.remove(source);
            markFailed(fileName, failure);
            logger.error("Could not initialize incremental copy for file={}", source, failure);
        }
    }

    /** Runs one exclusive pass over a candidate on a worker thread. */
    private void submit(Candidate candidate, Consumer<Candidate> pass) {
        if (!running.get() || !candidate.workScheduled.compareAndSet(false, true)) return;
        try {
            workers.execute(() -> pass.accept(candidate));
        } catch (RuntimeException rejected) {
            candidate.workScheduled.set(false);
            fail(candidate, rejected);
        }
    }

    private void copyAvailable(Candidate candidate) {
        StabilityDetector.Snapshot afterCopy = null;
        boolean copied = false;
        try {
            StabilityDetector.Snapshot beforeCopy = stabilityDetector.sample(candidate.source);
            long copiedSoFar = candidate.copiedBytes;
            requireNoShrink(candidate.source, beforeCopy.size(), copiedSoFar);

            long copiedBytes = fileMover.appendRange(
                    candidate.source, candidate.staging, copiedSoFar, beforeCopy.size());
            candidate.copiedBytes = copiedBytes;
            afterCopy = stabilityDetector.sample(candidate.source);
            candidate.markQuietFrom(afterCopy);
            copied = true;
            logger.info("Staged file={} copiedBytes={} appendedBytes={}",
                    candidate.fileName, copiedBytes, copiedBytes - copiedSoFar);
        } catch (Exception failure) {
            fail(candidate, failure);
        } finally {
            candidate.workScheduled.set(false);
        }

        if (!copied || !running.get() || !candidates.containsKey(candidate.source)) return;
        if (afterCopy.size() > candidate.copiedBytes) {
            submit(candidate, this::copyAvailable);
        } else {
            scheduleCheck(candidate, config.pollIntervalMillis());
        }
    }

    private void scheduleCheck(Candidate candidate, long delayMillis) {
        if (!running.get() || !candidate.checkScheduled.compareAndSet(false, true)) return;
        try {
            scheduler.schedule(() -> checkStability(candidate), delayMillis, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException ignored) {
            candidate.checkScheduled.set(false);
        }
    }

    private void checkStability(Candidate candidate) {
        candidate.checkScheduled.set(false);
        if (!running.get() || !candidates.containsKey(candidate.source)) return;
        if (candidate.workScheduled.get()) {
            scheduleCheck(candidate, config.pollIntervalMillis());
            return;
        }
        try {
            StabilityDetector.Snapshot current = stabilityDetector.sample(candidate.source);
            requireNoShrink(candidate.source, current.size(), candidate.copiedBytes);
            if (current.size() > candidate.copiedBytes) {
                submit(candidate, this::copyAvailable);
                return;
            }

            if (!stabilityDetector.unchanged(candidate.stagedSnapshot, current)) {
                candidate.markQuietFrom(current);
            }
            if (stabilityDetector.quietFor(candidate.quietSinceNanos, System.nanoTime())) {
                submit(candidate, this::publish);
            } else {
                scheduleCheck(candidate, config.pollIntervalMillis());
            }
        } catch (Exception failure) {
            fail(candidate, failure);
        }
    }

    private void publish(Candidate candidate) {
        boolean published = false;
        boolean retry = false;
        long started = System.nanoTime();
        try {
            StabilityDetector.Snapshot beforePublish = stabilityDetector.sample(candidate.source);
            if (beforePublish.size() != candidate.copiedBytes
                    || !stabilityDetector.unchanged(candidate.stagedSnapshot, beforePublish)) {
                candidate.markQuietFrom(beforePublish);
                retry = true;
                return;
            }

            fileMover.publish(candidate.staging, candidate.destination);
            published = true;
            StabilityDetector.Snapshot afterPublish = stabilityDetector.sample(candidate.source);
            if (!stabilityDetector.unchanged(beforePublish, afterPublish)) {
                fileMover.rollbackPublication(candidate.destination, candidate.staging);
                published = false;
                candidate.markQuietFrom(afterPublish);
                retry = true;
                return;
            }

            Files.delete(candidate.source);
            try {
                repository.markArchived(candidate.fileName, candidate.destination.getParent(), Instant.now());
            } catch (RuntimeException databaseFailure) {
                logger.error("Published file but could not mark it ARCHIVED; startup recovery will reconcile file={}",
                        candidate.fileName, databaseFailure);
            }
            logger.info("Archived file={} destination={} bytes={} durationMs={}",
                    candidate.fileName, candidate.destination, candidate.copiedBytes,
                    (System.nanoTime() - started) / 1_000_000);
            discard(candidate);
        } catch (Exception failure) {
            if (published && Files.exists(candidate.destination) && !Files.exists(candidate.staging)) {
                try {
                    fileMover.rollbackPublication(candidate.destination, candidate.staging);
                } catch (IOException rollbackFailure) {
                    failure.addSuppressed(rollbackFailure);
                }
            }
            fail(candidate, failure);
        } finally {
            candidate.workScheduled.set(false);
            if (retry && running.get() && candidates.containsKey(candidate.source)) {
                if (candidate.stagedSnapshot.size() > candidate.copiedBytes) {
                    submit(candidate, this::copyAvailable);
                } else {
                    scheduleCheck(candidate, config.pollIntervalMillis());
                }
            }
        }
    }

    private void fail(Candidate candidate, Exception failure) {
        if (!candidates.containsKey(candidate.source)) return;
        markFailed(candidate.fileName, failure);
        logger.error("Incremental archive operation failed for file={} staging={}",
                candidate.source, candidate.staging, failure);
        discard(candidate);
    }

    private void markFailed(String fileName, Exception failure) {
        try {
            repository.markFailed(fileName);
        } catch (RuntimeException updateFailure) {
            failure.addSuppressed(updateFailure);
        }
    }

    private void discard(Candidate candidate) {
        candidates.remove(candidate.source, candidate);
        inFlight.remove(candidate.source);
    }

    private static void requireNoShrink(Path source, long sourceSize, long copiedBytes) throws IOException {
        if (sourceSize < copiedBytes) {
            throw new IOException("Append-only source shrank below what was already copied for " + source
                    + ": sourceSize=" + sourceSize + " copiedBytes=" + copiedBytes);
        }
    }

    private static final class Candidate {
        private final Path source;
        private final String fileName;
        private final Path destination;
        private final Path staging;
        private final AtomicBoolean workScheduled = new AtomicBoolean();
        private final AtomicBoolean checkScheduled = new AtomicBoolean();
        /** In-memory only: staging is discarded on restart, so this never outlives the run. */
        private volatile long copiedBytes;
        private volatile StabilityDetector.Snapshot stagedSnapshot;
        private volatile long quietSinceNanos;

        private Candidate(Path source, String fileName, Path destination, Path staging) {
            this.source = source;
            this.fileName = fileName;
            this.destination = destination;
            this.staging = staging;
        }

        /** Restarts the quiet period from this observation of the source. */
        private void markQuietFrom(StabilityDetector.Snapshot snapshot) {
            this.stagedSnapshot = snapshot;
            this.quietSinceNanos = System.nanoTime();
        }
    }
}
