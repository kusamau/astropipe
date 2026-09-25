package io.astropipe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Drives each detected image through staging to the archive.
 *
 * <p>Finding files is {@link SourceDetector}'s job; this class decides what happens to one
 * once it has been found. A detected file is only published after its size and modification
 * time have held still for the configured quiet period.
 *
 * <h2>Threading</h2>
 * Every decision and every mutation of a {@link Candidate} happens on the single scheduler
 * thread, which owns {@link #candidates}. Worker threads run blocking I/O only: they read
 * fields the scheduler wrote before dispatch, return a value, and never mutate shared state.
 * Handing work to an executor and posting the result back establishes the happens-before
 * edges in both directions, so none of this needs locks, atomics or {@code volatile}.
 *
 * <p>The invariant that keeps it simple: <strong>a tracked file has at most one outstanding
 * action</strong> — one worker pass or one scheduled poll, never both, never two. It holds
 * structurally, because only {@link #advance} chooses the next action and it runs only after
 * the previous one has completed. Preserve that and no guard flag is needed; break it and no
 * guard flag would save you.
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

    /** Scheduler thread only. */
    private final Map<Path, Candidate> candidates = new HashMap<>();

    private volatile boolean running;

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
                sourceDirectory, scheduler, config.reconciliationIntervalSeconds(), this::onDetected);
    }

    void start() {
        running = true;
        detector.start();
    }

    void awaitTermination() throws InterruptedException {
        detector.awaitTermination();
    }

    @Override
    public synchronized void close() {
        if (!running) return;
        running = false;
        detector.close();
    }

    /** Detector callback, on any thread: hand the file to its owner. */
    private void onDetected(Path source, SourceDetector.Detection detection) {
        onSchedulerThread(() -> admit(source, detection));
    }

    // ---------------------------------------------------------------- scheduler thread only

    /** Claims a newly seen file and starts tracking it. */
    private void admit(Path source, SourceDetector.Detection detection) {
        if (candidates.containsKey(source)) return;
        String fileName = source.getFileName().toString();

        LocalDateTime timestamp;
        try {
            timestamp = SatelliteImageName.parse(fileName);
        } catch (SatelliteImageName.InvalidNameException invalid) {
            // A name that cannot be parsed is never claimed, so every later listing meets
            // it as though it were new. Say so once, when this could be the first sighting,
            // and stay quiet on the re-scans: otherwise one stray file in the source
            // directory warns on every sweep for the life of the process.
            logger.atLevel(detection.firstSighting() ? Level.WARN : Level.DEBUG)
                    .log("Ignoring malformed source entry={} detectedBy={} reason={}",
                            source, detection, invalid.getMessage());
            return;
        }

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

        // The ledger row is in progress from here on, so a failure below has to close it out.
        Candidate candidate;
        try {
            Path destination = pathResolver.resolve(fileName, timestamp);
            Path staging = fileMover.stagingPath(destination);
            fileMover.createStaging(staging);
            candidate = new Candidate(source, fileName, destination, staging);
        } catch (Exception failure) {
            markFailed(fileName, failure);
            logger.error("Could not start archiving file={}", source, failure);
            return;
        }

        candidates.put(source, candidate);
        logger.info("Detected file={} detectedBy={}", fileName, detection);
        advance(candidate);
    }

    /**
     * The one decision point: look at the source and choose this candidate's next action.
     * Every path either dispatches exactly one worker pass, schedules exactly one poll, or
     * finishes the candidate.
     */
    private void advance(Candidate candidate) {
        if (!running || !candidates.containsKey(candidate.source)) return;
        try {
            StabilityDetector.Snapshot current = stabilityDetector.sample(candidate.source);
            requireNoShrink(candidate.source, current.size(), candidate.copiedBytes);

            if (current.size() > candidate.copiedBytes) {
                startCopy(candidate, current.size());
                return;
            }
            if (!stabilityDetector.unchanged(candidate.stagedSnapshot, current)) {
                candidate.markQuietFrom(current);
            }
            if (stabilityDetector.quietFor(candidate.quietSinceNanos, System.nanoTime())) {
                startPublication(candidate);
            } else {
                schedulePoll(candidate);
            }
        } catch (Exception failure) {
            fail(candidate, failure);
        }
    }

    private void startCopy(Candidate candidate, long upTo) {
        long from = candidate.copiedBytes;
        offload(candidate,
                () -> fileMover.appendRange(candidate.source, candidate.staging, from, upTo),
                copied -> copyFinished(candidate, from, copied));
    }

    private void copyFinished(Candidate candidate, long from, long copied) {
        candidate.copiedBytes = copied;
        logger.info("Staged file={} copiedBytes={} appendedBytes={}",
                candidate.fileName, copied, copied - from);
        advance(candidate);
    }

    private void startPublication(Candidate candidate) {
        long started = System.nanoTime();
        offload(candidate,
                () -> publishToArchive(candidate),
                outcome -> publicationFinished(candidate, outcome, started));
    }

    private void publicationFinished(Candidate candidate, Publication outcome, long startedNanos) {
        if (outcome == Publication.RETRY) {
            logger.debug("Source changed while publishing file={}; waiting for quiet again",
                    candidate.fileName);
            advance(candidate);
            return;
        }
        try {
            repository.markArchived(candidate.fileName, candidate.destination.getParent(), Instant.now());
        } catch (RuntimeException databaseFailure) {
            logger.error("Published file but could not mark it ARCHIVED; startup recovery will reconcile file={}",
                    candidate.fileName, databaseFailure);
        }
        logger.info("Archived file={} destination={} bytes={} durationMs={}",
                candidate.fileName, candidate.destination, candidate.copiedBytes,
                (System.nanoTime() - startedNanos) / 1_000_000);
        candidates.remove(candidate.source);
    }

    private void schedulePoll(Candidate candidate) {
        try {
            scheduler.schedule(() -> advance(candidate),
                    config.pollIntervalMillis(), TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException ignored) {
            // Shutting down.
        }
    }

    private void fail(Candidate candidate, Exception failure) {
        if (candidates.remove(candidate.source) == null) return;
        markFailed(candidate.fileName, failure);
        logger.error("Archive operation failed for file={} staging={}",
                candidate.source, candidate.staging, failure);
    }

    private void markFailed(String fileName, Exception failure) {
        try {
            repository.markFailed(fileName);
        } catch (RuntimeException updateFailure) {
            failure.addSuppressed(updateFailure);
        }
    }

    // ---------------------------------------------------------------- worker threads

    /**
     * Runs blocking I/O on a worker and applies its result back on the scheduler thread.
     * The task may read candidate fields written before dispatch, but must not write any.
     */
    private <T> void offload(Candidate candidate, Callable<T> work, Consumer<T> onSuccess) {
        try {
            workers.execute(() -> {
                try {
                    T result = work.call();
                    onSchedulerThread(() -> onSuccess.accept(result));
                } catch (Exception failure) {
                    onSchedulerThread(() -> fail(candidate, failure));
                }
            });
        } catch (RejectedExecutionException rejected) {
            fail(candidate, rejected);
        }
    }

    /** Publishes staging, or reports that the source moved and the quiet period must restart. */
    private Publication publishToArchive(Candidate candidate) throws IOException {
        StabilityDetector.Snapshot before = stabilityDetector.sample(candidate.source);
        if (before.size() != candidate.copiedBytes
                || !stabilityDetector.unchanged(candidate.stagedSnapshot, before)) {
            return Publication.RETRY;
        }

        fileMover.publish(candidate.staging, candidate.destination);
        try {
            StabilityDetector.Snapshot after = stabilityDetector.sample(candidate.source);
            if (!stabilityDetector.unchanged(before, after)) {
                fileMover.rollbackPublication(candidate.destination, candidate.staging);
                return Publication.RETRY;
            }
            Files.delete(candidate.source);
            return Publication.DONE;
        } catch (IOException failure) {
            if (Files.exists(candidate.destination) && !Files.exists(candidate.staging)) {
                try {
                    fileMover.rollbackPublication(candidate.destination, candidate.staging);
                } catch (IOException rollbackFailure) {
                    failure.addSuppressed(rollbackFailure);
                }
            }
            throw failure;
        }
    }

    // ----------------------------------------------------------------

    private void onSchedulerThread(Runnable action) {
        if (!running) return;
        try {
            scheduler.execute(() -> {
                if (running) action.run();
            });
        } catch (RejectedExecutionException ignored) {
            // Shutting down.
        }
    }

    private static void requireNoShrink(Path source, long sourceSize, long copiedBytes) throws IOException {
        if (sourceSize < copiedBytes) {
            throw new IOException("Append-only source shrank below what was already copied for " + source
                    + ": sourceSize=" + sourceSize + " copiedBytes=" + copiedBytes);
        }
    }

    private enum Publication { DONE, RETRY }

    /** Owned by the scheduler thread; plain fields are deliberate. See the class javadoc. */
    private static final class Candidate {
        private final Path source;
        private final String fileName;
        private final Path destination;
        private final Path staging;
        private long copiedBytes;
        private StabilityDetector.Snapshot stagedSnapshot;
        private long quietSinceNanos;

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
