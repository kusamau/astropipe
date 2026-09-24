package io.astropipe;

import io.astropipe.exception.InvalidSatelliteImageNameException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.ClosedWatchServiceException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public final class SourceWatcher implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(SourceWatcher.class);

    private final Path sourceDirectory;
    private final FileNameParser parser;
    private final StabilityDetector stabilityDetector;
    private final ArchivePathResolver pathResolver;
    private final FileMover fileMover;
    private final ArchiveRepository repository;
    private final ThreadPoolExecutor workers;
    private final ScheduledExecutorService scheduler;
    private final int reconciliationIntervalSeconds;
    private final Set<Path> inFlight = ConcurrentHashMap.newKeySet();
    private final ConcurrentHashMap<Path, Candidate> candidates = new ConcurrentHashMap<>();
    private final AtomicBoolean running = new AtomicBoolean();
    private final CountDownLatch stopped = new CountDownLatch(1);

    private WatchService watchService;
    private Thread watcherThread;

    public SourceWatcher(
            Path sourceDirectory,
            FileNameParser parser,
            StabilityDetector stabilityDetector,
            ArchivePathResolver pathResolver,
            FileMover fileMover,
            ArchiveRepository repository,
            ThreadPoolExecutor workers,
            ScheduledExecutorService scheduler,
            int reconciliationIntervalSeconds) {
        this.sourceDirectory = sourceDirectory;
        this.parser = parser;
        this.stabilityDetector = stabilityDetector;
        this.pathResolver = pathResolver;
        this.fileMover = fileMover;
        this.repository = repository;
        this.workers = workers;
        this.scheduler = scheduler;
        this.reconciliationIntervalSeconds = reconciliationIntervalSeconds;
    }

    public void start() throws IOException {
        boolean watchServiceActive = initializeWatchService();
        running.set(true);

        sweep("startup");
        scheduler.scheduleAtFixedRate(
                () -> sweep("periodic reconciliation"),
                reconciliationIntervalSeconds,
                reconciliationIntervalSeconds,
                TimeUnit.SECONDS);

        if (watchServiceActive) {
            watcherThread = new Thread(this::watchLoop, "source-watcher");
            watcherThread.start();
        }
    }

    public void awaitTermination() throws InterruptedException {
        stopped.await();
    }

    private boolean initializeWatchService() {
        WatchService candidate = null;
        try {
            candidate = sourceDirectory.getFileSystem().newWatchService();
            sourceDirectory.register(candidate,
                    StandardWatchEventKinds.ENTRY_CREATE,
                    StandardWatchEventKinds.ENTRY_MODIFY);
            watchService = candidate;
            return true;
        } catch (IOException | UnsupportedOperationException e) {
            if (candidate != null) {
                try {
                    candidate.close();
                } catch (IOException closeFailure) {
                    e.addSuppressed(closeFailure);
                }
            }
            watchService = null;
            logger.warn("WatchService unavailable for source directory={}; "
                    + "continuing with reconciliation-only detection", sourceDirectory, e);
            return false;
        }
    }

    private void watchLoop() {
        try {
            while (running.get()) {
                WatchKey key = watchService.take();
                for (WatchEvent<?> event : key.pollEvents()) {
                    if (event.kind() == StandardWatchEventKinds.OVERFLOW) {
                        logger.warn("WatchService overflow detected; scheduling an immediate reconciliation sweep");
                        scheduler.execute(() -> sweep("overflow reconciliation"));
                        continue;
                    }
                    @SuppressWarnings("unchecked")
                    WatchEvent<Path> pathEvent = (WatchEvent<Path>) event;
                    detect(sourceDirectory.resolve(pathEvent.context()), "watch " + event.kind().name());
                }
                if (!key.reset()) {
                    logger.error("Source directory watch key is no longer valid: {}", sourceDirectory);
                    break;
                }
            }
        } catch (ClosedWatchServiceException ignored) {
            // Normal shutdown.
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            running.set(false);
            stopped.countDown();
        }
    }

    private void sweep(String reason) {
        if (!running.get()) return;
        try (var paths = Files.list(sourceDirectory)) {
            paths.filter(Files::isRegularFile).forEach(path -> detect(path, reason));
        } catch (IOException e) {
            logger.error("Could not perform {} of source directory={}", reason, sourceDirectory, e);
        }
    }

    private void detect(Path path, String reason) {
        Path normalized = path.toAbsolutePath().normalize();
        String fileName = normalized.getFileName().toString();
        LocalDateTime timestamp;
        try {
            timestamp = parser.parse(normalized);
        } catch (InvalidSatelliteImageNameException invalid) {
            logger.warn("Ignoring malformed source entry={} detectedBy={} reason={}",
                    normalized, reason, invalid.getMessage());
            return;
        }
        Candidate existing = candidates.get(normalized);
        if (existing != null) {
            scheduleCheck(existing, 0);
            return;
        }
        if (!inFlight.add(normalized)) return;

        try {
            ArchiveRepository.CopyRecord record = repository.beginOrResume(fileName, Instant.now());
            if (record.terminal()) {
                logger.debug("Ignoring terminal file={} status={} detectedBy={}",
                        fileName, record.status(), reason);
                inFlight.remove(normalized);
                return;
            }

            Path destination = pathResolver.resolve(fileName, timestamp);
            Path staging = fileMover.stagingPath(destination);
            long checkpoint = fileMover.normalizeStaging(staging, record.copiedBytes());
            if (checkpoint != record.copiedBytes()) repository.resetCheckpoint(fileName);

            StabilityDetector.Snapshot snapshot = stabilityDetector.sample(normalized);
            if (snapshot.size() < checkpoint) {
                throw new IOException("Append-only source shrank below checkpoint for " + normalized
                        + ": sourceSize=" + snapshot.size() + " checkpoint=" + checkpoint);
            }

            Candidate candidate = new Candidate(normalized, fileName, destination, staging, checkpoint);
            candidates.put(normalized, candidate);
            logger.info("Detected file={} detectedBy={} checkpoint={}", fileName, reason, checkpoint);
            if (snapshot.size() > checkpoint) {
                scheduleCopy(candidate);
            } else {
                repository.markStaging(fileName, checkpoint);
                candidate.stagedSnapshot = snapshot;
                candidate.quietSinceNanos = System.nanoTime();
                scheduleCheck(candidate, stabilityDetector.pollIntervalMillis());
            }
        } catch (Exception failure) {
            try {
                ArchiveRepository.CopyRecord record = repository.beginOrResume(fileName, Instant.now());
                if (!record.terminal()) repository.markFailed(fileName);
            } catch (RuntimeException updateFailure) {
                failure.addSuppressed(updateFailure);
            }
            inFlight.remove(normalized);
            logger.error("Could not initialize incremental copy for file={}", normalized, failure);
        }
    }

    private void scheduleCopy(Candidate candidate) {
        if (!running.get() || !candidate.workScheduled.compareAndSet(false, true)) return;
        try {
            workers.execute(new ArchiveTask(() -> copyAvailable(candidate)));
        } catch (RuntimeException rejected) {
            candidate.workScheduled.set(false);
            fail(candidate, rejected);
        }
    }

    private void copyAvailable(Candidate candidate) {
        StabilityDetector.Snapshot afterCopy = null;
        boolean copied = false;
        try {
            repository.markCopying(candidate.fileName);
            long normalizedCheckpoint = fileMover.normalizeStaging(candidate.staging, candidate.checkpoint);
            if (normalizedCheckpoint != candidate.checkpoint) {
                repository.resetCheckpoint(candidate.fileName);
                candidate.checkpoint = 0;
            }

            StabilityDetector.Snapshot beforeCopy = stabilityDetector.sample(candidate.source);
            long checkpoint = candidate.checkpoint;
            if (beforeCopy.size() < checkpoint) {
                throw new IOException("Append-only source shrank below checkpoint for " + candidate.source
                        + ": sourceSize=" + beforeCopy.size() + " checkpoint=" + checkpoint);
            }
            if (checkpoint > 0) {
                long resumeOffset = Math.max(0, checkpoint - stabilityDetector.resumeOverlapBytes());
                fileMover.validateOverlap(candidate.source, candidate.staging, resumeOffset, checkpoint);
            }

            long copiedBytes = fileMover.appendRange(
                    candidate.source, candidate.staging, checkpoint, beforeCopy.size());
            repository.markStaging(candidate.fileName, copiedBytes);
            candidate.checkpoint = copiedBytes;
            afterCopy = stabilityDetector.sample(candidate.source);
            candidate.stagedSnapshot = afterCopy;
            candidate.quietSinceNanos = System.nanoTime();
            copied = true;
            logger.info("Staged file={} copiedBytes={} appendedBytes={}",
                    candidate.fileName, copiedBytes, copiedBytes - checkpoint);
        } catch (Exception failure) {
            fail(candidate, failure);
        } finally {
            candidate.workScheduled.set(false);
        }

        if (!copied || !running.get() || !candidates.containsKey(candidate.source)) return;
        if (afterCopy.size() > candidate.checkpoint) {
            scheduleCopy(candidate);
        } else {
            scheduleCheck(candidate, stabilityDetector.pollIntervalMillis());
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
            scheduleCheck(candidate, stabilityDetector.pollIntervalMillis());
            return;
        }
        try {
            StabilityDetector.Snapshot current = stabilityDetector.sample(candidate.source);
            if (current.size() < candidate.checkpoint) {
                throw new IOException("Append-only source shrank below checkpoint for " + candidate.source
                        + ": sourceSize=" + current.size() + " checkpoint=" + candidate.checkpoint);
            }
            if (current.size() > candidate.checkpoint) {
                scheduleCopy(candidate);
                return;
            }

            long now = System.nanoTime();
            if (!stabilityDetector.unchanged(candidate.stagedSnapshot, current)) {
                candidate.stagedSnapshot = current;
                candidate.quietSinceNanos = now;
            }
            if (stabilityDetector.quietFor(candidate.quietSinceNanos, now)) {
                schedulePublication(candidate);
            } else {
                scheduleCheck(candidate, stabilityDetector.pollIntervalMillis());
            }
        } catch (Exception failure) {
            fail(candidate, failure);
        }
    }

    private void schedulePublication(Candidate candidate) {
        if (!running.get() || !candidate.workScheduled.compareAndSet(false, true)) return;
        try {
            workers.execute(new ArchiveTask(() -> publish(candidate)));
        } catch (RuntimeException rejected) {
            candidate.workScheduled.set(false);
            fail(candidate, rejected);
        }
    }

    private void publish(Candidate candidate) {
        boolean published = false;
        boolean retry = false;
        long started = System.nanoTime();
        try {
            StabilityDetector.Snapshot beforePublish = stabilityDetector.sample(candidate.source);
            if (beforePublish.size() != candidate.checkpoint
                    || !stabilityDetector.unchanged(candidate.stagedSnapshot, beforePublish)) {
                candidate.stagedSnapshot = beforePublish;
                candidate.quietSinceNanos = System.nanoTime();
                retry = true;
                return;
            }

            fileMover.publish(candidate.staging, candidate.destination);
            published = true;
            StabilityDetector.Snapshot afterPublish = stabilityDetector.sample(candidate.source);
            if (!stabilityDetector.unchanged(beforePublish, afterPublish)) {
                fileMover.rollbackPublication(candidate.destination, candidate.staging);
                published = false;
                candidate.stagedSnapshot = afterPublish;
                candidate.quietSinceNanos = System.nanoTime();
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
            long durationMillis = (System.nanoTime() - started) / 1_000_000;
            logger.info("Archived file={} destination={} bytes={} durationMs={}",
                    candidate.fileName, candidate.destination, candidate.checkpoint, durationMillis);
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
                if (candidate.stagedSnapshot.size() > candidate.checkpoint) {
                    scheduleCopy(candidate);
                } else {
                    scheduleCheck(candidate, stabilityDetector.pollIntervalMillis());
                }
            }
        }
    }

    private void fail(Candidate candidate, Exception failure) {
        if (!candidates.containsKey(candidate.source)) return;
        try {
            repository.markFailed(candidate.fileName);
        } catch (RuntimeException updateFailure) {
            failure.addSuppressed(updateFailure);
        }
        logger.error("Incremental archive operation failed for file={} staging={}",
                candidate.source, candidate.staging, failure);
        discard(candidate);
    }

    private void discard(Candidate candidate) {
        candidates.remove(candidate.source, candidate);
        inFlight.remove(candidate.source);
    }

    @Override
    public void close() {
        if (!running.getAndSet(false)) return;
        WatchService activeWatchService = watchService;
        if (activeWatchService != null) {
            try {
                activeWatchService.close();
            } catch (IOException e) {
                logger.warn("Could not close source WatchService cleanly", e);
            }
        }
        if (watcherThread != null) watcherThread.interrupt();
        stopped.countDown();
    }

    private static final class Candidate {
        private final Path source;
        private final String fileName;
        private final Path destination;
        private final Path staging;
        private final AtomicBoolean workScheduled = new AtomicBoolean();
        private final AtomicBoolean checkScheduled = new AtomicBoolean();
        private volatile long checkpoint;
        private volatile StabilityDetector.Snapshot stagedSnapshot;
        private volatile long quietSinceNanos;

        private Candidate(Path source, String fileName, Path destination, Path staging, long checkpoint) {
            this.source = source;
            this.fileName = fileName;
            this.destination = destination;
            this.staging = staging;
            this.checkpoint = checkpoint;
        }
    }
}
