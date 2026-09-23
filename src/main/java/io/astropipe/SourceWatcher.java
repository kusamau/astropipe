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
import java.time.LocalDateTime;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
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
    private final WatchServiceFactory watchServiceFactory;
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
        this(sourceDirectory, parser, stabilityDetector, pathResolver, fileMover, repository,
                workers, scheduler, reconciliationIntervalSeconds,
                directory -> directory.getFileSystem().newWatchService());
    }

    SourceWatcher(
            Path sourceDirectory,
            FileNameParser parser,
            StabilityDetector stabilityDetector,
            ArchivePathResolver pathResolver,
            FileMover fileMover,
            ArchiveRepository repository,
            ThreadPoolExecutor workers,
            ScheduledExecutorService scheduler,
            int reconciliationIntervalSeconds,
            WatchServiceFactory watchServiceFactory) {
        this.sourceDirectory = sourceDirectory;
        this.parser = parser;
        this.stabilityDetector = stabilityDetector;
        this.pathResolver = pathResolver;
        this.fileMover = fileMover;
        this.repository = repository;
        this.workers = workers;
        this.scheduler = scheduler;
        this.reconciliationIntervalSeconds = reconciliationIntervalSeconds;
        this.watchServiceFactory = Objects.requireNonNull(watchServiceFactory, "watchServiceFactory");
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
            candidate = watchServiceFactory.create(sourceDirectory);
            sourceDirectory.register(candidate,
                    StandardWatchEventKinds.ENTRY_CREATE,
                    StandardWatchEventKinds.ENTRY_MODIFY);
            watchService = candidate;
            return true;
        } catch (IOException e) {
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
        if (repository.isKnown(fileName)) {
            logger.debug("Ignoring known file={} detectedBy={}", fileName, reason);
            return;
        }
        if (!inFlight.add(normalized)) return;

        logger.info("Detected file={} detectedBy={}", fileName, reason);
        if (!stabilityDetector.enabled()) {
            logger.info("Stability check disabled; accepting file={}", fileName);
            submit(normalized, timestamp);
            return;
        }
        Candidate candidate = new Candidate(timestamp);
        candidates.put(normalized, candidate);
        scheduleStabilityCheck(normalized, candidate, 0);
    }

    private void scheduleStabilityCheck(Path path, Candidate candidate, long delayMillis) {
        scheduler.schedule(() -> checkStability(path, candidate), delayMillis, TimeUnit.MILLISECONDS);
    }

    private void checkStability(Path path, Candidate candidate) {
        if (!running.get()) {
            discard(path);
            return;
        }
        try {
            StabilityDetector.Snapshot current = stabilityDetector.sample(path);
            if (stabilityDetector.unchanged(candidate.previous, current)) {
                candidate.identicalSamples++;
            } else {
                candidate.identicalSamples = 1;
            }
            candidate.previous = current;

            if (candidate.identicalSamples >= stabilityDetector.consecutiveStableChecks()) {
                candidates.remove(path);
                logger.info("Stability check passed file={} samples={} size={}",
                        path.getFileName(), candidate.identicalSamples, current.size());
                submit(path, candidate.timestamp);
            } else {
                scheduleStabilityCheck(path, candidate, stabilityDetector.pollIntervalMillis());
            }
        } catch (IOException e) {
            logger.warn("Stability check failed file={}; it will be rediscovered by reconciliation", path, e);
            discard(path);
        }
    }

    private void submit(Path path, LocalDateTime timestamp) {
        try {
            workers.execute(new ArchiveTask(path, timestamp, pathResolver, fileMover, repository,
                    () -> inFlight.remove(path)));
        } catch (RuntimeException rejected) {
            inFlight.remove(path);
            logger.error("Could not submit archive work for file={}", path, rejected);
        }
    }

    private void discard(Path path) {
        candidates.remove(path);
        inFlight.remove(path);
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

    @FunctionalInterface
    interface WatchServiceFactory {
        WatchService create(Path sourceDirectory) throws IOException;
    }

    private static final class Candidate {
        private final LocalDateTime timestamp;
        private StabilityDetector.Snapshot previous;
        private int identicalSamples;

        private Candidate(LocalDateTime timestamp) {
            this.timestamp = timestamp;
        }
    }
}
