package io.astropipe;

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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * Notices regular files in the source directory and reports each one, together with
 * how it was found, to a sink. It does not interpret names: deciding what is a
 * satellite image is the pipeline's job.
 *
 * <p>Each report carries a {@link Detection} saying how the file was found, which lets
 * the pipeline tell a first sighting apart from a re-scan of everything.
 *
 * <p>{@code WatchService} supplies low-latency {@code ENTRY_CREATE} and
 * {@code ENTRY_MODIFY} events. It cannot recover events missed while the process was
 * stopped and may overflow, so a startup listing and a periodic listing run alongside
 * it, and an explicit {@code OVERFLOW} schedules an immediate listing. Where
 * {@code WatchService} is unavailable, detection falls back to listing alone.
 */
final class SourceDetector implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(SourceDetector.class);

    private final Path sourceDirectory;
    private final ScheduledExecutorService scheduler;
    private final int listingIntervalSeconds;
    private final BiConsumer<Path, Detection> onDetected;
    private final CountDownLatch stopped = new CountDownLatch(1);

    private volatile boolean running;
    /** Guarded by {@code this}; separate from {@link #running} so cleanup still runs
     *  when the watch loop has already stopped itself. */
    private boolean closed;

    private WatchService watchService;
    private Thread watcherThread;

    SourceDetector(
            Path sourceDirectory,
            ScheduledExecutorService scheduler,
            int listingIntervalSeconds,
            BiConsumer<Path, Detection> onDetected) {
        this.sourceDirectory = sourceDirectory;
        this.scheduler = scheduler;
        this.listingIntervalSeconds = listingIntervalSeconds;
        this.onDetected = onDetected;
    }

    /** Reports everything already present, then keeps reporting until closed. */
    void start() {
        boolean watchServiceActive = initializeWatchService();
        running = true;

        sweep(Detection.STARTUP_LISTING);
        scheduler.scheduleAtFixedRate(
                () -> sweep(Detection.PERIODIC_LISTING),
                listingIntervalSeconds,
                listingIntervalSeconds,
                TimeUnit.SECONDS);

        if (watchServiceActive) {
            watcherThread = new Thread(this::watchLoop, "source-watcher");
            watcherThread.start();
        }
    }

    void awaitTermination() throws InterruptedException {
        stopped.await();
    }

    @Override
    public synchronized void close() {
        if (closed) return;
        closed = true;
        running = false;
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
                    + "continuing with listing-only detection", sourceDirectory, e);
            return false;
        }
    }

    private void watchLoop() {
        try {
            while (running) {
                WatchKey key = watchService.take();
                for (WatchEvent<?> event : key.pollEvents()) {
                    onWatchEvent(event);
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
            running = false;
            stopped.countDown();
        }
    }

    /**
     * {@link StandardWatchEventKinds}' constants are plain {@code WatchEvent.Kind} instances,
     * not an enum, so a guard stands in for the case label a real enum switch would allow.
     */
    private void onWatchEvent(WatchEvent<?> event) {
        switch (event.kind()) {
            case WatchEvent.Kind<?> kind when kind == StandardWatchEventKinds.OVERFLOW -> onOverflow();
            case WatchEvent.Kind<?> kind when kind == StandardWatchEventKinds.ENTRY_CREATE -> onEntryCreated(event);
            case WatchEvent.Kind<?> kind when kind == StandardWatchEventKinds.ENTRY_MODIFY -> onEntryModified(event);
            default -> logger.warn("Ignoring unexpected watch event kind={}", event.kind());
        }
    }

    private void onOverflow() {
        logger.warn("WatchService overflow detected; scheduling an immediate listing");
        scheduler.execute(() -> sweep(Detection.OVERFLOW_LISTING));
    }

    private void onEntryCreated(WatchEvent<?> event) {
        report(contextPath(event), Detection.FILE_CREATED);
    }

    private void onEntryModified(WatchEvent<?> event) {
        report(contextPath(event), Detection.FILE_MODIFIED);
    }

    private Path contextPath(WatchEvent<?> event) {
        return sourceDirectory.resolve((Path) event.context());
    }

    private void sweep(Detection detection) {
        if (!running) return;
        try (var paths = Files.list(sourceDirectory)) {
            paths.filter(Files::isRegularFile).forEach(path -> report(path, detection));
        } catch (IOException e) {
            logger.error("Could not perform {} of source directory={}", detection, sourceDirectory, e);
        }
    }

    private void report(Path path, Detection detection) {
        onDetected.accept(path.toAbsolutePath().normalize(), detection);
    }

    /**
     * How a file came to be reported.
     *
     * <p>{@link #firstSighting()} separates the two that can plausibly be the first time
     * this run has seen a given file from the listings that re-report everything already
     * there. Only the former deserve to be noisy about a file the pipeline will reject.
     */
    enum Detection {
        STARTUP_LISTING("startup listing", true),
        FILE_CREATED("watch ENTRY_CREATE", true),
        FILE_MODIFIED("watch ENTRY_MODIFY", true),
        PERIODIC_LISTING("periodic listing", false),
        OVERFLOW_LISTING("overflow listing", false);

        private final String label;
        private final boolean firstSighting;

        Detection(String label, boolean firstSighting) {
            this.label = label;
            this.firstSighting = firstSighting;
        }

        boolean firstSighting() {
            return firstSighting;
        }

        @Override
        public String toString() {
            return label;
        }
    }
}
