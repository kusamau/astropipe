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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * Notices regular files in the source directory and reports each one, together with
 * how it was found, to a sink. It does not interpret names: deciding what is a
 * satellite image is the pipeline's job.
 *
 * <p>Each report carries a {@link Detection} saying how the file was found, which the
 * pipeline logs with every file it takes on or refuses.
 *
 * <p>{@code WatchService} supplies low-latency {@code ENTRY_CREATE} events. Modifications are
 * not watched: once a file has been reported the pipeline follows it by polling, so an event
 * would add nothing. {@code WatchService} cannot recover events missed while the process was
 * stopped and may overflow, so a startup listing and a periodic listing run alongside
 * it, and an explicit {@code OVERFLOW} schedules an immediate listing. Where
 * {@code WatchService} is unavailable, detection falls back to listing alone.
 *
 * <p>The watch loop and the listings run on threads this class creates and ends on {@link #close()}.
 */
final class SourceDetector implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(SourceDetector.class);

    private final Path sourceDirectory;
    /** Runs the periodic and overflow listings, one at a time. */
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
            Thread.ofPlatform().name("reconciliation-", 1).factory());
    private final int listingIntervalSeconds;
    private final BiConsumer<Path, Detection> onDetected;
    private final CountDownLatch stopped = new CountDownLatch(1);

    private volatile boolean running;
    /** Guarded by {@code this}; separate from {@link #running} so cleanup still runs
     *  when the watch loop has already stopped itself. */
    private boolean closed;

    private WatchService watchService;
    private Thread watcherThread;
    private final EventKindHandler eventKindHandler;

    SourceDetector(Path sourceDirectory, int listingIntervalSeconds, BiConsumer<Path, Detection> onDetected) {
        // Normalized once: every path reported is this directory resolved against a plain file name.
        this.sourceDirectory = sourceDirectory.toAbsolutePath().normalize();
        this.listingIntervalSeconds = listingIntervalSeconds;
        this.onDetected = onDetected;
        this.eventKindHandler = new EventKindHandler(this.sourceDirectory, onDetected, scheduler, this::sweep);
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
        scheduler.shutdownNow();
        stopped.countDown();
    }

    private boolean initializeWatchService() {
        WatchService candidate = null;
        try {
            candidate = sourceDirectory.getFileSystem().newWatchService();
            sourceDirectory.register(candidate, StandardWatchEventKinds.ENTRY_CREATE);
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
                    eventKindHandler.handle(event);
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

    private void sweep(Detection detection) {
        if (!running) return;
        try (var paths = Files.list(sourceDirectory)) {
            // A listing still running when the detector closes stops at its next entry.
            paths.filter(Files::isRegularFile).takeWhile(path -> running).forEach(path -> onDetected.accept(path, detection));
        } catch (IOException e) {
            logger.error("Could not perform {} of source directory={}", detection, sourceDirectory, e);
        }
    }

    /** How a file came to be reported. */
    enum Detection {
        STARTUP_LISTING("startup listing"),
        FILE_CREATED("watch ENTRY_CREATE"),
        PERIODIC_LISTING("periodic listing"),
        OVERFLOW_LISTING("overflow listing");

        private final String label;

        Detection(String label) {
            this.label = label;
        }

        @Override
        public String toString() {
            return label;
        }
    }
}
