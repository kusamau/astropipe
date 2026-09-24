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
    private final BiConsumer<Path, String> onDetected;
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
            BiConsumer<Path, String> onDetected) {
        this.sourceDirectory = sourceDirectory;
        this.scheduler = scheduler;
        this.listingIntervalSeconds = listingIntervalSeconds;
        this.onDetected = onDetected;
    }

    /** Reports everything already present, then keeps reporting until closed. */
    void start() {
        boolean watchServiceActive = initializeWatchService();
        running = true;

        sweep("startup");
        scheduler.scheduleAtFixedRate(
                () -> sweep("periodic reconciliation"),
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
                    if (event.kind() == StandardWatchEventKinds.OVERFLOW) {
                        logger.warn("WatchService overflow detected; scheduling an immediate listing");
                        scheduler.execute(() -> sweep("overflow reconciliation"));
                        continue;
                    }
                    Path context = (Path) event.context();
                    report(sourceDirectory.resolve(context), "watch " + event.kind().name());
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

    private void sweep(String reason) {
        if (!running) return;
        try (var paths = Files.list(sourceDirectory)) {
            paths.filter(Files::isRegularFile).forEach(path -> report(path, reason));
        } catch (IOException e) {
            logger.error("Could not perform {} of source directory={}", reason, sourceDirectory, e);
        }
    }

    private void report(Path path, String reason) {
        onDetected.accept(path.toAbsolutePath().normalize(), reason);
    }
}
