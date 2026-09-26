package io.astropipe;

import java.nio.file.Path;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * The watcher, the two thread pools it runs on, and their teardown.
 *
 * <p>This exists to own the order things stop in. The watcher stops accepting work first,
 * the scheduler then stops polling, and the workers finish what they are already holding —
 * which has to happen before the caller closes the database underneath them.
 *
 * <p>Constructing one starts nothing, so a caller can hold it before {@link #start()} runs
 * and still be sure {@link #close()} will take down whatever starting managed to build.
 */
final class Pipeline implements AutoCloseable {
    private final ThreadPoolExecutor workers;
    private final ScheduledExecutorService scheduler;
    private final SourceWatcher watcher;

    Pipeline(
            Config config,
            Path sourceDirectory,
            ArchivePathResolver pathResolver,
            FileMover fileMover,
            ArchiveRepository repository) {
        this.workers = new ThreadPoolExecutor(
                config.workerPoolSize(),
                config.workerPoolSize(),
                0L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(config.workerQueueCapacity()),
                Thread.ofPlatform().name("archive-worker-", 1).factory(),
                new ThreadPoolExecutor.CallerRunsPolicy());
        this.scheduler = Executors.newSingleThreadScheduledExecutor(
                Thread.ofPlatform().name("reconciliation-", 1).factory());
        this.watcher = new SourceWatcher(
                sourceDirectory, config, pathResolver, fileMover, repository, workers, scheduler);
    }

    /** Opens the watch and starts reporting. Whatever this half-builds, {@link #close()} takes down. */
    void start() {
        watcher.start();
    }

    /** Blocks until something asks the pipeline to stop. */
    void awaitStop() throws InterruptedException {
        watcher.awaitTermination();
    }

    /** Asks it to stop without waiting, which is all a signal handler should do. */
    void requestStop() {
        watcher.close();
    }

    @Override
    public void close() {
        // Reverse order of the resource list, and the language still closes the rest
        // if one of them throws on the way out.
        try (workers; scheduler; watcher) {
            // Nothing to do here: closing them in order is the whole job.
        }
    }
}
