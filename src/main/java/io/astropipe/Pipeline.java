package io.astropipe;

import java.nio.file.Path;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * A running pipeline: the watcher, the two thread pools it runs on, and their teardown.
 *
 * <p>This exists to own the order things stop in. The watcher stops accepting work first,
 * the scheduler then stops polling, and the workers finish what they are already holding —
 * which has to happen before the caller closes the database underneath them.
 */
final class Pipeline implements AutoCloseable {
    private final SourceWatcher watcher;
    private final ScheduledExecutorService scheduler;
    private final ThreadPoolExecutor workers;

    private Pipeline(SourceWatcher watcher, ScheduledExecutorService scheduler, ThreadPoolExecutor workers) {
        this.watcher = watcher;
        this.scheduler = scheduler;
        this.workers = workers;
    }

    /** Builds the pipeline and starts it watching. */
    static Pipeline start(
            Config config,
            Path sourceDirectory,
            ArchivePathResolver pathResolver,
            FileMover fileMover,
            ArchiveRepository repository) {
        ThreadPoolExecutor workers = new ThreadPoolExecutor(
                config.workerPoolSize(),
                config.workerPoolSize(),
                0L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(config.workerQueueCapacity()),
                Thread.ofPlatform().name("archive-worker-", 1).factory(),
                new ThreadPoolExecutor.CallerRunsPolicy());
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
                Thread.ofPlatform().name("reconciliation-", 1).factory());
        try {
            SourceWatcher watcher = new SourceWatcher(
                    sourceDirectory, config, pathResolver, fileMover, repository, workers, scheduler);
            watcher.start();
            return new Pipeline(watcher, scheduler, workers);
        } catch (RuntimeException | Error failedToStart) {
            // The caller never received a Pipeline, so nothing else will close these.
            try (workers; scheduler) {
                throw failedToStart;
            }
        }
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
