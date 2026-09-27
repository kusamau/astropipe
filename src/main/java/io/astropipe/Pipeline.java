package io.astropipe;

import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * The watcher, the executor its transfers run on, and their teardown.
 *
 * <p>This exists to own the order things stop in: transfers are interrupted, detection stops,
 * and the transfers have ended — all before the caller closes the database underneath them.
 * Each image gets a virtual thread of its own for as long as it is in flight.
 *
 * <p>Constructing one starts nothing, so a caller can hold it before {@link #start()} runs
 * and still be sure {@link #close()} will take down whatever starting managed to build.
 */
final class Pipeline implements AutoCloseable {
    private final ExecutorService transfers;
    private final SourceWatcher watcher;

    Pipeline(
            Config config,
            Path sourceDirectory,
            ArchivePathResolver pathResolver,
            FileMover fileMover,
            ArchiveRepository repository) {
        this.transfers = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("transfer-", 1).factory());
        this.watcher = new SourceWatcher(sourceDirectory, config, pathResolver, fileMover, repository, transfers);
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
        // Interrupt rather than drain: an interrupted transfer leaves its image in progress
        // for startup reconciliation, so waiting out a long copy would buy nothing. The
        // resources then close in reverse order, and the language still closes the rest if
        // one of them throws on the way out.
        try (transfers; watcher) {
            transfers.shutdownNow();
        }
    }
}
