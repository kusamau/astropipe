package io.astropipe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** Entry point: load the JSON config, wire the pipeline, and run until interrupted. */
public final class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);
    private static final String DEFAULT_CONFIG_FILE = "config.json";

    private Main() { }

    public static void main(String[] args) {
        Path configFile = Path.of(args.length > 0 ? args[0] : DEFAULT_CONFIG_FILE);
        try {
            run(Config.load(configFile));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            logger.error("Astropipe stopped: {}", e.getMessage(), e);
            System.exit(1);
        }
    }

    private static void run(Config config) throws IOException, InterruptedException {
        Path sourceDirectory = directory(config.sourceDir(), "sourceDir", false);
        Path archiveDirectory = directory(config.archiveDir(), "archiveDir", true);
        logger.info("Source store={} archive store={}",
                Files.getFileStore(sourceDirectory), Files.getFileStore(archiveDirectory));

        ArchivePathResolver pathResolver = new ArchivePathResolver(archiveDirectory);
        FileMover fileMover = new FileMover();

        try (ArchiveRepository repository = new ArchiveRepository(config.databaseUrl())) {
            repository.initialize();
            ArchiveReconciler.Result reconciliation =
                    new ArchiveReconciler(sourceDirectory, pathResolver, fileMover, repository).reconcile();
            logger.info("Startup reconciliation: archived={} resumable={} failed={}",
                    reconciliation.archived(), reconciliation.resumable(), reconciliation.failed());

            ThreadPoolExecutor workers = new ThreadPoolExecutor(
                    config.workerPoolSize(),
                    config.workerPoolSize(),
                    0L,
                    TimeUnit.MILLISECONDS,
                    new ArrayBlockingQueue<>(config.workerQueueCapacity()),
                    namedThreads("archive-worker-"),
                    new ThreadPoolExecutor.CallerRunsPolicy());
            ScheduledExecutorService scheduler =
                    Executors.newSingleThreadScheduledExecutor(namedThreads("reconciliation-"));
            SourceWatcher watcher = new SourceWatcher(
                    sourceDirectory, config, pathResolver, fileMover, repository, workers, scheduler);

            stopOnShutdownSignal(watcher);
            try {
                watcher.start();
                logger.info("Monitoring source directory={} archive directory={}",
                        sourceDirectory, archiveDirectory);
                watcher.awaitTermination();
            } finally {
                logger.info("Shutdown started");
                watcher.close();
                scheduler.shutdownNow();
                drain(workers);
                logger.info("Shutdown complete");
            }
        }
    }

    /** Unblocks the main thread on Ctrl-C or SIGTERM, then waits for it to drain. */
    private static void stopOnShutdownSignal(SourceWatcher watcher) {
        Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            watcher.close();
            try {
                mainThread.join(TimeUnit.SECONDS.toMillis(30));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "astropipe-shutdown"));
    }

    private static void drain(ThreadPoolExecutor workers) {
        workers.shutdown();
        try {
            if (!workers.awaitTermination(30, TimeUnit.SECONDS)) {
                logger.warn("Worker drain timed out; interrupting remaining work");
                workers.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            workers.shutdownNow();
        }
    }

    static Path directory(String configured, String setting, boolean createIfMissing) throws IOException {
        Path directory = Path.of(configured).toAbsolutePath().normalize();
        if (createIfMissing && !Files.exists(directory)) {
            Files.createDirectories(directory);
            logger.info("Created {} directory={}", setting, directory);
        }
        if (!Files.isDirectory(directory)) {
            throw new IOException(setting + " is not an existing directory: " + directory);
        }
        if (!Files.isReadable(directory) || !Files.isWritable(directory)) {
            throw new IOException(setting + " directory must be readable and writable: " + directory);
        }
        return directory;
    }

    private static ThreadFactory namedThreads(String prefix) {
        AtomicInteger sequence = new AtomicInteger();
        return runnable -> new Thread(runnable, prefix + sequence.incrementAndGet());
    }
}
