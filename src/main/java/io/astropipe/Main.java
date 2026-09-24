package io.astropipe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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
        logStorageLayout(sourceDirectory, archiveDirectory);

        ArchivePathResolver pathResolver = new ArchivePathResolver(archiveDirectory);
        FileMover fileMover = new FileMover();

        try (Database database = new Database(config.databaseUrl())) {
            ArchiveRepository repository = new ArchiveRepository(database);
            repository.initialize();
            recoverPreviousRun(sourceDirectory, pathResolver, fileMover, repository);
            archiveUntilStopped(config, sourceDirectory, pathResolver, fileMover, repository);
        }
    }

    /** Whether source and archive share a device decides how much copying really costs. */
    private static void logStorageLayout(Path sourceDirectory, Path archiveDirectory) throws IOException {
        logger.info("Source directory={} store={}", sourceDirectory, Files.getFileStore(sourceDirectory));
        logger.info("Archive directory={} store={}", archiveDirectory, Files.getFileStore(archiveDirectory));
    }

    /** Resolves whatever the last run left mid-flight before new work is accepted. */
    private static void recoverPreviousRun(
            Path sourceDirectory,
            ArchivePathResolver pathResolver,
            FileMover fileMover,
            ArchiveRepository repository) throws IOException {
        ArchiveReconciler.Result result =
                new ArchiveReconciler(sourceDirectory, pathResolver, fileMover, repository).reconcile();
        logger.info("Startup reconciliation: archived={} resumable={} failed={}",
                result.archived(), result.resumable(), result.failed());
    }

    /** Runs the pipeline until a shutdown signal arrives, then drains it. */
    private static void archiveUntilStopped(
            Config config,
            Path sourceDirectory,
            ArchivePathResolver pathResolver,
            FileMover fileMover,
            ArchiveRepository repository) throws InterruptedException {
        ThreadPoolExecutor workers = newWorkerPool(config);
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
                Thread.ofPlatform().name("reconciliation-", 1).factory());
        SourceWatcher watcher = new SourceWatcher(
                sourceDirectory, config, pathResolver, fileMover, repository, workers, scheduler);

        try {
            watcher.start();
            // Only now can close() do anything: before start() it short-circuits, so a
            // signal arriving here would have left a running watcher no hook could stop.
            stopOnShutdownSignal(watcher);
            logger.info("Monitoring source directory={}", sourceDirectory);
            watcher.awaitTermination();
        } finally {
            shutDown(watcher, scheduler, workers);
        }
    }

    private static ThreadPoolExecutor newWorkerPool(Config config) {
        return new ThreadPoolExecutor(
                config.workerPoolSize(),
                config.workerPoolSize(),
                0L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(config.workerQueueCapacity()),
                Thread.ofPlatform().name("archive-worker-", 1).factory(),
                new ThreadPoolExecutor.CallerRunsPolicy());
    }

    private static void shutDown(
            SourceWatcher watcher, ScheduledExecutorService scheduler, ThreadPoolExecutor workers) {
        logger.info("Shutdown started");
        watcher.close();
        scheduler.shutdownNow();
        drain(workers);
        logger.info("Shutdown complete");
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
}
