package io.astropipe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

/**
 * Simulates an archiving procedure following satellite's downlinks.
 * Covers only archiving operations.
 *

 * */
public final class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);
    private static final String DEFAULT_CONFIG_FILE = "config.json";

    private Main() { }

    /**
     * Loads the configuration file then runs until interrupted.
     */
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
        // Source directory Path
        Path sourceDirectory = directory(config.sourceDir(), "sourceDir", false);
        logger.info("Source directory={} store={}", sourceDirectory, Files.getFileStore(sourceDirectory));

        // Archive directory Path
        Path archiveDirectory = directory(config.archiveDir(), "archiveDir", true);
        logger.info("Archive directory={} store={}", archiveDirectory, Files.getFileStore(archiveDirectory));

        // Responsible to create the required Path for each file to be archived
        ArchivePathResolver pathResolver = new ArchivePathResolver(archiveDirectory);

        FileMover fileMover = new FileMover();

        try (Database database = new Database(config.databaseUrl())) {
            ArchiveRepository repository = new ArchiveRepository(database);
            repository.initialize();
            recoverPreviousRun(sourceDirectory, pathResolver, fileMover, repository);
            archiveUntilStopped(config, sourceDirectory, pathResolver, fileMover, repository);
        }
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

    /** Archives until a shutdown signal arrives or the watch ends, then stops the transfers. */
    private static void archiveUntilStopped(
            Config config,
            Path sourceDirectory,
            ArchivePathResolver pathResolver,
            FileMover fileMover,
            ArchiveRepository repository) throws InterruptedException {
        try (SourceWatcher watcher = new SourceWatcher(sourceDirectory, config, pathResolver, fileMover, repository)) {
            watcher.start();
            // Only once started: stopping a detector that has not started shuts its listing
            // scheduler, and start() would then fail to schedule the periodic listing.
            stopOnShutdownSignal(watcher);
            logger.info("Monitoring source directory={}", sourceDirectory);
            watcher.awaitTermination();
            logger.info("Shutdown started");
        }
        logger.info("Shutdown complete");
    }

    /** Unblocks the main thread on Ctrl-C or SIGTERM, then waits for it to finish shutting down. */
    private static void stopOnShutdownSignal(SourceWatcher watcher) {
        Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            watcher.requestStop();
            try {
                mainThread.join(TimeUnit.SECONDS.toMillis(30));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "astropipe-shutdown"));
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
