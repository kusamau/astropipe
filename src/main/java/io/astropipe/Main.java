package io.astropipe;

import io.astropipe.config.props.AppConfig;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@SpringBootApplication
@EnableConfigurationProperties(AppConfig.class)
public final class Main implements CommandLineRunner {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);
    private final AppConfig config;
    private final ArchiveRepository repository;
    private final AtomicBoolean shutdownStarted = new AtomicBoolean();
    private volatile SourceWatcher watcher;
    private volatile ScheduledExecutorService scheduler;
    private volatile ThreadPoolExecutor workers;

    public Main(AppConfig config, ArchiveRepository repository) {
        this.config = config;
        this.repository = repository;
    }

    public static void main(String[] args) {
        SpringApplication.run(Main.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        Path sourceDirectory = Path.of(config.sourceDir()).toAbsolutePath().normalize();
        Path archiveDirectory = Path.of(config.archiveDir()).toAbsolutePath().normalize();
        validateDirectory(sourceDirectory, "sourceDir");
        prepareArchiveDirectory(archiveDirectory, config.createArchiveDir());

        logger.info("File movement mode: INCREMENTAL_STAGING sourceStore={} archiveStore={}",
                Files.getFileStore(sourceDirectory), Files.getFileStore(archiveDirectory));

        FileNameParser parser = new FileNameParser();
        ArchivePathResolver resolver = new ArchivePathResolver(archiveDirectory);
        FileMover mover = new FileMover();

        repository.initialize();
        ArchiveRepository.ReconciliationResult reconciliation =
                repository.reconcileInProgress(sourceDirectory, parser, resolver);
        logger.info("Startup database reconciliation: archived={} resumable={} failed={}",
                reconciliation.archived(), reconciliation.resumable(), reconciliation.failed());

        workers = new ThreadPoolExecutor(
                config.workers().poolSize(),
                config.workers().poolSize(),
                0L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(config.workers().queueCapacity()),
                namedThreads("archive-worker-"),
                new ThreadPoolExecutor.CallerRunsPolicy());
        scheduler = Executors.newSingleThreadScheduledExecutor(namedThreads("reconciliation-"));
        StabilityDetector stabilityDetector = new StabilityDetector(config.stability());
        watcher = new SourceWatcher(
                sourceDirectory,
                parser,
                stabilityDetector,
                resolver,
                mover,
                repository,
                workers,
                scheduler,
                config.watch().reconciliationIntervalSeconds());

        try {
            watcher.start();
            logger.info("Monitoring source directory={} archive directory={}",
                    sourceDirectory, archiveDirectory);
            watcher.awaitTermination();
        } finally {
            shutdown();
        }
    }

    @PreDestroy
    void shutdown() {
        if (!shutdownStarted.compareAndSet(false, true)) return;
        logger.info("Shutdown started");

        SourceWatcher activeWatcher = watcher;
        if (activeWatcher != null) activeWatcher.close();

        ScheduledExecutorService activeScheduler = scheduler;
        if (activeScheduler != null) activeScheduler.shutdownNow();

        ThreadPoolExecutor activeWorkers = workers;
        if (activeWorkers != null) {
            activeWorkers.shutdown();
            try {
                if (!activeWorkers.awaitTermination(30, TimeUnit.SECONDS)) {
                    logger.warn("Worker drain timed out; interrupting remaining work");
                    activeWorkers.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                activeWorkers.shutdownNow();
            }
        }
        logger.info("Shutdown complete");
    }

    private static void validateDirectory(Path directory, String setting) throws IOException {
        if (!Files.exists(directory)) {
            throw new IOException(setting + " directory does not exist: " + directory);
        }
        if (!Files.isDirectory(directory)) {
            throw new IOException(setting + " is not a directory: " + directory);
        }
        if (!Files.isReadable(directory) || !Files.isWritable(directory)) {
            throw new IOException(setting + " directory must be readable and writable: " + directory);
        }
    }

    static void prepareArchiveDirectory(Path directory, boolean createIfMissing) throws IOException {
        if (!Files.exists(directory)) {
            if (!createIfMissing) {
                throw new IOException("archiveDir directory does not exist: " + directory);
            }
            try {
                Files.createDirectories(directory);
                logger.info("Created archiveDir directory={}", directory);
            } catch (IOException e) {
                throw new IOException("Could not create archiveDir directory: " + directory, e);
            }
        }
        validateDirectory(directory, "archiveDir");
    }

    private static ThreadFactory namedThreads(String prefix) {
        AtomicInteger sequence = new AtomicInteger();
        return runnable -> new Thread(runnable, prefix + sequence.incrementAndGet());
    }
}
