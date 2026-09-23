package io.astropipe;

import io.astropipe.config.props.StabilityProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ArchiverIT {
    @TempDir
    Path temporaryDirectory;

    private SourceWatcher watcher;
    private ThreadPoolExecutor workers;
    private ScheduledExecutorService scheduler;

    @AfterEach
    void tearDown() throws Exception {
        if (watcher != null) watcher.close();
        if (scheduler != null) scheduler.shutdownNow();
        if (workers != null) {
            workers.shutdown();
            workers.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void archivesStartupAndWatchEventsAndIgnoresMalformedFiles() throws Exception {
        Fixture fixture = startFixture(false, 10, 2,
                "SAT_IMG_20211231235959999.raw");
        Path startupDestination = fixture.archive.resolve(
                "2021/12/31/SAT_IMG_20211231235959999.raw");
        await(() -> Files.exists(startupDestination), Duration.ofSeconds(5));

        Path watched = fixture.source.resolve("SAT_IMG_20220101000000000.raw");
        Files.writeString(watched, "watch event");
        Path watchedDestination = fixture.archive.resolve(
                "2022/01/01/SAT_IMG_20220101000000000.raw");
        await(() -> Files.exists(watchedDestination), Duration.ofSeconds(5));

        Path malformed = fixture.source.resolve("not-an-image.raw");
        Files.writeString(malformed, "ignored");
        TimeUnit.MILLISECONDS.sleep(150);
        assertTrue(Files.exists(malformed));
    }

    @Test
    void doesNotMoveFileWhileUnsafeGeneratorIsStillWriting() throws Exception {
        Fixture fixture = startFixture(true, 100, 2, null);
        String fileName = "SAT_IMG_20240229120000000.raw";
        Path source = fixture.source.resolve(fileName);
        Path destination = fixture.archive.resolve("2024/02/29").resolve(fileName);
        long generatedSize = 10L * 64 * 1024;
        AtomicReference<Throwable> writerFailure = new AtomicReference<>();

        Thread writer = new Thread(() -> {
            try {
                writeInChunks(source, generatedSize);
            } catch (Throwable e) {
                writerFailure.set(e);
            }
        });
        writer.start();

        TimeUnit.MILLISECONDS.sleep(350);
        assertTrue(Files.exists(source), "changing source must remain in place");
        assertFalse(Files.exists(destination));
        writer.join();
        assertNull(writerFailure.get());
        await(() -> Files.exists(destination), Duration.ofSeconds(5));
        assertTrue(Files.size(destination) == generatedSize);
    }

    @Test
    void fallsBackToReconciliationWhenWatchServiceIsUnavailable() throws Exception {
        Fixture fixture = startFixtureWithUnavailableWatchService(
                "SAT_IMG_20231231235959999.raw");
        Path startupDestination = fixture.archive.resolve(
                "2023/12/31/SAT_IMG_20231231235959999.raw");
        await(() -> Files.exists(startupDestination), Duration.ofSeconds(5));

        Path source = fixture.source.resolve("SAT_IMG_20240101000000000.raw");
        Files.writeString(source, "reconciliation fallback");
        Path destination = fixture.archive.resolve(
                "2024/01/01/SAT_IMG_20240101000000000.raw");
        await(() -> Files.exists(destination), Duration.ofSeconds(5));
    }

    private static void writeInChunks(Path path, long size) throws IOException, InterruptedException {
        byte[] buffer = new byte[64 * 1024];
        try (OutputStream output = Files.newOutputStream(path,
                StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            long remaining = size;
            while (remaining > 0) {
                int count = (int) Math.min(remaining, buffer.length);
                output.write(buffer, 0, count);
                remaining -= count;
                if (remaining > 0) TimeUnit.MILLISECONDS.sleep(100);
            }
        }
    }

    private Fixture startFixture(
            boolean stabilityEnabled,
            int pollMillis,
            int stableChecks,
            String startupFile) throws Exception {
        Path source = Files.createDirectory(temporaryDirectory.resolve("source"));
        Path archive = Files.createDirectory(temporaryDirectory.resolve("archive"));
        if (startupFile != null) Files.writeString(source.resolve(startupFile), "startup");

        var dataSource = new DriverManagerDataSource(
                "jdbc:sqlite:" + temporaryDirectory.resolve("archive.db"));
        ArchiveRepository repository = new ArchiveRepository(new JdbcTemplate(dataSource));
        repository.initialize();
        workers = new ThreadPoolExecutor(2, 2, 0, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(100), new ThreadPoolExecutor.CallerRunsPolicy());
        scheduler = Executors.newSingleThreadScheduledExecutor();
        watcher = new SourceWatcher(
                source,
                new FileNameParser(),
                new StabilityDetector(new StabilityProperties(stabilityEnabled, pollMillis, stableChecks)),
                new ArchivePathResolver(archive),
                new FileMover(false),
                repository,
                workers,
                scheduler,
                1);
        watcher.start();
        return new Fixture(source, archive);
    }

    private Fixture startFixtureWithUnavailableWatchService(String startupFile) throws Exception {
        Path source = Files.createDirectory(temporaryDirectory.resolve("source"));
        Path archive = Files.createDirectory(temporaryDirectory.resolve("archive"));
        Files.writeString(source.resolve(startupFile), "startup");

        var dataSource = new DriverManagerDataSource(
                "jdbc:sqlite:" + temporaryDirectory.resolve("archive.db"));
        ArchiveRepository repository = new ArchiveRepository(new JdbcTemplate(dataSource));
        repository.initialize();
        workers = new ThreadPoolExecutor(2, 2, 0, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(100), new ThreadPoolExecutor.CallerRunsPolicy());
        scheduler = Executors.newSingleThreadScheduledExecutor();
        watcher = new SourceWatcher(
                source,
                new FileNameParser(),
                new StabilityDetector(new StabilityProperties(false, 100, 2)),
                new ArchivePathResolver(archive),
                new FileMover(false),
                repository,
                workers,
                scheduler,
                1,
                ignored -> { throw new IOException("WatchService intentionally unavailable"); });
        watcher.start();
        return new Fixture(source, archive);
    }

    private static void await(CheckedCondition condition, Duration timeout) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.evaluate()) return;
            TimeUnit.MILLISECONDS.sleep(20);
        }
        assertTrue(condition.evaluate(), "condition was not met within " + timeout);
    }

    private interface CheckedCondition {
        boolean evaluate() throws Exception;
    }

    private record Fixture(Path source, Path archive) { }
}
