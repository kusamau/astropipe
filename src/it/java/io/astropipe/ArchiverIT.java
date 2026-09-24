package io.astropipe;

import io.astropipe.config.props.StabilityProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
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
    private FileSystem sourceFileSystem;

    @AfterEach
    void tearDown() throws Exception {
        if (watcher != null) watcher.close();
        if (scheduler != null) scheduler.shutdownNow();
        if (workers != null) {
            workers.shutdown();
            workers.awaitTermination(5, TimeUnit.SECONDS);
        }
        if (sourceFileSystem != null) sourceFileSystem.close();
    }

    @Test
    void archivesStartupAndWatchEventsAndIgnoresMalformedFiles() throws Exception {
        Fixture fixture = startFixture(10, 50, 4,
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
        Fixture fixture = startFixture(50, 250, 64 * 1024, null);
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
        assertTrue(Files.exists(destination.resolveSibling(fileName + ".tmp")),
                "a growing source should be copied into archive-side staging");
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

    @Test
    void failsWithoutAppendingWhenTheResumeOverlapDiffers() throws Exception {
        Fixture fixture = startFixture(25, 2_000, 4, null);
        String fileName = "SAT_IMG_20240301120000000.raw";
        Path source = Files.writeString(fixture.source.resolve(fileName), "abcdefgh");
        Path destination = fixture.archive.resolve("2024/03/01").resolve(fileName);
        Path staging = destination.resolveSibling(fileName + ".tmp");
        await(() -> Files.exists(staging) && Files.size(staging) == 8, Duration.ofSeconds(5));

        Files.writeString(staging, "abcdefXh", StandardOpenOption.TRUNCATE_EXISTING);
        Files.writeString(source, "ijkl", StandardOpenOption.APPEND);

        await(() -> fixture.repository.beginOrResume(fileName, Instant.now()).status()
                == ArchiveRepository.Status.FAILED, Duration.ofSeconds(5));
        assertTrue(Files.exists(source));
        assertTrue(Files.exists(staging));
        assertFalse(Files.exists(destination));
        assertTrue(Files.readString(staging).equals("abcdefXh"));
    }

    @Test
    void resumesFromCheckpointAndDropsUncheckpointedTail() throws Exception {
        Path sourceDirectory = Files.createDirectory(temporaryDirectory.resolve("source"));
        Path archive = Files.createDirectory(temporaryDirectory.resolve("archive"));
        String fileName = "SAT_IMG_20240302120000000.raw";
        Path source = Files.writeString(sourceDirectory.resolve(fileName), "abcdefgh");
        ArchivePathResolver resolver = new ArchivePathResolver(archive);
        Path destination = resolver.resolve(fileName, new FileNameParser().parse(source));
        Path staging = Files.writeString(destination.resolveSibling(fileName + ".tmp"), "abcdZZ");

        var dataSource = new DriverManagerDataSource(
                "jdbc:sqlite:" + temporaryDirectory.resolve("archive.db"));
        ArchiveRepository repository = new ArchiveRepository(new JdbcTemplate(dataSource));
        repository.initialize();
        repository.beginOrResume(fileName, Instant.now());
        repository.markStaging(fileName, 4);
        workers = new ThreadPoolExecutor(2, 2, 0, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(100), new ThreadPoolExecutor.CallerRunsPolicy());
        scheduler = Executors.newSingleThreadScheduledExecutor();
        watcher = new SourceWatcher(
                sourceDirectory,
                new FileNameParser(),
                new StabilityDetector(new StabilityProperties(25, 50, 4)),
                resolver,
                new FileMover(),
                repository,
                workers,
                scheduler,
                1);
        watcher.start();

        await(() -> Files.exists(destination), Duration.ofSeconds(5));
        assertTrue(Files.readString(destination).equals("abcdefgh"));
        assertFalse(Files.exists(source));
        assertFalse(Files.exists(staging));
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
            int pollMillis,
            int quietMillis,
            int overlapBytes,
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
                new StabilityDetector(new StabilityProperties(pollMillis, quietMillis, overlapBytes)),
                new ArchivePathResolver(archive),
                new FileMover(),
                repository,
                workers,
                scheduler,
                1);
        watcher.start();
        return new Fixture(source, archive, repository);
    }

    private Fixture startFixtureWithUnavailableWatchService(String startupFile) throws Exception {
        sourceFileSystem = FileSystems.newFileSystem(
                temporaryDirectory.resolve("source.zip"), Map.of("create", "true"));
        Path source = Files.createDirectory(sourceFileSystem.getPath("/source"));
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
                new StabilityDetector(new StabilityProperties(50, 100, 4)),
                new ArchivePathResolver(archive),
                new FileMover(),
                repository,
                workers,
                scheduler,
                1);
        watcher.start();
        return new Fixture(source, archive, repository);
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

    private record Fixture(Path source, Path archive, ArchiveRepository repository) { }
}
