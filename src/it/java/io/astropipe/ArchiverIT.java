package io.astropipe;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ArchiverIT {
    private static final int CHUNK_SIZE = 64 * 1024;

    @TempDir
    Path temporaryDirectory;

    private SourceWatcher watcher;
    private ThreadPoolExecutor workers;
    private ScheduledExecutorService scheduler;
    private FileSystem sourceFileSystem;
    private ArchiveRepository repository;
    private Database database;

    @AfterEach
    void tearDown() throws Exception {
        if (watcher != null) watcher.close();
        if (scheduler != null) scheduler.shutdownNow();
        if (workers != null) {
            workers.shutdown();
            workers.awaitTermination(5, TimeUnit.SECONDS);
        }
        if (database != null) database.close();
        if (sourceFileSystem != null) sourceFileSystem.close();
    }

    /** The pipeline settings the watcher reads, tuned down so tests stay fast. */
    private static Config config(int pollMillis, int quietMillis) {
        return config(pollMillis, quietMillis, Config.SourceAfterArchive.DELETE);
    }

    private static Config config(int pollMillis, int quietMillis, Config.SourceAfterArchive after) {
        return new Config("source", "archive", null, pollMillis, quietMillis, 1, 2, 100, after);
    }

    @Test
    void archivesStartupAndWatchEventsAndIgnoresMalformedFiles() throws Exception {
        Fixture fixture = start(config(10, 50), "SAT_IMG_20211231235959999.raw");
        Path startupDestination = fixture.archive.resolve(
                "2021/12/31/SAT_IMG_20211231235959999.raw");
        Await.until("the startup file is archived", () -> Files.exists(startupDestination));

        Path watched = fixture.source.resolve("SAT_IMG_20220101000000000.raw");
        Files.writeString(watched, "watch event");
        Path watchedDestination = fixture.archive.resolve(
                "2022/01/01/SAT_IMG_20220101000000000.raw");
        Await.until("the watched file is archived", () -> Files.exists(watchedDestination));

        Path malformed = fixture.source.resolve("not-an-image.raw");
        Files.writeString(malformed, "ignored");
        TimeUnit.MILLISECONDS.sleep(150);
        assertTrue(Files.exists(malformed));
    }

    @Test
    void doesNotMoveFileWhileUnsafeGeneratorIsStillWriting() throws Exception {
        // A long quiet period so that a scheduling hiccup cannot be mistaken for the
        // producer having stopped, and a write long enough to outlast detection. How
        // quickly WatchService reports a new file is a property of the platform - it is
        // instant on Linux and polled on macOS - so nothing here may depend on it.
        Fixture fixture = start(config(50, 800), null);
        String fileName = "SAT_IMG_20240229120000000.raw";
        Path source = fixture.source.resolve(fileName);
        Path destination = fixture.archive.resolve("2024/02/29").resolve(fileName);
        Path staging = destination.resolveSibling(fileName + ".tmp");
        int chunks = 20;

        AtomicBoolean stagingSeen = new AtomicBoolean();
        AtomicReference<Throwable> writerFailure = new AtomicReference<>();
        Thread writer = new Thread(() -> {
            try {
                writeInChunks(source, chunks, () -> {
                    // The invariant, checked after every chunk rather than at one
                    // arbitrary moment: a source that is still growing is never published.
                    assertFalse(Files.exists(destination),
                            "published while the source was still growing");
                    assertTrue(Files.exists(source), "a changing source must remain in place");
                    if (Files.exists(staging)) stagingSeen.set(true);
                });
            } catch (Throwable e) {
                writerFailure.set(e);
            }
        });

        writer.start();
        writer.join();

        Throwable failure = writerFailure.get();
        if (failure != null) throw new AssertionError("the producer thread failed", failure);
        assertTrue(stagingSeen.get(),
                "a growing source should be copied into archive-side staging as it grows");

        Await.until("the image is published", () -> Files.exists(destination));
        assertEquals((long) chunks * CHUNK_SIZE, Files.size(destination));
        assertFalse(Files.exists(source), "the source is removed once its image is published");
        assertFalse(Files.exists(staging));
    }

    @Test
    void keepsTheSourceWhenConfiguredTo() throws Exception {
        Fixture fixture = start(config(25, 50, Config.SourceAfterArchive.KEEP), null);
        String fileName = "SAT_IMG_20240401080000000.raw";
        Path source = Files.writeString(fixture.source.resolve(fileName), "keep me");
        Path destination = fixture.archive.resolve("2024/04/01").resolve(fileName);

        Await.until("the image is published", () -> Files.exists(destination));

        assertEquals("keep me", Files.readString(destination));
        assertTrue(Files.exists(source), "the source must survive under sourceAfterArchive=keep");

        // The ledger, not the absence of the file, is what stops it being archived again:
        // the retained source is re-listed on every sweep and has to be refused each time.
        Await.until("at least two further listings have seen the retained file",
                () -> repository.claim(fileName, Instant.now()) == ArchiveRepository.Status.ARCHIVED);
        TimeUnit.MILLISECONDS.sleep(300);
        assertEquals(List.of(), repository.inProgressFileNames());
        assertEquals("keep me", Files.readString(destination));
        assertTrue(Files.exists(source));
        assertFalse(Files.exists(destination.resolveSibling(fileName + ".tmp")));
    }

    @Test
    void fallsBackToReconciliationWhenWatchServiceIsUnavailable() throws Exception {
        Fixture fixture = startWithoutWatchService("SAT_IMG_20231231235959999.raw");
        Path startupDestination = fixture.archive.resolve(
                "2023/12/31/SAT_IMG_20231231235959999.raw");
        Await.until("the startup file is archived", () -> Files.exists(startupDestination));

        Path source = fixture.source.resolve("SAT_IMG_20240101000000000.raw");
        Files.writeString(source, "reconciliation fallback");
        Path destination = fixture.archive.resolve(
                "2024/01/01/SAT_IMG_20240101000000000.raw");
        Await.until("the image is published", () -> Files.exists(destination));
    }

    @Test
    void failsAndKeepsBothArtifactsWhenTheSourceShrinks() throws Exception {
        Fixture fixture = start(config(25, 2_000), null);
        String fileName = "SAT_IMG_20240301120000000.raw";
        Path source = Files.writeString(fixture.source.resolve(fileName), "abcdefgh");
        Path destination = fixture.archive.resolve("2024/03/01").resolve(fileName);
        Path staging = destination.resolveSibling(fileName + ".tmp");
        Await.until("the source is fully staged", () -> Files.exists(staging) && Files.size(staging) == 8);

        // An append-only source may never lose bytes it has already handed over.
        Files.writeString(source, "abcd", StandardOpenOption.TRUNCATE_EXISTING);

        Await.until("the ledger records the failure",
                () -> repository.claim(fileName, Instant.now()) == ArchiveRepository.Status.FAILED);
        assertTrue(Files.exists(source));
        assertTrue(Files.exists(staging), "a failed transfer keeps its artifacts for inspection");
        assertFalse(Files.exists(destination));
    }

    @Test
    void discardsStagingLeftByAPreviousRunAndCopiesFromTheStart() throws Exception {
        Path sourceDirectory = Files.createDirectory(temporaryDirectory.resolve("source"));
        Path archive = Files.createDirectory(temporaryDirectory.resolve("archive"));
        String fileName = "SAT_IMG_20240302120000000.raw";
        Path source = Files.writeString(sourceDirectory.resolve(fileName), "abcdefgh");
        Path destination = Files.createDirectories(archive.resolve("2024/03/02")).resolve(fileName);
        // Staging from a dead run, holding bytes that never matched the source.
        Path staging = Files.writeString(destination.resolveSibling(fileName + ".tmp"), "ZZZZZZ");

        start(sourceDirectory, archive, config(25, 50));

        Await.until("the image is published", () -> Files.exists(destination));
        assertEquals("abcdefgh", Files.readString(destination));
        assertFalse(Files.exists(source));
        assertFalse(Files.exists(staging));
    }

    /** Writes in place, slowly, running {@code afterEachChunk} once every chunk lands. */
    private static void writeInChunks(Path path, int chunks, Runnable afterEachChunk)
            throws IOException, InterruptedException {
        byte[] buffer = new byte[CHUNK_SIZE];
        try (OutputStream output = Files.newOutputStream(path,
                StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            for (int written = 0; written < chunks; written++) {
                output.write(buffer);
                output.flush();
                afterEachChunk.run();
                if (written < chunks - 1) TimeUnit.MILLISECONDS.sleep(100);
            }
        }
    }

    /** Starts a watcher over a fresh source and archive under the temporary directory. */
    private Fixture start(Config config, String startupFile) throws Exception {
        Path source = Files.createDirectory(temporaryDirectory.resolve("source"));
        Path archive = Files.createDirectory(temporaryDirectory.resolve("archive"));
        if (startupFile != null) Files.writeString(source.resolve(startupFile), "startup");
        return start(source, archive, config);
    }

    /** Starts a watcher over the given directories, which the caller has already prepared. */
    private Fixture start(Path source, Path archive, Config config) throws Exception {
        database = new Database("jdbc:sqlite:" + temporaryDirectory.resolve("archive.db"));
        repository = new ArchiveRepository(database);
        repository.initialize();
        workers = new ThreadPoolExecutor(2, 2, 0, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(100), new ThreadPoolExecutor.CallerRunsPolicy());
        scheduler = Executors.newSingleThreadScheduledExecutor();
        watcher = new SourceWatcher(source, config, new ArchivePathResolver(archive),
                new FileMover(), repository, workers, scheduler);
        watcher.start();
        return new Fixture(source, archive);
    }

    /** A source on a zip filesystem, which supports no WatchService at all. */
    private Fixture startWithoutWatchService(String startupFile) throws Exception {
        sourceFileSystem = FileSystems.newFileSystem(
                temporaryDirectory.resolve("source.zip"), Map.of("create", "true"));
        Path source = Files.createDirectory(sourceFileSystem.getPath("/source"));
        Files.writeString(source.resolve(startupFile), "startup");
        return start(source, Files.createDirectory(temporaryDirectory.resolve("archive")), config(50, 100));
    }

    private record Fixture(Path source, Path archive) { }
}
