package io.astropipe;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ArchiverIT {
    private static final int CHUNK_SIZE = 64 * 1024;

    @TempDir
    Path temporaryDirectory;

    private SourceWatcher watcher;
    private FileSystem sourceFileSystem;
    private ArchiveRepository repository;
    private Database database;
    private final ListAppender<ILoggingEvent> watcherLog = new ListAppender<>();

    @AfterEach
    void tearDown() throws Exception {
        watcherLogger().detachAppender(watcherLog);
        watcherLogger().setLevel(null);
        if (watcher != null) watcher.close();
        if (database != null) database.close();
        if (sourceFileSystem != null) sourceFileSystem.close();
    }

    /** The pipeline settings the watcher reads, tuned down so tests stay fast. */
    private static Config config(int pollMillis, int quietMillis) {
        return new Config("source", "archive", null, pollMillis, quietMillis, 1, 2);
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

    @Test
    void archivesManyImagesOfTheSameDayArrivingTogether() throws Exception {
        Fixture fixture = start(config(10, 50), null);
        List<Path> destinations = new ArrayList<>();
        for (int image = 0; image < 50; image++) {
            String fileName = "SAT_IMG_20240304120000%03d.raw".formatted(image);
            Files.writeString(fixture.source.resolve(fileName), "image " + image);
            destinations.add(fixture.archive.resolve("2024/03/04").resolve(fileName));
        }

        Await.until("every image is archived", () -> destinations.stream().allMatch(Files::exists));
        try (var remaining = Files.list(fixture.source)) {
            assertEquals(0, remaining.count(), "every source is removed once its image is published");
        }
    }

    @Test
    void leavesATransferStoppedMidwayInProgressForTheNextStart() throws Exception {
        // A quiet period long enough that the transfer is still waiting when it is stopped.
        Fixture fixture = start(config(25, 60_000), null);
        String fileName = "SAT_IMG_20240305120000000.raw";
        Path source = Files.writeString(fixture.source.resolve(fileName), "abcdefgh");
        Path staging = fixture.archive.resolve("2024/03/05").resolve(fileName + ".tmp");
        Await.until("the source is fully staged", () -> Files.exists(staging) && Files.size(staging) == 8);

        assertTimeoutPreemptively(Duration.ofSeconds(5), watcher::close);

        assertEquals(List.of(fileName), repository.inProgressFileNames(),
                "a stop is not a failure: startup reconciliation must be able to pick the image up again");
        assertTrue(Files.exists(source));
    }

    @Test
    void warnsOnceAboutAMalformedNameEvenWhenOnlyListingsFindIt() throws Exception {
        // Without a WatchService, a file that appears after startup is only ever met by listings.
        watcherLogger().setLevel(Level.DEBUG);
        watcherLog.start();
        watcherLogger().addAppender(watcherLog);
        Fixture fixture = startWithoutWatchService("SAT_IMG_20231231235959999.raw");

        Files.writeString(fixture.source.resolve("not-an-image.txt"), "stray");

        Await.until("two periodic listings have met the stray file", () -> strayFileReports().size() >= 2);
        List<Level> levels = strayFileReports();
        assertEquals(Level.WARN, levels.getFirst(), "the first time this run meets a stray file is worth a warning");
        assertEquals(1, Collections.frequency(levels, Level.WARN), "and only the first time");
    }

    private static Logger watcherLogger() {
        return (Logger) LoggerFactory.getLogger(SourceWatcher.class);
    }

    /** Levels of the watcher's reports about the stray file, oldest first. */
    private List<Level> strayFileReports() {
        synchronized (watcherLog) { // appenders append under their own lock
            return watcherLog.list.stream()
                    .filter(event -> event.getFormattedMessage().contains("not-an-image.txt"))
                    .map(ILoggingEvent::getLevel)
                    .toList();
        }
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
        watcher = new SourceWatcher(source, config, archive, repository);
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
