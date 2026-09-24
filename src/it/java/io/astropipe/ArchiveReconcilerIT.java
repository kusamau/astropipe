package io.astropipe;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ArchiveReconcilerIT {
    @TempDir
    Path temporaryDirectory;

    private Path source;
    private ArchivePathResolver resolver;
    private final FileMover fileMover = new FileMover();
    private ArchiveRepository repository;
    private ArchiveReconciler reconciler;

    @BeforeEach
    void setUp() throws Exception {
        source = Files.createDirectory(temporaryDirectory.resolve("source"));
        resolver = new ArchivePathResolver(Files.createDirectory(temporaryDirectory.resolve("archive")));
        repository = new ArchiveRepository("jdbc:sqlite:" + temporaryDirectory.resolve("archive.db"));
        repository.initialize();
        reconciler = new ArchiveReconciler(source, resolver, fileMover, repository);
    }

    @AfterEach
    void tearDown() {
        if (repository != null) repository.close();
    }

    @Test
    void marksAPublishedFileArchivedAndRemovesALeftoverIdenticalSource() throws Exception {
        String published = "SAT_IMG_20240101120000000.raw";
        String duplicated = "SAT_IMG_20240101120000001.raw";
        claim(published, duplicated);
        Files.writeString(destinationOf(published), "complete");
        Files.writeString(destinationOf(duplicated), "complete");
        Files.writeString(source.resolve(duplicated), "complete");

        assertEquals(new ArchiveReconciler.Result(2, 0, 0), reconciler.reconcile());
        assertFalse(Files.exists(source.resolve(duplicated)), "identical leftover source must be removed");
        assertTrue(repository.activeEntries().isEmpty());
    }

    @Test
    void returnsAnIncompletePublicationToStaging() throws Exception {
        String fileName = "SAT_IMG_20240101120000000.raw";
        claim(fileName);
        Path destination = Files.writeString(destinationOf(fileName), "abcd");
        Files.writeString(source.resolve(fileName), "abcdefgh");

        assertEquals(new ArchiveReconciler.Result(0, 1, 0), reconciler.reconcile());
        assertFalse(Files.exists(destination));
        assertEquals("abcd", Files.readString(fileMover.stagingPath(destination)));
        assertEquals(java.util.List.of(new ArchiveRepository.CopyEntry(fileName, 4)), repository.activeEntries());
    }

    @Test
    void leavesAStillPresentSourceResumable() throws Exception {
        String fileName = "SAT_IMG_20240101120000000.raw";
        claim(fileName);
        repository.markStaging(fileName, 4);
        Files.writeString(source.resolve(fileName), "abcdefgh");

        assertEquals(new ArchiveReconciler.Result(0, 1, 0), reconciler.reconcile());
        assertEquals(java.util.List.of(new ArchiveRepository.CopyEntry(fileName, 4)), repository.activeEntries());
    }

    @Test
    void failsRowsThatCannotBeRecovered() throws Exception {
        String vanished = "SAT_IMG_20240101120000000.raw";
        String shrunk = "SAT_IMG_20240101120000001.raw";
        String conflicted = "SAT_IMG_20240101120000002.raw";
        claim(vanished, shrunk, conflicted);
        Files.writeString(destinationOf(shrunk), "abcdefgh");
        Files.writeString(source.resolve(shrunk), "abcd");
        Path conflictedDestination = Files.writeString(destinationOf(conflicted), "abcd");
        Files.writeString(fileMover.stagingPath(conflictedDestination), "abcd");
        Files.writeString(source.resolve(conflicted), "abcdefgh");

        assertEquals(new ArchiveReconciler.Result(0, 0, 3), reconciler.reconcile());
        assertTrue(repository.activeEntries().isEmpty());
    }

    private void claim(String... fileNames) {
        for (String fileName : fileNames) repository.beginOrResume(fileName, Instant.now());
    }

    private Path destinationOf(String fileName) throws Exception {
        return resolver.resolve(fileName, SatelliteImageName.parse(fileName));
    }
}
