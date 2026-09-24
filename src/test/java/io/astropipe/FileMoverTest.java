package io.astropipe;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FileMoverTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void appendsOnlyTheRequestedRangeAndPublishesAtomically() throws Exception {
        Path source = temporaryDirectory.resolve("source.raw");
        Path destination = temporaryDirectory.resolve("archive.raw");
        FileMover mover = new FileMover();
        Path staging = mover.stagingPath(destination);
        Files.writeString(source, "abcdefgh");

        assertEquals(0, mover.normalizeStaging(staging, 0));
        assertEquals(4, mover.appendRange(source, staging, 0, 4));
        mover.validateOverlap(source, staging, 2, 4);
        assertEquals(8, mover.appendRange(source, staging, 4, 8));
        mover.publish(staging, destination);

        assertEquals("abcdefgh", Files.readString(destination));
        assertTrue(Files.exists(source));
        assertFalse(Files.exists(staging));
    }

    @Test
    void detectsOverlapMismatchBeforeAppending() throws Exception {
        Path source = temporaryDirectory.resolve("source.raw");
        Path staging = temporaryDirectory.resolve("archive.raw.tmp");
        Files.writeString(source, "abcdefgh");
        Files.writeString(staging, "abcX");

        assertThrows(FileMover.OverlapMismatchException.class,
                () -> new FileMover().validateOverlap(source, staging, 0, 4));
        assertEquals("abcX", Files.readString(staging));
    }

    @Test
    void truncatesOnlyBytesBeyondTheDurableCheckpoint() throws Exception {
        Path staging = temporaryDirectory.resolve("archive.raw.tmp");
        Files.writeString(staging, "abcdefgh");

        assertEquals(4, new FileMover().normalizeStaging(staging, 4));

        assertEquals("abcd", Files.readString(staging));
    }

    @Test
    void resetsStagingWhenItIsShorterThanTheCheckpoint() throws Exception {
        Path staging = temporaryDirectory.resolve("archive.raw.tmp");
        Files.writeString(staging, "abc", StandardOpenOption.CREATE_NEW);

        assertEquals(0, new FileMover().normalizeStaging(staging, 4));

        assertEquals(0, Files.size(staging));
    }
}
