package io.astropipe;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FileMoverTest {
    @TempDir
    Path temporaryDirectory;

    private final FileMover mover = new FileMover();

    @Test
    void appendsOnlyTheRequestedRangeAndPublishesAtomically() throws Exception {
        Path source = Files.writeString(temporaryDirectory.resolve("source.raw"), "abcdefgh");
        Path destination = temporaryDirectory.resolve("archive.raw");
        Path staging = mover.stagingPath(destination);
        mover.createStaging(staging);

        assertEquals(4, mover.appendRange(source, staging, 0, 4));
        assertEquals("abcd", Files.readString(staging));
        assertEquals(8, mover.appendRange(source, staging, 4, 8));
        mover.publish(staging, destination);

        assertEquals("abcdefgh", Files.readString(destination));
        assertTrue(Files.exists(source), "publishing does not remove the source");
        assertFalse(Files.exists(staging));
    }

    @Test
    void copiesARangeSpanningSeveralTransferCallsIntact() throws Exception {
        byte[] bytes = new byte[Math.toIntExact(FileMover.TRANSFER_CHUNK * 2 + 3)];
        for (int i = 0; i < bytes.length; i++) bytes[i] = (byte) (i * 31 + i / 4096);
        Path source = Files.write(temporaryDirectory.resolve("source.raw"), bytes);
        Path staging = temporaryDirectory.resolve("archive.raw.tmp");
        mover.createStaging(staging);

        assertEquals(bytes.length, mover.appendRange(source, staging, 0, bytes.length));
        assertEquals(-1, Files.mismatch(source, staging));
    }

    @Test
    void startsStagingEmptyEvenWhenAPreviousAttemptLeftBytes() throws Exception {
        Path staging = Files.writeString(temporaryDirectory.resolve("archive.raw.tmp"), "stale");

        mover.createStaging(staging);

        assertEquals(0, Files.size(staging));
    }

    @Test
    void refusesToAppendAtAnOffsetThatDoesNotMatchStaging() throws Exception {
        Path source = Files.writeString(temporaryDirectory.resolve("source.raw"), "abcdefgh");
        Path staging = temporaryDirectory.resolve("archive.raw.tmp");
        mover.createStaging(staging);

        assertThrows(java.io.IOException.class, () -> mover.appendRange(source, staging, 4, 8));
    }

    @Test
    void refusesToPublishOverAnExistingDestinationAndRollsBack() throws Exception {
        Path destination = Files.writeString(temporaryDirectory.resolve("archive.raw"), "already there");
        Path staging = Files.writeString(temporaryDirectory.resolve("archive.raw.tmp"), "new");

        assertThrows(java.nio.file.FileAlreadyExistsException.class, () -> mover.publish(staging, destination));

        Files.delete(staging);
        mover.rollbackPublication(destination, staging);
        assertEquals("already there", Files.readString(staging));
        assertFalse(Files.exists(destination));
    }
}
