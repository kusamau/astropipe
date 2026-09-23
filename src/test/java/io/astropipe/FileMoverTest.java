package io.astropipe;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FileMoverTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void usesAtomicRenameOnSameFilestore() throws Exception {
        Path source = temporaryDirectory.resolve("source.raw");
        Path destination = temporaryDirectory.resolve("archive.raw");
        byte[] content = "satellite-data".getBytes();
        Files.write(source, content);

        FileMover.MoveResult result = new FileMover(false).move(source, destination);

        assertEquals(FileMover.MoveMethod.ATOMIC_RENAME, result.method());
        assertFalse(Files.exists(source));
        assertArrayEquals(content, Files.readAllBytes(destination));
    }

    @Test
    void forcedCopyPublishesCompleteFileAndDeletesSource() throws Exception {
        Path source = temporaryDirectory.resolve("source.raw");
        Path destination = temporaryDirectory.resolve("archive.raw");
        byte[] content = new byte[3 * 1024 * 1024 + 17];
        content[content.length - 1] = 42;
        Files.write(source, content);

        FileMover.MoveResult result = new FileMover(true).move(source, destination);

        assertEquals(FileMover.MoveMethod.COPY_FALLBACK, result.method());
        assertFalse(Files.exists(source));
        assertFalse(Files.exists(destination.resolveSibling("archive.raw.tmp")));
        assertArrayEquals(content, Files.readAllBytes(destination));
    }

    @Test
    void neverOverwritesExistingArchiveFile() throws Exception {
        Path source = temporaryDirectory.resolve("source.raw");
        Path destination = temporaryDirectory.resolve("archive.raw");
        Files.writeString(source, "new");
        Files.writeString(destination, "existing");

        assertThrows(FileAlreadyExistsException.class,
                () -> new FileMover(false).move(source, destination));
        assertEquals("new", Files.readString(source));
        assertEquals("existing", Files.readString(destination));
    }

    @Test
    void removesOnlyAstropipeTemporaryFilesRecursively() throws Exception {
        Path nested = Files.createDirectories(temporaryDirectory.resolve("2024/02/29"));
        Path stale = Files.writeString(nested.resolve("SAT_IMG_20240229120000000.raw.tmp"), "partial");
        Path unrelated = Files.writeString(nested.resolve("notes.tmp"), "keep");
        Path malformed = Files.writeString(nested.resolve("SAT_IMG_invalid.raw.tmp"), "keep");
        Path invalidDate = Files.writeString(
                nested.resolve("SAT_IMG_20210229120000000.raw.tmp"), "keep");

        int removed = new FileMover(false).cleanStaleTemporaryFiles(temporaryDirectory);

        assertEquals(1, removed);
        assertFalse(Files.exists(stale));
        assertTrue(Files.exists(unrelated));
        assertTrue(Files.exists(malformed));
        assertTrue(Files.exists(invalidDate));
    }
}
