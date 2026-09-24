package io.astropipe;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MainTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void createsMissingArchiveDirectoryWithParents() throws Exception {
        Path archive = temporaryDirectory.resolve("nested/archive");

        assertEquals(archive, Main.directory(archive.toString(), "archiveDir", true));
        assertTrue(Files.isDirectory(archive));
    }

    @Test
    void rejectsMissingSourceDirectory() {
        Path source = temporaryDirectory.resolve("source");

        IOException failure = assertThrows(IOException.class,
                () -> Main.directory(source.toString(), "sourceDir", false));

        assertEquals("sourceDir is not an existing directory: " + source, failure.getMessage());
    }

    @Test
    void rejectsPathThatIsNotADirectory() throws Exception {
        Path archive = Files.writeString(temporaryDirectory.resolve("archive"), "not a directory");

        IOException failure = assertThrows(IOException.class,
                () -> Main.directory(archive.toString(), "archiveDir", true));

        assertEquals("archiveDir is not an existing directory: " + archive, failure.getMessage());
    }
}
