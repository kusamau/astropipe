package io.astropipe;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MainTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void createsMissingArchiveDirectoryWhenEnabled() throws Exception {
        Path archive = temporaryDirectory.resolve("nested/archive");

        Main.prepareArchiveDirectory(archive, true);

        assertThat(archive).isDirectory();
    }

    @Test
    void rejectsMissingArchiveDirectoryWhenDisabled() {
        Path archive = temporaryDirectory.resolve("archive");

        assertThatThrownBy(() -> Main.prepareArchiveDirectory(archive, false))
                .isInstanceOf(IOException.class)
                .hasMessage("archiveDir directory does not exist: " + archive);
    }

    @Test
    void rejectsArchivePathThatIsNotDirectory() throws Exception {
        Path archive = Files.writeString(temporaryDirectory.resolve("archive"), "not a directory");

        assertThatThrownBy(() -> Main.prepareArchiveDirectory(archive, true))
                .isInstanceOf(IOException.class)
                .hasMessage("archiveDir is not a directory: " + archive);
    }
}
