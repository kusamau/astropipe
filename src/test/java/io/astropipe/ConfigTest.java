package io.astropipe;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConfigTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void loadsTheShippedConfigurationFile() throws Exception {
        Config config = Config.load(Path.of("config.json"));

        assertEquals("./data/source", config.sourceDir());
        assertEquals("./data/archive", config.archiveDir());
        assertEquals("jdbc:sqlite:./data/archive.db", config.databaseUrl());
    }

    @Test
    void appliesDefaultsToEverythingButTheTwoRequiredDirectories() throws Exception {
        Config config = load("""
                {"sourceDir": "source", "archiveDir": "archive"}
                """);

        assertEquals("jdbc:sqlite:./data/archive.db", config.databaseUrl());
        assertEquals(250, config.pollIntervalMillis());
        assertEquals(250, config.quietPeriodMillis());
        assertEquals(30, config.reconciliationIntervalSeconds());
        assertEquals(2, config.workerPoolSize());
        assertEquals(10_000, config.workerQueueCapacity());
    }

    @Test
    void overridesDefaults() throws Exception {
        Config config = load("""
                {"sourceDir": "source", "archiveDir": "archive", "workerPoolSize": 4}
                """);

        assertEquals(4, config.workerPoolSize());
        assertEquals(2, new Config("source", "archive", null, 0, 0, 0, 0, 0, null).workerPoolSize());
    }

    @Test
    void keepsTheSourceOnlyWhenAskedTo() throws Exception {
        // "move" is what the requirement asks for, so removing the source is the default.
        assertEquals(Config.SourceAfterArchive.DELETE,
                load("""
                        {"sourceDir": "source", "archiveDir": "archive"}
                        """).sourceAfterArchive());
        assertTrue(load("""
                {"sourceDir": "source", "archiveDir": "archive"}
                """).deletesArchivedSource());

        Config keeping = load("""
                {"sourceDir": "source", "archiveDir": "archive", "sourceAfterArchive": "keep"}
                """);
        assertEquals(Config.SourceAfterArchive.KEEP, keeping.sourceAfterArchive());
        assertFalse(keeping.deletesArchivedSource());
    }

    @Test
    void rejectsAnUnknownRetentionMode() {
        assertMessageContains("sourceAfterArchive must be \"delete\" or \"keep\", not \"archive\"", """
                {"sourceDir": "source", "archiveDir": "archive", "sourceAfterArchive": "archive"}
                """);
    }

    @Test
    void rejectsMissingDirectories() {
        assertMessageContains("sourceDir must not be blank", """
                {"archiveDir": "archive"}
                """);
        assertMessageContains("archiveDir must not be blank", """
                {"sourceDir": "source"}
                """);
    }

    @Test
    void rejectsSourceAndArchivePointingAtTheSameDirectory() {
        assertMessageContains("must be different directories", """
                {"sourceDir": "./data", "archiveDir": "data"}
                """);
    }

    @Test
    void rejectsNegativeTuningValues() {
        assertMessageContains("pollIntervalMillis must be greater than zero", """
                {"sourceDir": "source", "archiveDir": "archive", "pollIntervalMillis": -1}
                """);
    }

    @Test
    void rejectsUnknownSettings() {
        assertMessageContains("unknownSetting", """
                {"sourceDir": "source", "archiveDir": "archive", "unknownSetting": true}
                """);
    }

    private void assertMessageContains(String expected, String json) {
        IOException failure = assertThrows(IOException.class, () -> load(json));
        assertTrue(failure.getMessage().contains(expected),
                "expected \"" + expected + "\" in: " + failure.getMessage());
    }

    private Config load(String json) throws IOException {
        return Config.load(Files.writeString(temporaryDirectory.resolve("config.json"), json));
    }
}
