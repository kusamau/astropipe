package io.astropipe;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ArchiveRepositoryIT {
    @TempDir
    Path temporaryDirectory;

    private String databaseUrl;
    private ArchiveRepository repository;

    @BeforeEach
    void setUp() {
        databaseUrl = "jdbc:sqlite:" + temporaryDirectory.resolve("archive.db");
        repository = new ArchiveRepository(databaseUrl);
        repository.initialize();
    }

    @AfterEach
    void tearDown() {
        if (repository != null) repository.close();
    }

    @Test
    void primaryKeyIsAuthoritativeDedupeGuard() throws Exception {
        String fileName = "SAT_IMG_20210714102730046.raw";

        assertEquals(new ArchiveRepository.CopyRecord(ArchiveRepository.Status.COPYING, 0),
                repository.beginOrResume(fileName, Instant.now()));
        assertEquals(new ArchiveRepository.CopyRecord(ArchiveRepository.Status.COPYING, 0),
                repository.beginOrResume(fileName, Instant.now()));

        assertEquals(1, queryInt("SELECT COUNT(*) FROM archived_file WHERE file_name = ?", fileName));
    }

    @Test
    void recordsNameSavedTimeAndSavedFolderOnArchive() throws Exception {
        String fileName = "SAT_IMG_20210714102730046.raw";
        Path archiveDirectory = temporaryDirectory.resolve("archive/2021/07/14");
        Instant savedAt = Instant.parse("2021-07-14T10:30:00Z");
        repository.beginOrResume(fileName, Instant.now());

        repository.markStaging(fileName, 8);
        repository.markArchived(fileName, archiveDirectory, savedAt);

        assertEquals("ARCHIVED", queryString("SELECT status FROM archived_file WHERE file_name = ?", fileName));
        assertEquals(archiveDirectory.toString(),
                queryString("SELECT archive_dir FROM archived_file WHERE file_name = ?", fileName));
        assertEquals(savedAt.toString(),
                queryString("SELECT saved_at FROM archived_file WHERE file_name = ?", fileName));
    }

    @Test
    void appliesCopyingStagingAndFailedTransitions() throws Exception {
        String archived = "SAT_IMG_20210714102730046.raw";
        String failed = "SAT_IMG_20210714102730047.raw";
        repository.beginOrResume(archived, Instant.now());
        repository.beginOrResume(failed, Instant.now());

        repository.markStaging(archived, 4);
        repository.markCopying(archived);
        repository.markStaging(archived, 8);
        repository.resetCheckpoint(archived);
        repository.markFailed(failed);

        assertEquals(0, queryInt("SELECT copied_bytes FROM archived_file WHERE file_name = ?", archived));
        assertEquals("FAILED", queryString("SELECT status FROM archived_file WHERE file_name = ?", failed));
    }

    @Test
    void refusesToTransitionRowsThatAreAbsentOrAlreadyTerminal() {
        String missing = "SAT_IMG_20240101120000000.raw";
        ArchiveRepository.DatabaseException absent = assertThrows(
                ArchiveRepository.DatabaseException.class, () -> repository.markFailed(missing));
        assertTrue(absent.getMessage().contains("expected one active row but updated 0"));

        String archived = "SAT_IMG_20240101120000001.raw";
        repository.beginOrResume(archived, Instant.now());
        repository.markArchived(archived, temporaryDirectory, Instant.now());

        assertThrows(ArchiveRepository.DatabaseException.class, () -> repository.markFailed(archived));
    }

    @Test
    void listsOnlyRowsLeftInProgress() {
        String active = "SAT_IMG_20240101120000000.raw";
        String done = "SAT_IMG_20240101120000001.raw";
        repository.beginOrResume(active, Instant.now());
        repository.beginOrResume(done, Instant.now());
        repository.markStaging(active, 12);
        repository.markArchived(done, temporaryDirectory, Instant.now());

        assertEquals(java.util.List.of(new ArchiveRepository.CopyEntry(active, 12)), repository.activeEntries());
    }

    @Test
    void runsInWalModeWithRelaxedSynchronousWrites() throws Exception {
        // journal_mode is persisted in the database file; synchronous is per-connection,
        // so it has to be read back on the connection the repository actually writes on.
        assertEquals("wal", queryString("PRAGMA journal_mode").toLowerCase());
        assertEquals(1, repository.pragma("synchronous"));
    }

    @Test
    void rejectsTheLegacySchemaWithoutMutatingIt() throws Exception {
        String legacyUrl = "jdbc:sqlite:" + temporaryDirectory.resolve("legacy.db");
        try (Connection connection = DriverManager.getConnection(legacyUrl);
             Statement statement = connection.createStatement()) {
            statement.execute("""
                    CREATE TABLE archived_file (
                        file_name TEXT PRIMARY KEY,
                        status TEXT NOT NULL,
                        archive_dir TEXT,
                        saved_at TEXT,
                        first_seen_at TEXT NOT NULL
                    )
                    """);
        }

        ArchiveRepository legacy = new ArchiveRepository(legacyUrl);
        ArchiveRepository.DatabaseException failure =
                assertThrows(ArchiveRepository.DatabaseException.class, legacy::initialize);
        legacy.close();

        assertTrue(failure.getMessage().contains("create a fresh database"));
    }

    private int queryInt(String sql, Object... parameters) throws Exception {
        return query(sql, parameters).getAsInt();
    }

    private String queryString(String sql, Object... parameters) throws Exception {
        try (Connection connection = DriverManager.getConnection(databaseUrl);
             PreparedStatement statement = bind(connection, sql, parameters);
             ResultSet rows = statement.executeQuery()) {
            return rows.next() ? rows.getString(1) : null;
        }
    }

    private java.util.OptionalInt query(String sql, Object... parameters) throws Exception {
        try (Connection connection = DriverManager.getConnection(databaseUrl);
             PreparedStatement statement = bind(connection, sql, parameters);
             ResultSet rows = statement.executeQuery()) {
            return rows.next() ? java.util.OptionalInt.of(rows.getInt(1)) : java.util.OptionalInt.empty();
        }
    }

    private static PreparedStatement bind(Connection connection, String sql, Object... parameters)
            throws Exception {
        PreparedStatement statement = connection.prepareStatement(sql);
        for (int i = 0; i < parameters.length; i++) statement.setObject(i + 1, parameters[i]);
        return statement;
    }
}
