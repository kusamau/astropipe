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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ArchiveRepositoryIT {
    @TempDir
    Path temporaryDirectory;

    private String databaseUrl;
    private ArchiveRepository repository;
    private Database database;

    @BeforeEach
    void setUp() {
        databaseUrl = "jdbc:sqlite:" + temporaryDirectory.resolve("archive.db");
        database = new Database(databaseUrl);
        repository = new ArchiveRepository(database);
        repository.initialize();
    }

    @AfterEach
    void tearDown() {
        if (database != null) database.close();
    }

    @Test
    void primaryKeyIsAuthoritativeDedupeGuard() throws Exception {
        String fileName = "SAT_IMG_20210714102730046.raw";

        assertEquals(ArchiveRepository.Status.IN_PROGRESS, repository.claim(fileName, Instant.now()));
        assertEquals(ArchiveRepository.Status.IN_PROGRESS, repository.claim(fileName, Instant.now()));

        assertEquals(1, queryInt("SELECT COUNT(*) FROM archived_file WHERE file_name = ?", fileName));
    }

    @Test
    void reportsAnAlreadyFinishedNameAsTerminal() {
        String archived = "SAT_IMG_20210714102730046.raw";
        String failed = "SAT_IMG_20210714102730047.raw";
        repository.claim(archived, Instant.now());
        repository.claim(failed, Instant.now());
        repository.markArchived(archived, temporaryDirectory, Instant.now());
        repository.markFailed(failed);

        assertEquals(ArchiveRepository.Status.ARCHIVED, repository.claim(archived, Instant.now()));
        assertEquals(ArchiveRepository.Status.FAILED, repository.claim(failed, Instant.now()));
        assertTrue(ArchiveRepository.Status.ARCHIVED.terminal());
        assertTrue(ArchiveRepository.Status.FAILED.terminal());
        assertFalse(ArchiveRepository.Status.IN_PROGRESS.terminal());
    }

    @Test
    void recordsNameSavedTimeAndSavedFolderOnArchive() throws Exception {
        String fileName = "SAT_IMG_20210714102730046.raw";
        Path archiveDirectory = temporaryDirectory.resolve("archive/2021/07/14");
        Instant savedAt = Instant.parse("2021-07-14T10:30:00Z");
        repository.claim(fileName, Instant.now());

        repository.markArchived(fileName, archiveDirectory, savedAt);

        assertEquals("ARCHIVED", queryString("SELECT status FROM archived_file WHERE file_name = ?", fileName));
        assertEquals(archiveDirectory.toString(),
                queryString("SELECT archive_dir FROM archived_file WHERE file_name = ?", fileName));
        assertEquals(savedAt.toString(),
                queryString("SELECT saved_at FROM archived_file WHERE file_name = ?", fileName));
    }

    @Test
    void refusesToTransitionRowsThatAreAbsentOrAlreadyTerminal() {
        String missing = "SAT_IMG_20240101120000000.raw";
        Database.DatabaseException absent = assertThrows(
                Database.DatabaseException.class, () -> repository.markFailed(missing));
        assertTrue(absent.getMessage().contains("expected one in-progress row but updated 0"));

        String archived = "SAT_IMG_20240101120000001.raw";
        repository.claim(archived, Instant.now());
        repository.markArchived(archived, temporaryDirectory, Instant.now());

        assertThrows(Database.DatabaseException.class, () -> repository.markFailed(archived));
    }

    @Test
    void listsOnlyRowsLeftInProgress() {
        String active = "SAT_IMG_20240101120000000.raw";
        String done = "SAT_IMG_20240101120000001.raw";
        repository.claim(active, Instant.now());
        repository.claim(done, Instant.now());
        repository.markArchived(done, temporaryDirectory, Instant.now());

        assertEquals(java.util.List.of(active), repository.inProgressFileNames());
    }

    @Test
    void runsInWalModeWithRelaxedSynchronousWrites() throws Exception {
        // journal_mode is persisted in the database file; synchronous is per-connection,
        // so it has to be read back on the connection the repository actually writes on.
        assertEquals("wal", queryString("PRAGMA journal_mode").toLowerCase());
        assertEquals(1, database.pragma("synchronous"));
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
                        copied_bytes INTEGER NOT NULL DEFAULT 0,
                        archive_dir TEXT,
                        saved_at TEXT,
                        first_seen_at TEXT NOT NULL
                    )
                    """);
        }

        Database legacyDatabase = new Database(legacyUrl);
        Database.DatabaseException failure = assertThrows(Database.DatabaseException.class,
                () -> new ArchiveRepository(legacyDatabase).initialize());
        legacyDatabase.close();

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
