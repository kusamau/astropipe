package io.astropipe;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ArchiveRepositoryIT {
    @TempDir
    Path temporaryDirectory;

    private ArchiveRepository repository;
    private Database database;

    @BeforeEach
    void setUp() {
        database = new Database("jdbc:sqlite:" + temporaryDirectory.resolve("archive.db"));
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

        assertEquals(1, count("SELECT COUNT(*) FROM archived_file WHERE file_name = ?", fileName));
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

        assertEquals("ARCHIVED", column("status", fileName));
        assertEquals(archiveDirectory.toString(),
                column("archive_dir", fileName));
        assertEquals(savedAt.toString(),
                column("saved_at", fileName));
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
        assertEquals("wal", database.queryOne("PRAGMA journal_mode", r -> r.getString(1)).toLowerCase());
        assertEquals(1, database.pragma("synchronous"));
    }

    private String column(String name, String fileName) {
        return database.queryOne(
                "SELECT " + name + " FROM archived_file WHERE file_name = ?", r -> r.getString(1), fileName);
    }

    private int count(String sql, Object... parameters) {
        return database.queryOne(sql, r -> r.getInt(1), parameters);
    }
}
