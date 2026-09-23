package io.astropipe;

import io.astropipe.exception.ArchiveStateTransitionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
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

    @BeforeEach
    void setUp() throws Exception {
        databaseUrl = "jdbc:sqlite:" + temporaryDirectory.resolve("archive.db");
        repository = new ArchiveRepository(new JdbcTemplate(new DriverManagerDataSource(databaseUrl)));
        repository.initialize();
    }

    @Test
    void primaryKeyIsAuthoritativeDedupeGuard() throws Exception {
        String fileName = "SAT_IMG_20210714102730046.raw";
        assertTrue(repository.insertPending(fileName, Instant.now()));
        assertFalse(repository.insertPending(fileName, Instant.now()));
        assertEquals(1, rowCount(fileName));
    }

    @Test
    void appliesPendingArchivedAndFailedTransitions() throws Exception {
        String archived = "SAT_IMG_20210714102730046.raw";
        String failed = "SAT_IMG_20210714102730047.raw";
        repository.insertPending(archived, Instant.now());
        repository.insertPending(failed, Instant.now());

        repository.markArchived(archived, temporaryDirectory.resolve("2021/07/14"), Instant.now());
        repository.markFailed(failed);

        assertEquals("ARCHIVED", status(archived));
        assertEquals("FAILED", status(failed));
    }

    @Test
    void reportsInvalidStateTransitionsWithDomainContext() {
        ArchiveStateTransitionException missing = assertThrows(
                ArchiveStateTransitionException.class,
                () -> repository.markFailed("SAT_IMG_20240101120000000.raw"));
        assertEquals("SAT_IMG_20240101120000000.raw", missing.fileName());
        assertEquals("FAILED", missing.transition());
        assertEquals(0, missing.affectedRows());

        String archived = "SAT_IMG_20240101120000001.raw";
        repository.insertPending(archived, Instant.now());
        repository.markArchived(archived, temporaryDirectory.resolve("2024/01/01"), Instant.now());

        ArchiveStateTransitionException repeated = assertThrows(
                ArchiveStateTransitionException.class,
                () -> repository.markFailed(archived));
        assertEquals(archived, repeated.fileName());
        assertEquals("FAILED", repeated.transition());
        assertEquals(0, repeated.affectedRows());
    }

    @Test
    void reconcilesEveryPendingCrashState() throws Exception {
        Path source = Files.createDirectory(temporaryDirectory.resolve("source"));
        Path archive = Files.createDirectory(temporaryDirectory.resolve("archive"));
        ArchivePathResolver resolver = new ArchivePathResolver(archive);
        FileNameParser parser = new FileNameParser();
        String atDestination = "SAT_IMG_20240101120000000.raw";
        String atSource = "SAT_IMG_20240101120000001.raw";
        String missing = "SAT_IMG_20240101120000002.raw";
        repository.insertPending(atDestination, Instant.now());
        repository.insertPending(atSource, Instant.now());
        repository.insertPending(missing, Instant.now());
        Files.writeString(resolver.resolve(atDestination, parser.parse(Path.of(atDestination))), "complete");
        Files.writeString(source.resolve(atSource), "retry");

        ArchiveRepository.ReconciliationResult result =
                repository.reconcilePending(source, parser, resolver);

        assertEquals(new ArchiveRepository.ReconciliationResult(1, 1, 1), result);
        assertEquals("ARCHIVED", status(atDestination));
        assertEquals(0, rowCount(atSource));
        assertFalse(repository.isKnown(atSource));
        assertEquals("FAILED", status(missing));
    }

    private int rowCount(String fileName) throws Exception {
        try (var connection = DriverManager.getConnection(databaseUrl);
             var statement = connection.prepareStatement(
                     "SELECT COUNT(*) FROM archived_file WHERE file_name = ?")) {
            statement.setString(1, fileName);
            try (var rows = statement.executeQuery()) {
                return rows.getInt(1);
            }
        }
    }

    private String status(String fileName) throws Exception {
        try (var connection = DriverManager.getConnection(databaseUrl);
             var statement = connection.prepareStatement(
                     "SELECT status FROM archived_file WHERE file_name = ?")) {
            statement.setString(1, fileName);
            try (var rows = statement.executeQuery()) {
                return rows.next() ? rows.getString(1) : null;
            }
        }
    }
}
