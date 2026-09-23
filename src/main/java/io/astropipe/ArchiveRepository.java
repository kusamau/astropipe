package io.astropipe;

import io.astropipe.exception.ArchiveStateTransitionException;
import io.astropipe.exception.InvalidSatelliteImageNameException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Component
public final class ArchiveRepository {
    private static final Logger logger = LoggerFactory.getLogger(ArchiveRepository.class);

    private final JdbcTemplate jdbcTemplate;
    private final Set<String> knownFiles = ConcurrentHashMap.newKeySet();

    public ArchiveRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void initialize() {
        jdbcTemplate.execute("PRAGMA journal_mode=WAL");
        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS archived_file (
                    file_name     TEXT PRIMARY KEY,
                    status        TEXT NOT NULL,
                    archive_dir   TEXT,
                    saved_at      TEXT,
                    first_seen_at TEXT NOT NULL
                )
                """);
        jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_archived_file_status ON archived_file(status)");
        loadKnownFiles();
    }

    public boolean isKnown(String fileName) {
        return knownFiles.contains(fileName);
    }

    public boolean insertPending(String fileName, Instant firstSeenAt) {
        String sql = """
                INSERT OR IGNORE INTO archived_file
                    (file_name, status, archive_dir, saved_at, first_seen_at)
                VALUES (?, 'PENDING', NULL, NULL, ?)
                """;
        boolean inserted = jdbcTemplate.update(sql, fileName, firstSeenAt.toString()) == 1;
        knownFiles.add(fileName);
        return inserted;
    }

    public void markArchived(String fileName, Path archiveDirectory, Instant savedAt) {
        String sql = """
                UPDATE archived_file
                SET status = 'ARCHIVED', archive_dir = ?, saved_at = ?
                WHERE file_name = ? AND status = 'PENDING'
                """;
        int affected = jdbcTemplate.update(
                sql, archiveDirectory.toString(), savedAt.toString(), fileName);
        requireOneRow(affected, fileName, "ARCHIVED");
    }

    public void markFailed(String fileName) {
        updateStatus(fileName, "FAILED");
    }

    public ReconciliationResult reconcilePending(
            Path sourceDirectory,
            FileNameParser parser,
            ArchivePathResolver resolver) throws IOException {
        int archived = 0;
        int reset = 0;
        int failed = 0;
        for (String fileName : pendingFileNames()) {
            Path source = sourceDirectory.resolve(fileName);
            Path destination;
            try {
                LocalDateTime timestamp = parser.parse(Path.of(fileName));
                destination = resolver.resolve(fileName, timestamp);
            } catch (InvalidSatelliteImageNameException | IOException e) {
                markFailed(fileName);
                failed++;
                logger.warn("Pending database row has an invalid or unresolvable destination: {}", fileName, e);
                continue;
            }

            if (Files.exists(destination)) {
                markArchived(fileName, destination.getParent(), Instant.now());
                archived++;
                logger.info("Reconciled pending file as ARCHIVED: {}", fileName);
            } else if (Files.exists(source)) {
                deletePending(fileName);
                knownFiles.remove(fileName);
                reset++;
                logger.info("Reset pending file for reprocessing: {}", fileName);
            } else {
                markFailed(fileName);
                failed++;
                logger.warn("Pending file exists in neither source nor archive: {}", fileName);
            }
        }
        return new ReconciliationResult(archived, reset, failed);
    }

    private void loadKnownFiles() {
        knownFiles.clear();
        knownFiles.addAll(jdbcTemplate.query(
                "SELECT file_name FROM archived_file",
                (result, rowNumber) -> result.getString(1)));
    }

    private List<String> pendingFileNames() {
        return jdbcTemplate.query(
                "SELECT file_name FROM archived_file WHERE status = 'PENDING'",
                (result, rowNumber) -> result.getString(1));
    }

    private void updateStatus(String fileName, String status) {
        int affected = jdbcTemplate.update(
                "UPDATE archived_file SET status = ? WHERE file_name = ? AND status = 'PENDING'",
                status, fileName);
        requireOneRow(affected, fileName, status);
    }

    private void deletePending(String fileName) {
        int affected = jdbcTemplate.update(
                "DELETE FROM archived_file WHERE file_name = ? AND status = 'PENDING'",
                fileName);
        requireOneRow(affected, fileName, "reset");
    }

    private static void requireOneRow(int affected, String fileName, String transition) {
        if (affected != 1) {
            throw new ArchiveStateTransitionException(fileName, transition, affected);
        }
    }

    public record ReconciliationResult(int archived, int reset, int failed) { }
}
