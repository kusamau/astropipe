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
import java.nio.file.StandardCopyOption;
import java.util.List;

@Component
public final class ArchiveRepository {
    private static final Logger logger = LoggerFactory.getLogger(ArchiveRepository.class);
    private static final int SCHEMA_VERSION = 2;

    private final JdbcTemplate jdbcTemplate;

    public ArchiveRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void initialize() {
        jdbcTemplate.execute("PRAGMA journal_mode=WAL");
        boolean tableExists = Boolean.TRUE.equals(jdbcTemplate.queryForObject(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='archived_file')",
                Boolean.class));
        int version = jdbcTemplate.queryForObject("PRAGMA user_version", Integer.class);
        if (tableExists && version != SCHEMA_VERSION) {
            throw new IllegalStateException("Archive database schema version " + version
                    + " is incompatible with required version " + SCHEMA_VERSION
                    + "; create a fresh database");
        }
        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS archived_file (
                    file_name     TEXT PRIMARY KEY,
                    status        TEXT NOT NULL CHECK (status IN ('COPYING', 'STAGING', 'ARCHIVED', 'FAILED')),
                    copied_bytes  INTEGER NOT NULL DEFAULT 0 CHECK (copied_bytes >= 0),
                    archive_dir   TEXT,
                    saved_at      TEXT,
                    first_seen_at TEXT NOT NULL
                )
                """);
        jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_archived_file_status ON archived_file(status)");
        if (!tableExists) jdbcTemplate.execute("PRAGMA user_version = " + SCHEMA_VERSION);
    }

    public CopyRecord beginOrResume(String fileName, Instant firstSeenAt) {
        String sql = """
                INSERT OR IGNORE INTO archived_file
                    (file_name, status, copied_bytes, archive_dir, saved_at, first_seen_at)
                VALUES (?, 'COPYING', 0, NULL, NULL, ?)
                """;
        jdbcTemplate.update(sql, fileName, firstSeenAt.toString());
        return jdbcTemplate.queryForObject(
                "SELECT status, copied_bytes FROM archived_file WHERE file_name = ?",
                (row, rowNumber) -> new CopyRecord(
                        Status.valueOf(row.getString("status")), row.getLong("copied_bytes")),
                fileName);
    }

    public void markCopying(String fileName) {
        updateActive(fileName, "COPYING", null);
    }

    public void markStaging(String fileName, long copiedBytes) {
        updateActive(fileName, "STAGING", copiedBytes);
    }

    public void resetCheckpoint(String fileName) {
        updateActive(fileName, "COPYING", 0L);
    }

    public void markArchived(String fileName, Path archiveDirectory, Instant savedAt) {
        String sql = """
                UPDATE archived_file
                SET status = 'ARCHIVED', archive_dir = ?, saved_at = ?
                WHERE file_name = ? AND status IN ('COPYING', 'STAGING')
                """;
        int affected = jdbcTemplate.update(
                sql, archiveDirectory.toString(), savedAt.toString(), fileName);
        requireOneRow(affected, fileName, "ARCHIVED");
    }

    public void markFailed(String fileName) {
        int affected = jdbcTemplate.update("""
                UPDATE archived_file SET status = 'FAILED'
                WHERE file_name = ? AND status IN ('COPYING', 'STAGING')
                """, fileName);
        requireOneRow(affected, fileName, "FAILED");
    }

    public ReconciliationResult reconcileInProgress(
            Path sourceDirectory,
            FileNameParser parser,
            ArchivePathResolver resolver) throws IOException {
        int archived = 0;
        int resumable = 0;
        int failed = 0;
        for (CopyEntry entry : activeEntries()) {
            String fileName = entry.fileName();
            Path source = sourceDirectory.resolve(fileName);
            Path destination;
            try {
                LocalDateTime timestamp = parser.parse(Path.of(fileName));
                destination = resolver.resolve(fileName, timestamp);
            } catch (InvalidSatelliteImageNameException | IOException e) {
                markFailed(fileName);
                failed++;
                logger.warn("In-progress database row has an invalid or unresolvable destination: {}", fileName, e);
                continue;
            }

            if (Files.exists(destination)) {
                if (!Files.exists(source)) {
                    markArchived(fileName, destination.getParent(), Instant.now());
                    archived++;
                    logger.info("Reconciled published file as ARCHIVED: {}", fileName);
                    continue;
                }
                long sourceSize = Files.size(source);
                long destinationSize = Files.size(destination);
                if (sourceSize == destinationSize) {
                    Files.delete(source);
                    markArchived(fileName, destination.getParent(), Instant.now());
                    archived++;
                    logger.info("Reconciled published file and removed source: {}", fileName);
                } else if (sourceSize > destinationSize) {
                    Path staging = destination.resolveSibling(destination.getFileName() + ".tmp");
                    if (Files.exists(staging)) {
                        markFailed(fileName);
                        failed++;
                        logger.warn("Both destination and staging exist for in-progress file: {}", fileName);
                    } else {
                        Files.move(destination, staging, StandardCopyOption.ATOMIC_MOVE);
                        markStaging(fileName, destinationSize);
                        resumable++;
                        logger.info("Returned incomplete publication to staging: {}", fileName);
                    }
                } else {
                    markFailed(fileName);
                    failed++;
                    logger.warn("Destination is larger than append-only source: {}", fileName);
                }
            } else if (Files.exists(source)) {
                resumable++;
                logger.info("Found resumable staged file: {} copiedBytes={}", fileName, entry.copiedBytes());
            } else {
                markFailed(fileName);
                failed++;
                logger.warn("In-progress file exists in neither source nor archive: {}", fileName);
            }
        }
        return new ReconciliationResult(archived, resumable, failed);
    }

    private List<CopyEntry> activeEntries() {
        return jdbcTemplate.query(
                """
                SELECT file_name, copied_bytes FROM archived_file
                WHERE status IN ('COPYING', 'STAGING')
                """,
                (row, rowNumber) -> new CopyEntry(row.getString("file_name"), row.getLong("copied_bytes")));
    }

    private void updateActive(String fileName, String status, Long copiedBytes) {
        int affected = copiedBytes == null
                ? jdbcTemplate.update("""
                        UPDATE archived_file SET status = ?
                        WHERE file_name = ? AND status IN ('COPYING', 'STAGING')
                        """, status, fileName)
                : jdbcTemplate.update("""
                        UPDATE archived_file SET status = ?, copied_bytes = ?
                        WHERE file_name = ? AND status IN ('COPYING', 'STAGING')
                        """, status, copiedBytes, fileName);
        requireOneRow(affected, fileName, status);
    }

    private static void requireOneRow(int affected, String fileName, String transition) {
        if (affected != 1) {
            throw new ArchiveStateTransitionException(fileName, transition, affected);
        }
    }

    public enum Status { COPYING, STAGING, ARCHIVED, FAILED }

    public record CopyRecord(Status status, long copiedBytes) {
        public boolean terminal() {
            return status == Status.ARCHIVED || status == Status.FAILED;
        }
    }

    private record CopyEntry(String fileName, long copiedBytes) { }

    public record ReconciliationResult(int archived, int resumable, int failed) { }
}
