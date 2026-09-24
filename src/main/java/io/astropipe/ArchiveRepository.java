package io.astropipe;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

/**
 * The archive ledger. Its primary key on {@code file_name} is the authoritative guard
 * against processing the same image twice.
 *
 * <p>A row is {@code IN_PROGRESS} from the moment the file is claimed until it is either
 * {@code ARCHIVED} or {@code FAILED}. Copy progress is deliberately not recorded: staging
 * never survives a restart, so there is no checkpoint worth persisting.
 *
 * <p>Each state change is one statement whose {@code WHERE} clause carries the rule it
 * enforces, so the guard is applied by the database rather than by a read followed by a
 * write.
 */
final class ArchiveRepository {
    private static final int SCHEMA_VERSION = 3;

    private final Database database;

    ArchiveRepository(Database database) {
        this.database = database;
    }

    /** Compound: the version check and the schema creation must not interleave. */
    synchronized void initialize() {
        boolean tableExists = database.queryOne(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='archived_file')",
                row -> row.getBoolean(1));
        int version = database.pragma("user_version");
        if (tableExists && version != SCHEMA_VERSION) {
            throw new Database.DatabaseException("Archive database schema version " + version
                    + " is incompatible with required version " + SCHEMA_VERSION
                    + "; create a fresh database");
        }
        database.execute("""
                CREATE TABLE IF NOT EXISTS archived_file (
                    file_name     TEXT PRIMARY KEY,
                    status        TEXT NOT NULL CHECK (status IN ('IN_PROGRESS', 'ARCHIVED', 'FAILED')),
                    archive_dir   TEXT,
                    saved_at      TEXT,
                    first_seen_at TEXT NOT NULL
                )
                """);
        database.execute("CREATE INDEX IF NOT EXISTS idx_archived_file_status ON archived_file(status)");
        if (!tableExists) database.execute("PRAGMA user_version = " + SCHEMA_VERSION);
    }

    /**
     * Claims the file, or reports the state a previous run left it in.
     *
     * <p>Compound: the insert and the read that follows it must not interleave with
     * another claim of the same name, or both callers would believe they claimed it.
     */
    synchronized Status claim(String fileName, Instant firstSeenAt) {
        database.update("""
                INSERT OR IGNORE INTO archived_file
                    (file_name, status, archive_dir, saved_at, first_seen_at)
                VALUES (?, 'IN_PROGRESS', NULL, NULL, ?)
                """, fileName, firstSeenAt.toString());
        return database.queryOne("SELECT status FROM archived_file WHERE file_name = ?",
                row -> Status.valueOf(row.getString(1)), fileName);
    }

    void markArchived(String fileName, Path archiveDirectory, Instant savedAt) {
        int affected = database.update("""
                UPDATE archived_file SET status = 'ARCHIVED', archive_dir = ?, saved_at = ?
                WHERE file_name = ? AND status = 'IN_PROGRESS'
                """, archiveDirectory.toString(), savedAt.toString(), fileName);
        requireOneRow(affected, fileName, "ARCHIVED");
    }

    void markFailed(String fileName) {
        int affected = database.update("""
                UPDATE archived_file SET status = 'FAILED' WHERE file_name = ? AND status = 'IN_PROGRESS'
                """, fileName);
        requireOneRow(affected, fileName, "FAILED");
    }

    /** Names a previous run left mid-flight, for {@link ArchiveReconciler} to resolve. */
    List<String> inProgressFileNames() {
        return database.queryList("SELECT file_name FROM archived_file WHERE status = 'IN_PROGRESS'",
                row -> row.getString(1));
    }

    private static void requireOneRow(int affected, String fileName, String transition) {
        if (affected != 1) {
            throw new Database.DatabaseException("Could not transition " + fileName + " to " + transition
                    + "; expected one in-progress row but updated " + affected);
        }
    }

    enum Status {
        IN_PROGRESS, ARCHIVED, FAILED;

        boolean terminal() {
            return this != IN_PROGRESS;
        }
    }
}
