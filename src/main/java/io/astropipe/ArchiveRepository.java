package io.astropipe;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * The archive ledger. Its primary key on {@code file_name} is the authoritative guard
 * against processing the same image twice.
 *
 * <p>A row is {@code IN_PROGRESS} from the moment the file is claimed until it is either
 * {@code ARCHIVED} or {@code FAILED}. Copy progress is deliberately not recorded: staging
 * never survives a restart, so there is no checkpoint worth persisting.
 *
 * <p>A single connection serialises the handful of tiny statements the pipeline issues,
 * which is all SQLite would allow concurrently anyway.
 */
final class ArchiveRepository implements AutoCloseable {
    private static final int SCHEMA_VERSION = 3;

    private final Connection connection;

    ArchiveRepository(String databaseUrl) {
        try {
            this.connection = DriverManager.getConnection(databaseUrl);
        } catch (SQLException e) {
            throw new DatabaseException("Could not open archive database " + databaseUrl, e);
        }
    }

    synchronized void initialize() {
        execute("PRAGMA journal_mode=WAL");
        execute("PRAGMA synchronous=NORMAL");
        boolean tableExists = queryOne(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='archived_file')",
                row -> row.getBoolean(1));
        int version = pragma("user_version");
        if (tableExists && version != SCHEMA_VERSION) {
            throw new DatabaseException("Archive database schema version " + version
                    + " is incompatible with required version " + SCHEMA_VERSION
                    + "; create a fresh database");
        }
        execute("""
                CREATE TABLE IF NOT EXISTS archived_file (
                    file_name     TEXT PRIMARY KEY,
                    status        TEXT NOT NULL CHECK (status IN ('IN_PROGRESS', 'ARCHIVED', 'FAILED')),
                    archive_dir   TEXT,
                    saved_at      TEXT,
                    first_seen_at TEXT NOT NULL
                )
                """);
        execute("CREATE INDEX IF NOT EXISTS idx_archived_file_status ON archived_file(status)");
        if (!tableExists) execute("PRAGMA user_version = " + SCHEMA_VERSION);
    }

    /** Claims the file, or reports the state a previous run left it in. */
    synchronized Status claim(String fileName, Instant firstSeenAt) {
        update("""
                INSERT OR IGNORE INTO archived_file
                    (file_name, status, archive_dir, saved_at, first_seen_at)
                VALUES (?, 'IN_PROGRESS', NULL, NULL, ?)
                """, fileName, firstSeenAt.toString());
        return queryOne("SELECT status FROM archived_file WHERE file_name = ?",
                row -> Status.valueOf(row.getString(1)), fileName);
    }

    synchronized void markArchived(String fileName, Path archiveDirectory, Instant savedAt) {
        int affected = update("""
                UPDATE archived_file SET status = 'ARCHIVED', archive_dir = ?, saved_at = ?
                WHERE file_name = ? AND status = 'IN_PROGRESS'
                """, archiveDirectory.toString(), savedAt.toString(), fileName);
        requireOneRow(affected, fileName, "ARCHIVED");
    }

    synchronized void markFailed(String fileName) {
        int affected = update("""
                UPDATE archived_file SET status = 'FAILED' WHERE file_name = ? AND status = 'IN_PROGRESS'
                """, fileName);
        requireOneRow(affected, fileName, "FAILED");
    }

    /** Reads an integer PRAGMA on the working connection. */
    synchronized int pragma(String name) {
        return queryOne("PRAGMA " + name, row -> row.getInt(1));
    }

    /** Names a previous run left mid-flight, for {@link ArchiveReconciler} to resolve. */
    synchronized List<String> inProgressFileNames() {
        return queryList("SELECT file_name FROM archived_file WHERE status = 'IN_PROGRESS'",
                row -> row.getString(1));
    }

    @Override
    public synchronized void close() {
        try {
            connection.close();
        } catch (SQLException e) {
            throw new DatabaseException("Could not close archive database", e);
        }
    }

    private static void requireOneRow(int affected, String fileName, String transition) {
        if (affected != 1) {
            throw new DatabaseException("Could not transition " + fileName + " to " + transition
                    + "; expected one in-progress row but updated " + affected);
        }
    }

    private void execute(String sql) {
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        } catch (SQLException e) {
            throw new DatabaseException("Statement failed: " + sql, e);
        }
    }

    private int update(String sql, Object... parameters) {
        try (PreparedStatement statement = prepare(sql, parameters)) {
            return statement.executeUpdate();
        } catch (SQLException e) {
            throw new DatabaseException("Update failed: " + sql, e);
        }
    }

    private <T> T queryOne(String sql, RowMapper<T> mapper, Object... parameters) {
        List<T> rows = queryList(sql, mapper, parameters);
        if (rows.size() != 1) {
            throw new DatabaseException("Expected exactly one row but got " + rows.size() + ": " + sql);
        }
        return rows.getFirst();
    }

    private <T> List<T> queryList(String sql, RowMapper<T> mapper, Object... parameters) {
        try (PreparedStatement statement = prepare(sql, parameters);
             ResultSet rows = statement.executeQuery()) {
            List<T> results = new ArrayList<>();
            while (rows.next()) results.add(mapper.map(rows));
            return results;
        } catch (SQLException e) {
            throw new DatabaseException("Query failed: " + sql, e);
        }
    }

    private PreparedStatement prepare(String sql, Object... parameters) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(sql);
        for (int i = 0; i < parameters.length; i++) {
            statement.setObject(i + 1, parameters[i]);
        }
        return statement;
    }

    private interface RowMapper<T> {
        T map(ResultSet row) throws SQLException;
    }

    enum Status {
        IN_PROGRESS, ARCHIVED, FAILED;

        boolean terminal() {
            return this != IN_PROGRESS;
        }
    }

    static final class DatabaseException extends RuntimeException {
        DatabaseException(String message) {
            super(message);
        }

        DatabaseException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
