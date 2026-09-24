package io.astropipe;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * A single SQLite connection with the small amount of JDBC ceremony this application
 * needs, and nothing about archiving. Checked {@link SQLException}s become
 * {@link DatabaseException}, which callers are not expected to recover from.
 *
 * <p>Every method is synchronized, so the connection is safe to share. Callers that
 * need several statements to be indivisible must hold their own lock across them.
 */
final class Database implements AutoCloseable {
    private final Connection connection;

    Database(String url) {
        try {
            this.connection = DriverManager.getConnection(url);
        } catch (SQLException e) {
            throw new DatabaseException("Could not open database " + url, e);
        }
        // Write-ahead logging with relaxed fsync: durable across a process crash,
        // and only the last transactions are at risk if the machine loses power.
        execute("PRAGMA journal_mode=WAL");
        execute("PRAGMA synchronous=NORMAL");
    }

    synchronized void execute(String sql) {
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        } catch (SQLException e) {
            throw new DatabaseException("Statement failed: " + sql, e);
        }
    }

    synchronized int update(String sql, Object... parameters) {
        try (PreparedStatement statement = prepare(sql, parameters)) {
            return statement.executeUpdate();
        } catch (SQLException e) {
            throw new DatabaseException("Update failed: " + sql, e);
        }
    }

    synchronized <T> T queryOne(String sql, RowMapper<T> mapper, Object... parameters) {
        List<T> rows = queryList(sql, mapper, parameters);
        if (rows.size() != 1) {
            throw new DatabaseException("Expected exactly one row but got " + rows.size() + ": " + sql);
        }
        return rows.getFirst();
    }

    synchronized <T> List<T> queryList(String sql, RowMapper<T> mapper, Object... parameters) {
        try (PreparedStatement statement = prepare(sql, parameters);
             ResultSet rows = statement.executeQuery()) {
            List<T> results = new ArrayList<>();
            while (rows.next()) results.add(mapper.map(rows));
            return results;
        } catch (SQLException e) {
            throw new DatabaseException("Query failed: " + sql, e);
        }
    }

    /** Reads an integer PRAGMA on this connection. Some PRAGMAs are per-connection. */
    synchronized int pragma(String name) {
        return queryOne("PRAGMA " + name, row -> row.getInt(1));
    }

    @Override
    public synchronized void close() {
        try {
            connection.close();
        } catch (SQLException e) {
            throw new DatabaseException("Could not close database", e);
        }
    }

    private PreparedStatement prepare(String sql, Object... parameters) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(sql);
        for (int i = 0; i < parameters.length; i++) {
            statement.setObject(i + 1, parameters[i]);
        }
        return statement;
    }

    interface RowMapper<T> {
        T map(ResultSet row) throws SQLException;
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
