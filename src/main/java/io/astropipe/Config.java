package io.astropipe;

import com.fasterxml.jackson.databind.DatabindException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Path;

/**
 * The JSON configuration file, bound as a whole. Only {@code sourceDir} and
 * {@code archiveDir} are required; every other setting falls back to a default
 * when it is absent or zero.
 */
public record Config(
        String sourceDir,
        String archiveDir,
        String databaseUrl,
        int pollIntervalMillis,
        int quietPeriodMillis,
        int reconciliationIntervalSeconds,
        int workerPoolSize) {

    public Config {
        // Reject what cannot be defaulted...
        requireNotBlank(sourceDir, "sourceDir");
        requireNotBlank(archiveDir, "archiveDir");
        if (absolute(sourceDir).equals(absolute(archiveDir))) {
            throw new IllegalArgumentException("sourceDir and archiveDir must be different directories");
        }

        // ...then fill in what was left out.
        databaseUrl = databaseUrl == null || databaseUrl.isBlank()
                ? "jdbc:sqlite:./data/archive.db" : databaseUrl;
        pollIntervalMillis = positive(pollIntervalMillis, 250, "pollIntervalMillis");
        quietPeriodMillis = positive(quietPeriodMillis, 250, "quietPeriodMillis");
        reconciliationIntervalSeconds = positive(reconciliationIntervalSeconds, 30, "reconciliationIntervalSeconds");
        workerPoolSize = positive(workerPoolSize, 2, "workerPoolSize");
    }

    public static Config load(Path file) throws IOException {
        try {
            return new ObjectMapper().readValue(file.toFile(), Config.class);
        } catch (DatabindException e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            throw new IOException("Invalid configuration file " + file + ": " + cause.getMessage(), e);
        }
    }

    private static void requireNotBlank(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
    }

    private static int positive(int value, int fallback, String name) {
        if (value == 0) return fallback;
        if (value < 0) throw new IllegalArgumentException(name + " must be greater than zero");
        return value;
    }

    private static Path absolute(String directory) {
        return Path.of(directory).toAbsolutePath().normalize();
    }
}
