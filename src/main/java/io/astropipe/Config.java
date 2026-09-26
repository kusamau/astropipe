package io.astropipe;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.databind.DatabindException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Locale;

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
        int workerPoolSize,
        int workerQueueCapacity,
        SourceAfterArchive sourceAfterArchive) {

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
        workerQueueCapacity = positive(workerQueueCapacity, 10_000, "workerQueueCapacity");
        sourceAfterArchive = sourceAfterArchive == null ? SourceAfterArchive.DELETE : sourceAfterArchive;
    }

    public static Config load(Path file) throws IOException {
        try {
            return new ObjectMapper().readValue(file.toFile(), Config.class);
        } catch (DatabindException e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            throw new IOException("Invalid configuration file " + file + ": " + cause.getMessage(), e);
        }
    }

    /** Whether a published image is removed from the source, which is what "move" means. */
    boolean deletesArchivedSource() {
        return sourceAfterArchive == SourceAfterArchive.DELETE;
    }

    /**
     * What becomes of a source image once it has been archived.
     *
     * <p>{@code DELETE} is the default and is what the requirement to <em>move</em> images
     * asks for. {@code KEEP} leaves them in place, which helps while investigating but turns
     * the source directory from a queue that drains into one that only grows: every listing
     * re-reports each retained file and the ledger has to refuse it again.
     */
    enum SourceAfterArchive {
        DELETE, KEEP;

        @JsonCreator
        static SourceAfterArchive from(String value) {
            try {
                return valueOf(value.trim().toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                        "sourceAfterArchive must be \"delete\" or \"keep\", not \"" + value + "\"");
            }
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
