package io.astropipe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;

public final class ArchiveTask implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(ArchiveTask.class);

    private final Path source;
    private final LocalDateTime timestamp;
    private final ArchivePathResolver pathResolver;
    private final FileMover fileMover;
    private final ArchiveRepository repository;
    private final Runnable completion;

    public ArchiveTask(
            Path source,
            LocalDateTime timestamp,
            ArchivePathResolver pathResolver,
            FileMover fileMover,
            ArchiveRepository repository,
            Runnable completion) {
        this.source = source;
        this.timestamp = timestamp;
        this.pathResolver = pathResolver;
        this.fileMover = fileMover;
        this.repository = repository;
        this.completion = completion;
    }

    @Override
    public void run() {
        String fileName = source.getFileName().toString();
        long started = System.nanoTime();
        try {
            if (!Files.isRegularFile(source)) {
                logger.debug("Source disappeared before processing: {}", source);
                return;
            }
            if (!repository.insertPending(fileName, Instant.now())) {
                logger.debug("Skipping duplicate file already represented in the database: {}", fileName);
                return;
            }

            Path destination;
            FileMover.MoveResult moveResult;
            try {
                destination = pathResolver.resolve(fileName, timestamp);
                moveResult = fileMover.move(source, destination);
            } catch (IOException moveFailure) {
                recordFailure(fileName, started, moveFailure);
                return;
            }

            try {
                repository.markArchived(fileName, destination.getParent(), Instant.now());
                logger.info("Archived file={} mode={} destination={} durationMs={}",
                        fileName, moveResult.method(), destination, moveResult.durationMillis());
            } catch (DataAccessException databaseFailure) {
                logger.error("File moved but ARCHIVED database update failed; leaving PENDING for recovery: {}",
                        fileName, databaseFailure);
            }
        } catch (DataAccessException databaseFailure) {
            logger.error("Could not insert PENDING database row for file={}", fileName, databaseFailure);
        } finally {
            completion.run();
        }
    }

    private void recordFailure(String fileName, long started, Exception failure) {
        try {
            repository.markFailed(fileName);
        } catch (DataAccessException updateFailure) {
            failure.addSuppressed(updateFailure);
        }
        long durationMillis = (System.nanoTime() - started) / 1_000_000;
        logger.error("Archive operation failed for file={} source={} durationMs={}",
                fileName, source, durationMillis, failure);
    }
}
