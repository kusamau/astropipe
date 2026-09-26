package io.astropipe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;

/**
 * Resolves rows a previous run left {@code IN_PROGRESS} by comparing the ledger against
 * what is actually on disk.
 *
 * <p>Staging never survives a restart, so a surviving source file is the authority: any
 * partial artifact is discarded and the image is copied again from the beginning. Only a
 * published destination, or the loss of both copies, is terminal.
 */
final class ArchiveReconciler {
    private static final Logger logger = LoggerFactory.getLogger(ArchiveReconciler.class);

    private final Path sourceDirectory;
    private final ArchivePathResolver pathResolver;
    private final FileMover fileMover;
    private final ArchiveRepository repository;

    ArchiveReconciler(
            Path sourceDirectory,
            ArchivePathResolver pathResolver,
            FileMover fileMover,
            ArchiveRepository repository) {
        this.sourceDirectory = sourceDirectory;
        this.pathResolver = pathResolver;
        this.fileMover = fileMover;
        this.repository = repository;
    }

    Result reconcile() throws IOException {
        int archived = 0;
        int resumable = 0;
        int failed = 0;
        for (String fileName : repository.inProgressFileNames()) {
            switch (resolve(fileName)) {
                case ARCHIVED -> archived++;
                case RESUMABLE -> resumable++;
                case FAILED -> failed++;
            }
        }
        return new Result(archived, resumable, failed);
    }

    private Outcome resolve(String fileName) throws IOException {
        Path source = sourceDirectory.resolve(fileName);
        Path destination;
        try {
            destination = pathResolver.resolve(SatelliteImageName.parse(fileName));
        } catch (SatelliteImageName.InvalidNameException | IOException e) {
            logger.warn("In-progress row has an invalid or unresolvable destination: {}", fileName, e);
            return fail(fileName);
        }

        if (!Files.exists(source)) {
            if (!Files.exists(destination)) {
                logger.warn("In-progress file exists in neither source nor archive: {}", fileName);
                return fail(fileName);
            }
            repository.markArchived(fileName, destination.getParent(), Instant.now());
            logger.info("Reconciled published file as ARCHIVED: {}", fileName);
            return Outcome.ARCHIVED;
        }

        if (Files.exists(destination)) {
            long sourceSize = Files.size(source);
            long destinationSize = Files.size(destination);
            if (sourceSize == destinationSize) {
                Files.delete(source);
                repository.markArchived(fileName, destination.getParent(), Instant.now());
                logger.info("Reconciled published file and removed source: {}", fileName);
                return Outcome.ARCHIVED;
            }
            if (destinationSize > sourceSize) {
                logger.warn("Destination is larger than append-only source: {}", fileName);
                return fail(fileName);
            }
            // A published file shorter than its source is incomplete. The source is intact,
            // so discard it rather than leave a truncated image under a complete name.
            Files.delete(destination);
            logger.warn("Discarded an incomplete publication; it will be copied again: {}", fileName);
        }

        Files.deleteIfExists(fileMover.stagingPath(destination));
        logger.info("Source survives and will be copied again from the start: {}", fileName);
        return Outcome.RESUMABLE;
    }

    private Outcome fail(String fileName) {
        repository.markFailed(fileName);
        return Outcome.FAILED;
    }

    private enum Outcome { ARCHIVED, RESUMABLE, FAILED }

    record Result(int archived, int resumable, int failed) { }
}
