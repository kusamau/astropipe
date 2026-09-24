package io.astropipe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;

/**
 * Resolves rows a previous run left in {@code COPYING} or {@code STAGING} by comparing
 * the ledger against what is actually on disk. Staging files are durable recovery state,
 * so nothing here deletes them on spec.
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
        for (ArchiveRepository.CopyEntry entry : repository.activeEntries()) {
            switch (resolve(entry)) {
                case ARCHIVED -> archived++;
                case RESUMABLE -> resumable++;
                case FAILED -> failed++;
            }
        }
        return new Result(archived, resumable, failed);
    }

    private Outcome resolve(ArchiveRepository.CopyEntry entry) throws IOException {
        String fileName = entry.fileName();
        Path source = sourceDirectory.resolve(fileName);
        Path destination;
        try {
            destination = pathResolver.resolve(fileName, SatelliteImageName.parse(fileName));
        } catch (SatelliteImageName.InvalidNameException | IOException e) {
            logger.warn("In-progress row has an invalid or unresolvable destination: {}", fileName, e);
            return fail(fileName);
        }

        if (!Files.exists(destination)) {
            if (Files.exists(source)) {
                logger.info("Found resumable staged file: {} copiedBytes={}", fileName, entry.copiedBytes());
                return Outcome.RESUMABLE;
            }
            logger.warn("In-progress file exists in neither source nor archive: {}", fileName);
            return fail(fileName);
        }

        if (!Files.exists(source)) {
            return publishedWithoutSource(fileName, destination);
        }

        long sourceSize = Files.size(source);
        long destinationSize = Files.size(destination);
        if (sourceSize == destinationSize) {
            Files.delete(source);
            repository.markArchived(fileName, destination.getParent(), Instant.now());
            logger.info("Reconciled published file and removed source: {}", fileName);
            return Outcome.ARCHIVED;
        }
        if (sourceSize < destinationSize) {
            logger.warn("Destination is larger than append-only source: {}", fileName);
            return fail(fileName);
        }

        Path staging = fileMover.stagingPath(destination);
        if (Files.exists(staging)) {
            logger.warn("Both destination and staging exist for in-progress file: {}", fileName);
            return fail(fileName);
        }
        Files.move(destination, staging, StandardCopyOption.ATOMIC_MOVE);
        repository.markStaging(fileName, destinationSize);
        logger.info("Returned incomplete publication to staging: {}", fileName);
        return Outcome.RESUMABLE;
    }

    private Outcome publishedWithoutSource(String fileName, Path destination) {
        repository.markArchived(fileName, destination.getParent(), Instant.now());
        logger.info("Reconciled published file as ARCHIVED: {}", fileName);
        return Outcome.ARCHIVED;
    }

    private Outcome fail(String fileName) {
        repository.markFailed(fileName);
        return Outcome.FAILED;
    }

    private enum Outcome { ARCHIVED, RESUMABLE, FAILED }

    record Result(int archived, int resumable, int failed) { }
}
