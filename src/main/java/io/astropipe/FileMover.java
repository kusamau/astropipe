package io.astropipe;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

public final class FileMover {
    private static final long MAX_TRANSFER_CHUNK = 2L * 1024 * 1024 * 1024 - 1;

    private final boolean forceCopyPath;

    public FileMover(boolean forceCopyPath) {
        this.forceCopyPath = forceCopyPath;
    }

    public MoveResult move(Path source, Path destination) throws IOException {
        long started = System.nanoTime();
        if (Files.exists(destination)) {
            throw new FileAlreadyExistsException(destination.toString());
        }
        if (!forceCopyPath) {
            try {
                Files.move(source, destination, StandardCopyOption.ATOMIC_MOVE);
                return new MoveResult(MoveMethod.ATOMIC_RENAME, elapsedMillis(started));
            } catch (AtomicMoveNotSupportedException ignored) {
                // Cross-filestore moves take the crash-safe copy path below.
            }
        }
        copyThenDelete(source, destination);
        return new MoveResult(MoveMethod.COPY_FALLBACK, elapsedMillis(started));
    }

    private void copyThenDelete(Path source, Path destination) throws IOException {
        Path temporary = destination.resolveSibling(destination.getFileName() + ".tmp");
        boolean published = false;
        try {
            try (FileChannel input = FileChannel.open(source, StandardOpenOption.READ);
                 FileChannel output = FileChannel.open(temporary,
                         StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
                long size = input.size();
                long position = 0;
                while (position < size) {
                    long transferred = input.transferTo(position,
                            Math.min(size - position, MAX_TRANSFER_CHUNK), output);
                    if (transferred <= 0) {
                        throw new IOException("Copy made no progress for " + source + " at byte " + position);
                    }
                    position += transferred;
                }
                output.force(true);
            }
            Files.move(temporary, destination, StandardCopyOption.ATOMIC_MOVE);
            published = true;
            try {
                Files.delete(source);
            } catch (IOException deleteFailure) {
                try {
                    Files.delete(destination);
                    published = false;
                } catch (IOException rollbackFailure) {
                    deleteFailure.addSuppressed(rollbackFailure);
                }
                throw new IOException("Copied " + source + " but could not remove the source", deleteFailure);
            }
        } finally {
            if (!published) Files.deleteIfExists(temporary);
        }
    }

    public int cleanStaleTemporaryFiles(Path archiveRoot) throws IOException {
        List<IOException> failures = new ArrayList<>();
        int removed = 0;
        try (var paths = Files.walk(archiveRoot)) {
            for (Path path : paths.filter(Files::isRegularFile)
                    .filter(SatelliteImageName::isArchiveTemporaryFile).toList()) {
                try {
                    if (Files.deleteIfExists(path)) removed++;
                } catch (IOException e) {
                    failures.add(e);
                }
            }
        }
        if (!failures.isEmpty()) {
            IOException combined = new IOException("Could not remove all stale archive temporary files");
            failures.forEach(combined::addSuppressed);
            throw combined;
        }
        return removed;
    }

    private static long elapsedMillis(long started) {
        return (System.nanoTime() - started) / 1_000_000;
    }

    public enum MoveMethod { ATOMIC_RENAME, COPY_FALLBACK }

    public record MoveResult(MoveMethod method, long durationMillis) { }
}
