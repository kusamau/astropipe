package io.astropipe;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;

/**
 * Archive-side staging: append available source bytes into {@code <destination>.tmp},
 * then publish with an atomic rename. Copying is incremental so a very large image
 * starts moving while the producer is still writing it.
 *
 * <p>Staging is valid only for the run that created it. A restart discards whatever
 * a previous attempt left behind and copies again from the beginning, so there is no
 * checkpoint to carry across processes and no resume boundary to validate.
 */
final class FileMover {
    static final long TRANSFER_CHUNK = 64L * 1024 * 1024;

    private FileMover() { }

    static Path stagingPath(Path destination) {
        return destination.resolveSibling(destination.getFileName() + ".tmp");
    }

    /** Starts an empty staging file in its archive directory, discarding anything an earlier attempt left. */
    static void createStaging(Path staging) throws IOException {
        Files.createDirectories(staging.getParent());
        Files.deleteIfExists(staging);
        Files.createFile(staging);
    }

    /** Appends {@code from..to} of the source onto staging and returns the new size. */
    static long appendRange(Path source, Path staging, long from, long to) throws IOException {
        if (from < 0 || to < from) {
            throw new IllegalArgumentException("Invalid copy range " + from + ".." + to);
        }
        try (FileChannel input = FileChannel.open(source, StandardOpenOption.READ);
             FileChannel output = FileChannel.open(staging, StandardOpenOption.WRITE)) {
            long stagingSize = output.size();
            if (stagingSize != from) {
                throw new IOException("Staging size " + stagingSize
                        + " does not match copied bytes " + from + " for " + staging);
            }
            output.position(from);
            long position = from;
            // Each call is capped because a file copy already in the kernel ignores an interrupt:
            // shutdown then takes effect between chunks rather than after the whole image.
            while (position < to) {
                long transferred = input.transferTo(position, Math.min(to - position, TRANSFER_CHUNK), output);
                if (transferred <= 0) {
                    throw new IOException("Copy made no progress for " + source + " at byte " + position);
                }
                position += transferred;
            }
            output.force(true);
            return position;
        }
    }

    static void publish(Path staging, Path destination) throws IOException {
        if (Files.exists(destination)) {
            throw new FileAlreadyExistsException(destination.toString());
        }
        Files.move(staging, destination, StandardCopyOption.ATOMIC_MOVE);
    }

    static void rollbackPublication(Path destination, Path staging) throws IOException {
        if (Files.exists(staging)) {
            throw new FileAlreadyExistsException(staging.toString());
        }
        Files.move(destination, staging, StandardCopyOption.ATOMIC_MOVE);
    }
}
