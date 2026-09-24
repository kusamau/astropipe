package io.astropipe;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;

public final class FileMover {
    private static final long MAX_TRANSFER_CHUNK = 2L * 1024 * 1024 * 1024 - 1;
    private static final int COMPARISON_BUFFER_SIZE = 64 * 1024;

    public Path stagingPath(Path destination) {
        return destination.resolveSibling(destination.getFileName() + ".tmp");
    }

    public long normalizeStaging(Path staging, long checkpoint) throws IOException {
        if (!Files.exists(staging)) {
            Files.createFile(staging);
            return 0;
        }
        long size = Files.size(staging);
        if (size < checkpoint) {
            try (FileChannel output = FileChannel.open(staging, StandardOpenOption.WRITE)) {
                output.truncate(0);
                output.force(true);
            }
            return 0;
        }
        if (size > checkpoint) {
            try (FileChannel output = FileChannel.open(staging, StandardOpenOption.WRITE)) {
                output.truncate(checkpoint);
                output.force(true);
            }
        }
        return checkpoint;
    }

    public void validateOverlap(
            Path source,
            Path staging,
            long resumeOffset,
            long checkpoint) throws IOException {
        if (resumeOffset < 0 || resumeOffset > checkpoint) {
            throw new IllegalArgumentException("Invalid overlap range " + resumeOffset + ".." + checkpoint);
        }
        try (FileChannel sourceChannel = FileChannel.open(source, StandardOpenOption.READ);
             FileChannel stagingChannel = FileChannel.open(staging, StandardOpenOption.READ)) {
            ByteBuffer sourceBuffer = ByteBuffer.allocate(COMPARISON_BUFFER_SIZE);
            ByteBuffer stagingBuffer = ByteBuffer.allocate(COMPARISON_BUFFER_SIZE);
            long position = resumeOffset;
            while (position < checkpoint) {
                int count = (int) Math.min(COMPARISON_BUFFER_SIZE, checkpoint - position);
                readFully(sourceChannel, sourceBuffer, position, count, source);
                readFully(stagingChannel, stagingBuffer, position, count, staging);
                if (!sourceBuffer.equals(stagingBuffer)) {
                    throw new OverlapMismatchException(source, staging, resumeOffset, checkpoint);
                }
                position += count;
            }
        }
    }

    public long appendRange(Path source, Path staging, long from, long to) throws IOException {
        if (from < 0 || to < from) {
            throw new IllegalArgumentException("Invalid copy range " + from + ".." + to);
        }
        try (FileChannel input = FileChannel.open(source, StandardOpenOption.READ);
             FileChannel output = FileChannel.open(staging, StandardOpenOption.WRITE)) {
            long stagingSize = output.size();
            if (stagingSize != from) {
                throw new IOException("Staging size " + stagingSize
                        + " does not match checkpoint " + from + " for " + staging);
            }
            output.position(from);
            long position = from;
            while (position < to) {
                long transferred = input.transferTo(
                        position, Math.min(to - position, MAX_TRANSFER_CHUNK), output);
                if (transferred <= 0) {
                    throw new IOException("Copy made no progress for " + source + " at byte " + position);
                }
                position += transferred;
            }
            output.force(true);
            return position;
        }
    }

    public void publish(Path staging, Path destination) throws IOException {
        if (Files.exists(destination)) {
            throw new FileAlreadyExistsException(destination.toString());
        }
        Files.move(staging, destination, StandardCopyOption.ATOMIC_MOVE);
    }

    public void rollbackPublication(Path destination, Path staging) throws IOException {
        if (Files.exists(staging)) {
            throw new FileAlreadyExistsException(staging.toString());
        }
        Files.move(destination, staging, StandardCopyOption.ATOMIC_MOVE);
    }

    private static void readFully(
            FileChannel channel,
            ByteBuffer buffer,
            long position,
            int count,
            Path path) throws IOException {
        buffer.clear();
        buffer.limit(count);
        long offset = position;
        while (buffer.hasRemaining()) {
            int read = channel.read(buffer, offset);
            if (read < 0) {
                throw new IOException("Unexpected end of file while reading " + path + " at byte " + offset);
            }
            if (read == 0) {
                throw new IOException("Read made no progress for " + path + " at byte " + offset);
            }
            offset += read;
        }
        buffer.flip();
    }

    public static final class OverlapMismatchException extends IOException {
        public OverlapMismatchException(Path source, Path staging, long from, long to) {
            super("Source and staging overlap differ for " + source + " and " + staging
                    + " over bytes " + from + ".." + to);
        }
    }
}
