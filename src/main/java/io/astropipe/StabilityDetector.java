package io.astropipe;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;

/** Decides when a source file has stopped changing, by size and modification time. */
final class StabilityDetector {
    private final long quietPeriodNanos;

    StabilityDetector(int quietPeriodMillis) {
        this.quietPeriodNanos = quietPeriodMillis * 1_000_000L;
    }

    Snapshot sample(Path path) throws IOException {
        BasicFileAttributes attributes = Files.readAttributes(path, BasicFileAttributes.class);
        if (!attributes.isRegularFile()) {
            throw new IOException("Source path is not a regular file: " + path);
        }
        return new Snapshot(attributes.size(), attributes.lastModifiedTime());
    }

    boolean unchanged(Snapshot previous, Snapshot current) {
        return previous != null && previous.equals(current);
    }

    boolean quietFor(long sinceNanos, long nowNanos) {
        return nowNanos - sinceNanos >= quietPeriodNanos;
    }

    record Snapshot(long size, FileTime lastModifiedTime) { }
}
