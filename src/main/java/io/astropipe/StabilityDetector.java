package io.astropipe;

import io.astropipe.config.props.StabilityProperties;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.Objects;

public final class StabilityDetector {
    private final StabilityProperties properties;

    public StabilityDetector(StabilityProperties properties) {
        this.properties = Objects.requireNonNull(properties, "properties");
    }

    public boolean enabled() {
        return properties.enabled();
    }

    public int pollIntervalMillis() {
        return properties.pollIntervalMillis();
    }

    public int consecutiveStableChecks() {
        return properties.consecutiveStableChecks();
    }

    public Snapshot sample(Path path) throws IOException {
        BasicFileAttributes attributes = Files.readAttributes(path, BasicFileAttributes.class);
        if (!attributes.isRegularFile()) {
            throw new IOException("Source path is not a regular file: " + path);
        }
        return new Snapshot(attributes.size(), attributes.lastModifiedTime());
    }

    public boolean unchanged(Snapshot previous, Snapshot current) {
        return previous != null && previous.equals(current);
    }

    public record Snapshot(long size, FileTime lastModifiedTime) { }
}
