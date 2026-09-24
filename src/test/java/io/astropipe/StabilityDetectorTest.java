package io.astropipe;

import io.astropipe.config.props.StabilityProperties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StabilityDetectorTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void comparesBothSizeAndModificationTime() throws Exception {
        StabilityDetector detector = new StabilityDetector(new StabilityProperties(10, 20, 64));
        Path file = temporaryDirectory.resolve("file.raw");
        Files.writeString(file, "first");
        StabilityDetector.Snapshot first = detector.sample(file);
        StabilityDetector.Snapshot unchanged = detector.sample(file);
        assertTrue(detector.unchanged(first, unchanged));

        Files.writeString(file, "second-write");
        StabilityDetector.Snapshot changed = detector.sample(file);
        assertFalse(detector.unchanged(unchanged, changed));
    }

    @Test
    void reportsConfiguredPolicy() {
        StabilityDetector detector = new StabilityDetector(new StabilityProperties(75, 200, 4096));

        assertTrue(detector.pollIntervalMillis() == 75
                && detector.quietPeriodMillis() == 200
                && detector.resumeOverlapBytes() == 4096);
        assertFalse(detector.quietFor(1_000_000L, 200_999_999L));
        assertTrue(detector.quietFor(1_000_000L, 201_000_000L));
    }
}
