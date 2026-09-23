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
        StabilityDetector detector = new StabilityDetector(new StabilityProperties(true, 10, 2));
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
        StabilityDetector detector = new StabilityDetector(new StabilityProperties(false, 75, 4));
        assertFalse(detector.enabled());
        assertTrue(detector.pollIntervalMillis() == 75 && detector.consecutiveStableChecks() == 4);
    }
}
