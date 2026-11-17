package io.astropipe;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StabilityDetectorTest {
    @TempDir
    Path temporaryDirectory;

    private final StabilityDetector detector = new StabilityDetector(200);

    @Test
    void comparesBothSizeAndModificationTime() throws Exception {
        Path file = Files.writeString(temporaryDirectory.resolve("file.raw"), "first");
        StabilityDetector.Snapshot first = detector.sample(file);
        assertTrue(detector.unchanged(first, detector.sample(file)));

        Files.writeString(file, "second-write");
        assertFalse(detector.unchanged(first, detector.sample(file)));
    }

    @Test
    void treatsAMissingPreviousSnapshotAsChanged() throws Exception {
        Path file = Files.writeString(temporaryDirectory.resolve("file.raw"), "first");

        assertFalse(detector.unchanged(null, detector.sample(file)));
    }

    @Test
    void reportsQuietOnlyAfterTheFullQuietPeriod() {
        assertFalse(detector.quietFor(1_000_000L, 200_999_999L));
        assertTrue(detector.quietFor(1_000_000L, 201_000_000L));
    }
}
