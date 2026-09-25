package io.astropipe;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Detection is independent of the archive, so none of this needs a database. */
class SourceDetectorTest {
    @TempDir
    Path temporaryDirectory;

    private final Set<Path> detected = ConcurrentHashMap.newKeySet();
    private final List<SourceDetector.Detection> detections = new CopyOnWriteArrayList<>();
    private ScheduledExecutorService scheduler;
    private SourceDetector detector;

    @AfterEach
    void tearDown() {
        if (detector != null) detector.close();
        if (scheduler != null) scheduler.shutdownNow();
    }

    @Test
    void reportsFilesAlreadyPresentWhenItStarts() throws Exception {
        Path existing = Files.writeString(temporaryDirectory.resolve("SAT_IMG_20210714102730046.raw"), "x");

        start();

        assertTrue(detected.contains(existing));
    }

    @Test
    void reportsFilesThatAppearAfterItStarts() throws Exception {
        start();

        Path appeared = Files.writeString(temporaryDirectory.resolve("SAT_IMG_20220101000000000.raw"), "x");

        Await.until("the new file is reported", () -> detected.contains(appeared));
    }

    @Test
    void leavesNameInterpretationToTheCaller() throws Exception {
        Path malformed = Files.writeString(temporaryDirectory.resolve("not-an-image.txt"), "x");

        start();

        assertTrue(detected.contains(malformed), "the detector reports every regular file");
    }

    @Test
    void ignoresDirectories() throws Exception {
        Files.createDirectory(temporaryDirectory.resolve("staging"));

        start();

        assertEquals(Set.of(), detected);
    }

    @Test
    void stopsReportingOnceClosed() throws Exception {
        start();
        detector.close();

        Files.writeString(temporaryDirectory.resolve("SAT_IMG_20220101000000000.raw"), "x");
        TimeUnit.MILLISECONDS.sleep(300);

        assertEquals(Set.of(), detected);
    }

    @Test
    void reportsTheStartupListingAsSomethingWorthSayingOnce() throws Exception {
        Files.writeString(temporaryDirectory.resolve("SAT_IMG_20210714102730046.raw"), "x");

        start();

        assertEquals(List.of(SourceDetector.Detection.STARTUP_LISTING), detections);
        assertTrue(SourceDetector.Detection.STARTUP_LISTING.firstSighting());
    }

    @Test
    void reportsRepeatListingsAsNothingNew() throws Exception {
        // A file the pipeline will refuse stays put, so every later listing finds it again.
        Files.writeString(temporaryDirectory.resolve("not-an-image.txt"), "x");

        start();

        Await.until("a periodic listing re-reports the file",
                () -> detections.contains(SourceDetector.Detection.PERIODIC_LISTING));
        assertFalse(SourceDetector.Detection.PERIODIC_LISTING.firstSighting(),
                "a re-scan is not news; treating it as such warns on every sweep, forever");
        assertFalse(SourceDetector.Detection.OVERFLOW_LISTING.firstSighting());
    }

    private void start() {
        scheduler = Executors.newSingleThreadScheduledExecutor();
        detector = new SourceDetector(temporaryDirectory, scheduler, 1, (path, detection) -> {
            detected.add(path);
            detections.add(detection);
        });
        detector.start();
    }

}
