package io.astropipe;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.WatchService;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Detection is independent of the archive, so none of this needs a database. */
class SourceDetectorTest {
    @TempDir
    Path temporaryDirectory;

    private final Set<Path> detected = ConcurrentHashMap.newKeySet();
    private final List<SourceDetector.Detection> detections = new CopyOnWriteArrayList<>();
    private SourceDetector detector;

    @AfterEach
    void tearDown() {
        if (detector != null) detector.close();
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
    void endsItsListingThreadWhenClosed() throws Exception {
        start();

        detector.close();

        // Left running, it would be a non-daemon thread keeping the JVM alive.
        Await.until("the listing thread has ended", () -> Thread.getAllStackTraces().keySet().stream()
                .noneMatch(thread -> thread.getName().startsWith("reconciliation-")));
    }

    @Test
    void labelsWhatWasAlreadyThereAsTheStartupListing() throws Exception {
        Files.writeString(temporaryDirectory.resolve("SAT_IMG_20210714102730046.raw"), "x");

        start();

        assertEquals(List.of(SourceDetector.Detection.STARTUP_LISTING), detections);
    }

    @Test
    void reportsAFileLeftInPlaceAgainOnEveryPeriodicListing() throws Exception {
        // A file the pipeline will refuse stays put, so every later listing finds it again.
        Files.writeString(temporaryDirectory.resolve("not-an-image.txt"), "x");

        start();

        Await.until("a periodic listing re-reports the file",
                () -> detections.contains(SourceDetector.Detection.PERIODIC_LISTING));
    }

    @Test
    void reportsANewFileThroughTheWatchBeforeAnyListing() throws Exception {
        // Where the JDK cannot create a WatchService (on Linux, when the user's inotify instances
        // are used up), detection falls back to listing and this proves nothing, so it is skipped.
        assumeTrue(watchServiceAvailable(), "no WatchService can be created here");
        start(3600);

        Path appeared = Files.writeString(temporaryDirectory.resolve("SAT_IMG_20220101000000000.raw"), "x");

        Await.until("the watch reports the new file", () -> detected.contains(appeared));
        assertEquals(List.of(SourceDetector.Detection.FILE_CREATED), detections);
    }

    private void start() {
        start(1);
    }

    private void start(int listingIntervalSeconds) {
        detector = new SourceDetector(temporaryDirectory, listingIntervalSeconds, (path, detection) -> {
            detected.add(path);
            detections.add(detection);
        });
        detector.start();
    }

    private boolean watchServiceAvailable() {
        try (WatchService probe = temporaryDirectory.getFileSystem().newWatchService()) {
            return true;
        } catch (IOException unavailable) {
            return false;
        }
    }

}
