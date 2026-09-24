package io.astropipe;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Detection is independent of the archive, so none of this needs a database. */
class SourceDetectorTest {
    @TempDir
    Path temporaryDirectory;

    private final Set<Path> detected = ConcurrentHashMap.newKeySet();
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

        await(() -> detected.contains(appeared));
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

    private void start() {
        scheduler = Executors.newSingleThreadScheduledExecutor();
        detector = new SourceDetector(temporaryDirectory, scheduler, 1, (path, reason) -> detected.add(path));
        detector.start();
    }

    private static void await(BooleanSupplierWithException condition) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.get()) return;
            TimeUnit.MILLISECONDS.sleep(20);
        }
        assertTrue(condition.get(), "condition was not met within 5s");
    }

    private interface BooleanSupplierWithException {
        boolean get() throws Exception;
    }
}
