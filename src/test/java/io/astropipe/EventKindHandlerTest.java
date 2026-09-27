package io.astropipe;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class EventKindHandlerTest {
    @TempDir
    Path sourceDirectory;

    private final List<Path> reported = new ArrayList<>();
    private final List<SourceDetector.Detection> detections = new ArrayList<>();
    private final List<Runnable> scheduled = new ArrayList<>();
    private final List<SourceDetector.Detection> sweeps = new ArrayList<>();
    private EventKindHandler handler;

    @BeforeEach
    void createHandler() {
        handler = new EventKindHandler(
                sourceDirectory,
                (path, detection) -> {
                    reported.add(path);
                    detections.add(detection);
                },
                scheduled::add,
                sweeps::add);
    }

    @Test
    void reportsACreatedFileResolvedAgainstTheWatchedDirectory() throws Exception {
        Files.createFile(sourceDirectory.resolve("SAT_IMG_20210714102730046.raw"));

        handler.handle(event(StandardWatchEventKinds.ENTRY_CREATE, Path.of("SAT_IMG_20210714102730046.raw")));

        assertEquals(List.of(sourceDirectory.resolve("SAT_IMG_20210714102730046.raw")), reported);
        assertEquals(List.of(SourceDetector.Detection.FILE_CREATED), detections);
    }

    @Test
    void ignoresACreatedDirectory() throws Exception {
        // A producer's staging directory, for instance: the listings skip directories, and so does the watch.
        Files.createDirectory(sourceDirectory.resolve(".staging"));

        handler.handle(event(StandardWatchEventKinds.ENTRY_CREATE, Path.of(".staging")));

        assertEquals(List.of(), reported);
    }

    @Test
    void answersAnOverflowWithAListingOnTheScheduler() {
        // An overflow has no context: reading one as a file name would fail on null.
        handler.handle(event(StandardWatchEventKinds.OVERFLOW, null));

        assertEquals(List.of(), sweeps, "the listing must not run on the watcher thread");
        assertEquals(1, scheduled.size());
        scheduled.getFirst().run();
        assertEquals(List.of(SourceDetector.Detection.OVERFLOW_LISTING), sweeps);
        assertEquals(List.of(), reported);
    }

    @Test
    void ignoresKindsItNeverRegisteredFor() {
        handler.handle(event(StandardWatchEventKinds.ENTRY_DELETE, Path.of("gone.raw")));

        assertEquals(List.of(), reported);
        assertEquals(List.of(), scheduled);
    }

    private static <T> WatchEvent<T> event(WatchEvent.Kind<T> kind, T context) {
        return new WatchEvent<>() {
            @Override
            public WatchEvent.Kind<T> kind() {
                return kind;
            }

            @Override
            public int count() {
                return 1;
            }

            @Override
            public T context() {
                return context;
            }
        };
    }
}
