package io.astropipe;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;

import static org.junit.jupiter.api.Assertions.assertEquals;

class WatchEventClassifierTest {
    private final Path sourceDirectory = Path.of("/data/source");
    private final WatchEventClassifier classifier = new WatchEventClassifier(sourceDirectory);

    @Test
    void classifiesAnOverflowWithoutLookingForAFile() {
        // An overflow has no context: reading one as a file name would fail on null.
        assertEquals(new WatchEventClassifier.Overflow(),
                classifier.classify(event(StandardWatchEventKinds.OVERFLOW, null)));
    }

    @Test
    void resolvesACreatedEntryAgainstTheWatchedDirectory() {
        assertEquals(
                new WatchEventClassifier.FileEvent(
                        sourceDirectory.resolve("SAT_IMG_20210714102730046.raw"),
                        SourceDetector.Detection.FILE_CREATED),
                classifier.classify(event(
                        StandardWatchEventKinds.ENTRY_CREATE, Path.of("SAT_IMG_20210714102730046.raw"))));
    }

    @Test
    void resolvesAModifiedEntryAgainstTheWatchedDirectory() {
        assertEquals(
                new WatchEventClassifier.FileEvent(
                        sourceDirectory.resolve("SAT_IMG_20210714102730046.raw"),
                        SourceDetector.Detection.FILE_MODIFIED),
                classifier.classify(event(
                        StandardWatchEventKinds.ENTRY_MODIFY, Path.of("SAT_IMG_20210714102730046.raw"))));
    }

    @Test
    void setsAsideKindsItNeverRegisteredFor() {
        assertEquals(new WatchEventClassifier.Unexpected(StandardWatchEventKinds.ENTRY_DELETE),
                classifier.classify(event(StandardWatchEventKinds.ENTRY_DELETE, Path.of("gone.raw"))));
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
