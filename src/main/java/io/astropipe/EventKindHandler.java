package io.astropipe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.util.function.BiConsumer;

/**
 * Turns a raw watch event into the detector's own vocabulary: an overflow, a new file,
 * or a changed one. {@link StandardWatchEventKinds}' constants are plain
 * {@code WatchEvent.Kind} instances, not an enum, so a guard stands in for the case
 * label a real enum switch would allow.
 */
final class EventKindHandler {
    private static final Logger logger = LoggerFactory.getLogger(EventKindHandler.class);

    private final Path sourceDirectory;
    private final BiConsumer<Path, SourceDetector.Detection> onDetected;
    private final Runnable onOverflow;

    EventKindHandler(
            Path sourceDirectory,
            BiConsumer<Path, SourceDetector.Detection> onDetected,
            Runnable onOverflow) {
        this.sourceDirectory = sourceDirectory;
        this.onDetected = onDetected;
        this.onOverflow = onOverflow;
    }

    void handle(WatchEvent<?> event) {
        switch (event.kind()) {
            case WatchEvent.Kind<?> kind when kind == StandardWatchEventKinds.OVERFLOW -> onOverflow.run();
            case WatchEvent.Kind<?> kind when kind == StandardWatchEventKinds.ENTRY_CREATE ->
                    report(event, SourceDetector.Detection.FILE_CREATED);
            case WatchEvent.Kind<?> kind when kind == StandardWatchEventKinds.ENTRY_MODIFY ->
                    report(event, SourceDetector.Detection.FILE_MODIFIED);
            default -> logger.warn("Ignoring unexpected watch event kind={}", event.kind());
        }
    }

    private void report(WatchEvent<?> event, SourceDetector.Detection detection) {
        Path path = sourceDirectory.resolve((Path) event.context());
        onDetected.accept(path.toAbsolutePath().normalize(), detection);
    }
}
