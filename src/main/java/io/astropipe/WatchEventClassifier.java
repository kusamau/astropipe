package io.astropipe;

import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;

/**
 * Turns a raw watch event into the detector's own vocabulary: an overflow, a new file,
 * or a changed one. It only says what an event is; what to do about it is
 * {@link SourceDetector}'s decision.
 *
 * <p>{@link StandardWatchEventKinds}' constants are plain {@code WatchEvent.Kind} instances,
 * not an enum, so a guard stands in for the case label a real enum switch would allow.
 */
final class WatchEventClassifier {
    private final Path sourceDirectory;

    WatchEventClassifier(Path sourceDirectory) {
        this.sourceDirectory = sourceDirectory;
    }

    Classification classify(WatchEvent<?> event) {
        return switch (event.kind()) {
            case WatchEvent.Kind<?> kind when kind == StandardWatchEventKinds.OVERFLOW -> new Overflow();
            case WatchEvent.Kind<?> kind when kind == StandardWatchEventKinds.ENTRY_CREATE ->
                    fileEvent(event, SourceDetector.Detection.FILE_CREATED);
            case WatchEvent.Kind<?> kind when kind == StandardWatchEventKinds.ENTRY_MODIFY ->
                    fileEvent(event, SourceDetector.Detection.FILE_MODIFIED);
            default -> new Unexpected(event.kind());
        };
    }

    /** Only entry events carry a context: the file's name relative to the watched directory. */
    private FileEvent fileEvent(WatchEvent<?> event, SourceDetector.Detection detection) {
        return new FileEvent(sourceDirectory.resolve((Path) event.context()), detection);
    }

    sealed interface Classification permits Overflow, FileEvent, Unexpected { }

    /** The kernel dropped events; only a fresh listing can tell which files they were about. */
    record Overflow() implements Classification { }

    record FileEvent(Path path, SourceDetector.Detection detection) implements Classification { }

    /** A kind the detector never registered for. */
    record Unexpected(WatchEvent.Kind<?> kind) implements Classification { }
}
