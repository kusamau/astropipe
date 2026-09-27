package io.astropipe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Decides what each raw watch event leads to: a new file is reported to the pipeline,
 * and an overflow is answered with an immediate listing, since only a listing can
 * tell which files the lost events were about. {@link SourceDetector} supplies the means —
 * the sink, the scheduler and its listing — and every decision is made here.
 *
 * <p>{@link StandardWatchEventKinds}' constants are plain {@code WatchEvent.Kind} instances,
 * not an enum, so a guard stands in for the case label a real enum switch would allow.
 */
final class EventKindHandler {
    private static final Logger logger = LoggerFactory.getLogger(EventKindHandler.class);

    private final Path sourceDirectory;
    private final BiConsumer<Path, SourceDetector.Detection> onDetected;
    private final Executor scheduler;
    private final Consumer<SourceDetector.Detection> sweep;

    EventKindHandler(
            Path sourceDirectory,
            BiConsumer<Path, SourceDetector.Detection> onDetected,
            Executor scheduler,
            Consumer<SourceDetector.Detection> sweep) {
        this.sourceDirectory = sourceDirectory;
        this.onDetected = onDetected;
        this.scheduler = scheduler;
        this.sweep = sweep;
    }

    void handle(WatchEvent<?> event) {
        switch (event.kind()) {
            case WatchEvent.Kind<?> kind when kind == StandardWatchEventKinds.OVERFLOW -> onOverflow();
            case WatchEvent.Kind<?> kind when kind == StandardWatchEventKinds.ENTRY_CREATE -> onCreated(event);
            default -> logger.warn("Ignoring unexpected watch event kind={}", event.kind());
        }
    }

    /** On the scheduler, not inline: the watcher thread has to get back to draining events. */
    private void onOverflow() {
        logger.warn("WatchService overflow detected; scheduling an immediate listing");
        scheduler.execute(() -> sweep.accept(SourceDetector.Detection.OVERFLOW_LISTING));
    }

    /**
     * A created entry's context is its name relative to the watched directory. Only regular
     * files are reported, as the listings report: a directory is not an image.
     */
    private void onCreated(WatchEvent<?> event) {
        Path created = sourceDirectory.resolve((Path) event.context());
        if (Files.isRegularFile(created)) {
            onDetected.accept(created, SourceDetector.Detection.FILE_CREATED);
        }
    }
}
