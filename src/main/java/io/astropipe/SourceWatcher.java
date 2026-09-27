package io.astropipe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;

/**
 * Hands each newly detected image to an {@link ImageTransfer} running on a virtual thread of its
 * own. Finding files is {@link SourceDetector}'s job. The only state kept here is which images
 * are already in flight and which malformed entries have already been warned about.
 *
 * <p>{@link #close()} interrupts the transfers, stops detection and waits for the transfers to
 * end, all before the caller closes the database underneath them. Constructing one starts
 * nothing, so a caller can hold it before {@link #start()} runs and still be sure {@code close()}
 * will take down whatever starting managed to build.
 */
final class SourceWatcher implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(SourceWatcher.class);

    private final ImageTransfer transfer;
    private final ExecutorService transfers =
            Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("transfer-", 1).factory());
    private final SourceDetector detector;
    private final Set<Path> inFlight = ConcurrentHashMap.newKeySet();
    private final Set<Path> warnedMalformed = ConcurrentHashMap.newKeySet();

    SourceWatcher(
            Path sourceDirectory,
            Config config,
            Path archiveRoot,
            FileMover fileMover,
            ArchiveRepository repository) {
        this.transfer = new ImageTransfer(config, archiveRoot, fileMover, repository);
        this.detector = new SourceDetector(sourceDirectory, config.reconciliationIntervalSeconds(), this::onDetected);
    }

    /** Opens the watch and starts reporting. Whatever this half-builds, {@link #close()} takes down. */
    void start() {
        detector.start();
    }

    /** Blocks until something asks it to stop, or the watch ends by itself. */
    void awaitTermination() throws InterruptedException {
        detector.awaitTermination();
    }

    /** Stops detection without waiting, which is all a signal handler should do. */
    void requestStop() {
        detector.close();
    }

    @Override
    public void close() {
        // Interrupt rather than drain: an interrupted transfer leaves its image in progress
        // for startup reconciliation, so waiting out a long copy would buy nothing. The
        // resources then close in reverse order, and the language still closes the rest if
        // one of them throws on the way out.
        try (transfers; detector) {
            transfers.shutdownNow();
        }
    }

    /** Called on whichever thread found the file: the watcher, a listing, or startup. */
    private void onDetected(Path source, SourceDetector.Detection detection) {
        SatelliteImageName image;
        try {
            image = SatelliteImageName.parse(source);
        } catch (SatelliteImageName.InvalidNameException invalid) {
            // A name that cannot be parsed is never claimed, so every later listing meets it
            // again. Warn the first time this run meets it, whatever found it, and stay quiet
            // after: otherwise one stray file warns on every sweep for the life of the process.
            logger.atLevel(warnedMalformed.add(source) ? Level.WARN : Level.DEBUG)
                    .log("Ignoring malformed source entry={} detectedBy={} reason={}",
                            source, detection, invalid.getMessage());
            return;
        }
        if (!inFlight.add(source)) return;
        try {
            transfers.execute(() -> {
                try {
                    transfer.archive(source, image, detection);
                } finally {
                    inFlight.remove(source);
                }
            });
        } catch (RejectedExecutionException shuttingDown) {
            inFlight.remove(source);
        }
    }
}
