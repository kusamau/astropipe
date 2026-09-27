package io.astropipe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Hands each newly detected image to an {@link ImageTransfer} running on a thread of its own.
 * Finding files is {@link SourceDetector}'s job; the only state shared between images is
 * which of them are already in flight.
 */
final class SourceWatcher implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(SourceWatcher.class);

    private final ImageTransfer transfer;
    private final Executor transfers;
    private final SourceDetector detector;
    private final Set<Path> inFlight = ConcurrentHashMap.newKeySet();

    SourceWatcher(
            Path sourceDirectory,
            Config config,
            ArchivePathResolver pathResolver,
            FileMover fileMover,
            ArchiveRepository repository,
            Executor transfers,
            ScheduledExecutorService scheduler) {
        this.transfer = new ImageTransfer(config, pathResolver, fileMover, repository);
        this.transfers = transfers;
        this.detector = new SourceDetector(
                sourceDirectory, scheduler, config.reconciliationIntervalSeconds(), this::onDetected);
    }

    void start() {
        detector.start();
    }

    void awaitTermination() throws InterruptedException {
        detector.awaitTermination();
    }

    /** Stops detection only: whoever owns the transfer executor stops the transfers. */
    @Override
    public void close() {
        detector.close();
    }

    /** Called on whichever thread found the file: the watcher, a listing, or startup. */
    private void onDetected(Path source, SourceDetector.Detection detection) {
        SatelliteImageName image;
        try {
            image = SatelliteImageName.parse(source);
        } catch (SatelliteImageName.InvalidNameException invalid) {
            // A name that cannot be parsed is never claimed, so every later listing meets
            // it as though it were new. Say so once, when this could be the first sighting,
            // and stay quiet on the re-scans: otherwise one stray file in the source
            // directory warns on every sweep for the life of the process.
            logger.atLevel(detection.firstSighting() ? Level.WARN : Level.DEBUG)
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
