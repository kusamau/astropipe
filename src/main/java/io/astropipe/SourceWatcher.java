package io.astropipe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

/**
 * Hands each newly detected image to an {@link ImageTransfer} running on a thread of its own.
 * Finding files is {@link SourceDetector}'s job. The only state kept here is which images are
 * already in flight and which malformed entries have already been warned about.
 */
final class SourceWatcher implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(SourceWatcher.class);

    private final ImageTransfer transfer;
    private final Executor transfers;
    private final SourceDetector detector;
    private final Set<Path> inFlight = ConcurrentHashMap.newKeySet();
    private final Set<Path> warnedMalformed = ConcurrentHashMap.newKeySet();

    SourceWatcher(
            Path sourceDirectory,
            Config config,
            ArchivePathResolver pathResolver,
            FileMover fileMover,
            ArchiveRepository repository,
            Executor transfers) {
        this.transfer = new ImageTransfer(config, pathResolver, fileMover, repository);
        this.transfers = transfers;
        this.detector = new SourceDetector(sourceDirectory, config.reconciliationIntervalSeconds(), this::onDetected);
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
