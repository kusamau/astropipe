package io.astropipe;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;

/**
 * Responsible to create the requested directory structure for each archived file.
 * Maps an image timestamp onto {@code <archive>/<Year>/<Month>/<Day>}, creating it on first use.
 **/
final class ArchivePathResolver {
    private final Path archiveRoot;

    ArchivePathResolver(Path archiveRoot) {
        this.archiveRoot = archiveRoot.toAbsolutePath().normalize();
    }

    /** Safe to call concurrently: creating a directory that already exists, or that another caller is creating, succeeds. */
    Path resolve(SatelliteImageName image) throws IOException {
        LocalDate date = image.timestamp().toLocalDate();
        Path directory = archiveRoot
                .resolve("%04d".formatted(date.getYear()))
                .resolve("%02d".formatted(date.getMonthValue()))
                .resolve("%02d".formatted(date.getDayOfMonth()));
        try {
            Files.createDirectories(directory);
        } catch (IOException e) {
            throw new IOException("Could not create archive directory " + directory, e);
        }
        return directory.resolve(image.fileName());
    }
}
