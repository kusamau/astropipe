package io.astropipe;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Responsible to create the requested directory structure for each archived file.
 * Maps an image timestamp onto {@code <archive>/<Year>/<Month>/<Day>}, creating it on first use.
 **/
final class ArchivePathResolver {
    private final Path archiveRoot;
    private final Set<Path> preparedDirectories = ConcurrentHashMap.newKeySet();

    ArchivePathResolver(Path archiveRoot) {
        this.archiveRoot = archiveRoot.toAbsolutePath().normalize();
    }

    Path resolve(SatelliteImageName image) throws IOException {
        LocalDate date = image.timestamp().toLocalDate();
        Path directory = archiveRoot
                .resolve("%04d".formatted(date.getYear()))
                .resolve("%02d".formatted(date.getMonthValue()))
                .resolve("%02d".formatted(date.getDayOfMonth()));
        if (preparedDirectories.add(directory)) {
            try {
                Files.createDirectories(directory);
            } catch (IOException e) {
                preparedDirectories.remove(directory);
                throw new IOException("Could not create archive directory " + directory, e);
            }
        }
        return directory.resolve(image.fileName());
    }
}
