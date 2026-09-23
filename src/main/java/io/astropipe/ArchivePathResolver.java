package io.astropipe;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public final class ArchivePathResolver {
    private final Path archiveRoot;
    private final Set<Path> preparedDirectories = ConcurrentHashMap.newKeySet();

    public ArchivePathResolver(Path archiveRoot) {
        this.archiveRoot = archiveRoot.toAbsolutePath().normalize();
    }

    public Path resolve(String fileName, LocalDateTime timestamp) throws IOException {
        LocalDate date = timestamp.toLocalDate();
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
        return directory.resolve(fileName);
    }
}
