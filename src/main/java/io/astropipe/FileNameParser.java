package io.astropipe;

import java.nio.file.Path;
import java.time.LocalDateTime;

public final class FileNameParser {
    public LocalDateTime parse(Path path) {
        return SatelliteImageName.parse(path);
    }
}
