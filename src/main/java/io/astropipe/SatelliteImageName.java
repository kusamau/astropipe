package io.astropipe;

import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A parsed {@code SAT_IMG_<yyyyMMddHHmmssSSS>.raw} name.
 *
 * <p>The timestamp is not a separate fact about the file, it is carried by the name, so
 * the two travel together as one value. Callers that need both take this rather than a
 * name and a timestamp side by side, which could disagree.
 */
record SatelliteImageName(String fileName, LocalDateTime timestamp) {
    private static final Pattern FILE_NAME = Pattern.compile("SAT_IMG_(\\d{17})\\.raw");
    private static final DateTimeFormatter TIMESTAMP =
            DateTimeFormatter.ofPattern("uuuuMMddHHmmssSSS").withResolverStyle(ResolverStyle.STRICT);

    static SatelliteImageName parse(Path path) {
        return parse(path.getFileName().toString());
    }

    static SatelliteImageName parse(String fileName) {
        Matcher matcher = FILE_NAME.matcher(fileName);
        if (!matcher.matches()) {
            throw new InvalidNameException("File name does not match SAT_IMG_<timestamp>.raw: " + fileName);
        }
        try {
            return new SatelliteImageName(fileName, LocalDateTime.parse(matcher.group(1), TIMESTAMP));
        } catch (DateTimeParseException e) {
            throw new InvalidNameException("File name contains an invalid timestamp: " + fileName, e);
        }
    }

    /** Where the image belongs in the archive: {@code <root>/<yyyy>/<MM>/<dd>/<name>}. */
    Path archivePath(Path archiveRoot) {
        return archiveRoot.resolve("%04d".formatted(timestamp.getYear()))
                .resolve("%02d".formatted(timestamp.getMonthValue()))
                .resolve("%02d".formatted(timestamp.getDayOfMonth()))
                .resolve(fileName);
    }

    static final class InvalidNameException extends IllegalArgumentException {
        InvalidNameException(String message) {
            super(message);
        }

        InvalidNameException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
