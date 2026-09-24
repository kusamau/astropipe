package io.astropipe;

import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.util.regex.Matcher;

import java.util.regex.Pattern;

/** Parses the {@code SAT_IMG_<yyyyMMddHHmmssSSS>.raw} naming convention. */
final class SatelliteImageName {
    private static final Pattern FILE_NAME = Pattern.compile("SAT_IMG_(\\d{17})\\.raw");
    private static final DateTimeFormatter TIMESTAMP =
            DateTimeFormatter.ofPattern("uuuuMMddHHmmssSSS").withResolverStyle(ResolverStyle.STRICT);

    private SatelliteImageName() { }

    static LocalDateTime parse(Path path) {
        return parse(path.getFileName().toString());
    }

    static LocalDateTime parse(String fileName) {
        Matcher matcher = FILE_NAME.matcher(fileName);
        if (!matcher.matches()) {
            throw new InvalidNameException("File name does not match SAT_IMG_<timestamp>.raw: " + fileName);
        }
        try {
            return LocalDateTime.parse(matcher.group(1), TIMESTAMP);
        } catch (DateTimeParseException e) {
            throw new InvalidNameException("File name contains an invalid timestamp: " + fileName, e);
        }
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
