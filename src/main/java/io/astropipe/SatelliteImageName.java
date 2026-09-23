package io.astropipe;

import io.astropipe.exception.InvalidSatelliteImageNameException;

import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class SatelliteImageName {
    private static final String PREFIX = "SAT_IMG_";
    private static final String EXTENSION = ".raw";
    private static final String TEMPORARY_SUFFIX = ".tmp";
    private static final Pattern FILE_NAME = Pattern.compile(
            Pattern.quote(PREFIX) + "(\\d{17})" + Pattern.quote(EXTENSION));
    private static final DateTimeFormatter TIMESTAMP =
            DateTimeFormatter.ofPattern("uuuuMMddHHmmssSSS").withResolverStyle(ResolverStyle.STRICT);

    private SatelliteImageName() { }

    static LocalDateTime parse(Path path) {
        return parse(path.getFileName().toString());
    }

    static boolean isArchiveTemporaryFile(Path path) {
        String fileName = path.getFileName().toString();
        if (!fileName.endsWith(TEMPORARY_SUFFIX)) return false;
        String publishedName = fileName.substring(0, fileName.length() - TEMPORARY_SUFFIX.length());
        try {
            parse(publishedName);
            return true;
        } catch (InvalidSatelliteImageNameException invalid) {
            return false;
        }
    }

    private static LocalDateTime parse(String fileName) {
        Matcher matcher = FILE_NAME.matcher(fileName);
        if (!matcher.matches()) {
            throw new InvalidSatelliteImageNameException(
                    "File name does not match SAT_IMG_<timestamp>.raw: " + fileName);
        }
        try {
            return LocalDateTime.parse(matcher.group(1), TIMESTAMP);
        } catch (DateTimeParseException e) {
            throw new InvalidSatelliteImageNameException(
                    "File name contains an invalid timestamp: " + fileName, e);
        }
    }
}
