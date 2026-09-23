package io.astropipe;

import io.astropipe.exception.InvalidSatelliteImageNameException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FileNameParserTest {
    private final FileNameParser parser = new FileNameParser();

    @Test
    void parsesTimestampFromValidFileName() {
        assertEquals(LocalDateTime.of(2021, 7, 14, 10, 27, 30, 46_000_000),
                parser.parse(Path.of("SAT_IMG_20210714102730046.raw")));
    }

    @Test
    void acceptsLeapDay() {
        assertEquals(LocalDateTime.of(2024, 2, 29, 12, 0),
                parser.parse(Path.of("SAT_IMG_20240229120000000.raw")));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "image.raw",
            "SAT_IMG_2021071410273004.raw",
            "SAT_IMG_20210714102730046.jpg",
            "SAT_IMG_20210229120000000.raw",
            "SAT_IMG_20211301120000000.raw",
            "SAT_IMG_20210714242730046.raw"
    })
    void rejectsMalformedNamesAndImpossibleDates(String name) {
        assertThrows(InvalidSatelliteImageNameException.class, () -> parser.parse(Path.of(name)));
    }
}
