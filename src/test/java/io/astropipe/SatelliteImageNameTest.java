package io.astropipe;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SatelliteImageNameTest {
    @Test
    void parsesTimestampFromValidFileName() {
        SatelliteImageName image = SatelliteImageName.parse(Path.of("SAT_IMG_20210714102730046.raw"));

        assertEquals("SAT_IMG_20210714102730046.raw", image.fileName());
        assertEquals(LocalDateTime.of(2021, 7, 14, 10, 27, 30, 46_000_000), image.timestamp());
    }

    @Test
    void carriesTheNameAndItsTimestampAsOneValue() {
        // The pair cannot disagree: parsing is the only way to obtain one.
        assertEquals(SatelliteImageName.parse("SAT_IMG_20240229120000000.raw"),
                SatelliteImageName.parse(Path.of("/anywhere/SAT_IMG_20240229120000000.raw")));
    }

    @Test
    void acceptsLeapDay() {
        assertEquals(LocalDateTime.of(2024, 2, 29, 12, 0),
                SatelliteImageName.parse("SAT_IMG_20240229120000000.raw").timestamp());
    }

    @Test
    void placesTheImageUnderItsZeroPaddedDate() {
        Path archive = Path.of("/archive");

        assertEquals(archive.resolve("2021/01/02/SAT_IMG_20210102030405006.raw"),
                SatelliteImageName.parse("SAT_IMG_20210102030405006.raw").archivePath(archive));
    }

    @Test
    void placesEachSideOfAYearBoundaryUnderItsOwnYear() {
        Path archive = Path.of("/archive");

        assertEquals(archive.resolve("2021/12/31/SAT_IMG_20211231235959999.raw"),
                SatelliteImageName.parse("SAT_IMG_20211231235959999.raw").archivePath(archive));
        assertEquals(archive.resolve("2022/01/01/SAT_IMG_20220101000000000.raw"),
                SatelliteImageName.parse("SAT_IMG_20220101000000000.raw").archivePath(archive));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "image.raw",
            "SAT_IMG_2021071410273004.raw",
            "SAT_IMG_20210714102730046.jpg",
            "SAT_IMG_20210714102730046.raw.tmp",
            "SAT_IMG_20210229120000000.raw",
            "SAT_IMG_20211301120000000.raw",
            "SAT_IMG_20210714242730046.raw"
    })
    void rejectsMalformedNamesAndImpossibleDates(String name) {
        assertThrows(SatelliteImageName.InvalidNameException.class, () -> SatelliteImageName.parse(name));
    }
}
