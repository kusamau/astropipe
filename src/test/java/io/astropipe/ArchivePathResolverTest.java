package io.astropipe;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ArchivePathResolverTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void buildsZeroPaddedDateHierarchyAndCreatesIt() throws Exception {
        Path archive = temporaryDirectory.resolve("archive");
        Files.createDirectory(archive);
        ArchivePathResolver resolver = new ArchivePathResolver(archive);

        Path result = resolver.resolve(SatelliteImageName.parse("SAT_IMG_20210102030405006.raw"));

        assertEquals(archive.resolve("2021/01/02/SAT_IMG_20210102030405006.raw").toAbsolutePath(), result);
        assertTrue(Files.isDirectory(archive.resolve("2021/01/02")));
    }

    @Test
    void resolvesBothSidesOfYearBoundary() throws Exception {
        Path archive = temporaryDirectory.resolve("archive");
        Files.createDirectory(archive);
        ArchivePathResolver resolver = new ArchivePathResolver(archive);

        assertTrue(resolver.resolve(SatelliteImageName.parse("SAT_IMG_20211231235959999.raw"))
                .toString().contains("2021/12/31"));
        assertTrue(resolver.resolve(SatelliteImageName.parse("SAT_IMG_20220101000000000.raw"))
                .toString().contains("2022/01/01"));
    }
}
