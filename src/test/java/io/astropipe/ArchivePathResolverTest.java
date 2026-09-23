package io.astropipe;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;

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

        Path result = resolver.resolve(
                "SAT_IMG_20210102030405006.raw",
                LocalDateTime.of(2021, 1, 2, 3, 4, 5, 6_000_000));

        assertEquals(archive.resolve("2021/01/02/SAT_IMG_20210102030405006.raw").toAbsolutePath(), result);
        assertTrue(Files.isDirectory(archive.resolve("2021/01/02")));
    }

    @Test
    void resolvesBothSidesOfYearBoundary() throws Exception {
        Path archive = temporaryDirectory.resolve("archive");
        Files.createDirectory(archive);
        ArchivePathResolver resolver = new ArchivePathResolver(archive);

        assertTrue(resolver.resolve("end.raw", LocalDateTime.of(2021, 12, 31, 23, 59))
                .toString().contains("2021/12/31"));
        assertTrue(resolver.resolve("start.raw", LocalDateTime.of(2022, 1, 1, 0, 0))
                .toString().contains("2022/01/01"));
    }
}
