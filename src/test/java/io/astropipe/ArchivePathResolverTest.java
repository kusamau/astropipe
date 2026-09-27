package io.astropipe;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

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

    @Test
    void everyCallerGetsAnExistingDirectoryEvenWhenTheyRaceForTheSameDay() throws Exception {
        Path archive = Files.createDirectory(temporaryDirectory.resolve("archive"));
        ArchivePathResolver resolver = new ArchivePathResolver(archive);
        int callers = 32;
        for (int day = 1; day <= 28; day++) {
            CountDownLatch start = new CountDownLatch(1);
            List<Future<Path>> staged = new ArrayList<>();
            try (ExecutorService pool = Executors.newFixedThreadPool(callers)) {
                for (int caller = 0; caller < callers; caller++) {
                    SatelliteImageName image =
                            SatelliteImageName.parse("SAT_IMG_202403%02d120000%03d.raw".formatted(day, caller));
                    staged.add(pool.submit(() -> {
                        start.await();
                        // What staging does next: this fails if the directory is not there yet.
                        return Files.createFile(resolver.resolve(image));
                    }));
                }
                start.countDown();
                for (Future<Path> file : staged) {
                    file.get();
                }
            }
        }
    }
}
