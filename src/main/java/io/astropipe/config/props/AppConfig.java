package io.astropipe.config.props;

import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

import java.nio.file.Path;

@Validated
@ConfigurationProperties(prefix = "astropipe", ignoreUnknownFields = false)
public record AppConfig(
        @NotBlank(message = "sourceDir must not be blank") String sourceDir,
        @NotBlank(message = "archiveDir must not be blank") String archiveDir,
        @Valid @DefaultValue WatchProperties watch,
        @Valid @DefaultValue StabilityProperties stability,
        @Valid @DefaultValue WorkerProperties workers,
        @DefaultValue("true") boolean createArchiveDir) {

    @AssertTrue(message = "sourceDir and archiveDir must be different directories")
    public boolean isSourceAndArchiveDirectoriesDifferent() {
        if (sourceDir == null || sourceDir.isBlank()
                || archiveDir == null || archiveDir.isBlank()) {
            return true;
        }

        Path normalizedSource = Path.of(sourceDir).toAbsolutePath().normalize();
        Path normalizedArchive = Path.of(archiveDir).toAbsolutePath().normalize();
        return !normalizedSource.equals(normalizedArchive);
    }
}
