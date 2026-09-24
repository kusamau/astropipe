package io.astropipe.config.props;

import jakarta.validation.constraints.Positive;
import org.springframework.boot.context.properties.bind.DefaultValue;

public record StabilityProperties(
        @Positive(message = "stability.pollIntervalMillis must be greater than zero")
        @DefaultValue("250") int pollIntervalMillis,
        @Positive(message = "stability.quietPeriodMillis must be greater than zero")
        @DefaultValue("250") int quietPeriodMillis,
        @Positive(message = "stability.resumeOverlapBytes must be greater than zero")
        @DefaultValue("65536") int resumeOverlapBytes) { }
