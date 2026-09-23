package io.astropipe.config.props;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Positive;
import org.springframework.boot.context.properties.bind.DefaultValue;

public record StabilityProperties(
        @DefaultValue("true") boolean enabled,
        @Positive(message = "stability.pollIntervalMillis must be greater than zero")
        @DefaultValue("250") int pollIntervalMillis,
        @Min(value = 2, message = "stability.consecutiveStableChecks must be at least 2")
        @DefaultValue("2") int consecutiveStableChecks) { }
