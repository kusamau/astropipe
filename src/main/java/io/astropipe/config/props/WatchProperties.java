package io.astropipe.config.props;

import jakarta.validation.constraints.Positive;
import org.springframework.boot.context.properties.bind.DefaultValue;

public record WatchProperties(
        @Positive(message = "watch.reconciliationIntervalSeconds must be greater than zero")
        @DefaultValue("30") int reconciliationIntervalSeconds) { }
