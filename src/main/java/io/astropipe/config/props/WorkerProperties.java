package io.astropipe.config.props;

import jakarta.validation.constraints.Positive;
import org.springframework.boot.context.properties.bind.DefaultValue;

public record WorkerProperties(
        @Positive(message = "workers.poolSize must be greater than zero")
        @DefaultValue("2") int poolSize,
        @Positive(message = "workers.queueCapacity must be greater than zero")
        @DefaultValue("10000") int queueCapacity) { }
