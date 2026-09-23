package io.astropipe;

import io.astropipe.config.props.AppConfig;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

import static org.assertj.core.api.Assertions.assertThat;

class AppConfigTest {
    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withUserConfiguration(Config.class);

    @Test
    void loadsApplicationYaml() {
        contextRunner
                .withInitializer(new ConfigDataApplicationContextInitializer())
                .run(context -> {
                    AppConfig config = context.getBean(AppConfig.class);

                    assertThat(config.sourceDir()).isEqualTo("./data/source");
                    assertThat(config.archiveDir()).isEqualTo("./data/archive");
                });
    }

    @Test
    void bindsPropertiesAndAppliesDefaults() {
        contextRunner
                .withPropertyValues(
                        "astropipe.source-dir=source",
                        "astropipe.archive-dir=archive",
                        "astropipe.workers.pool-size=4")
                .run(context -> {
                    AppConfig config = context.getBean(AppConfig.class);

                    assertThat(config.sourceDir()).isEqualTo("source");
                    assertThat(config.archiveDir()).isEqualTo("archive");
                    assertThat(config.watch().reconciliationIntervalSeconds()).isEqualTo(30);
                    assertThat(config.stability().enabled()).isTrue();
                    assertThat(config.stability().pollIntervalMillis()).isEqualTo(250);
                    assertThat(config.stability().consecutiveStableChecks()).isEqualTo(2);
                    assertThat(config.workers().poolSize()).isEqualTo(4);
                    assertThat(config.workers().queueCapacity()).isEqualTo(10_000);
                    assertThat(config.forceCopyPath()).isFalse();
                });
    }

    @Test
    void missingRequiredDirectoriesFailValidation() {
        contextRunner.run(context -> {
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure())
                    .hasStackTraceContaining("sourceDir must not be blank")
                    .hasStackTraceContaining("archiveDir must not be blank");
        });
    }

    @Test
    void invalidNestedPropertiesFailValidation() {
        contextRunner
                .withPropertyValues(
                        "astropipe.source-dir=source",
                        "astropipe.archive-dir=archive",
                        "astropipe.watch.reconciliation-interval-seconds=0",
                        "astropipe.stability.poll-interval-millis=0",
                        "astropipe.stability.consecutive-stable-checks=1",
                        "astropipe.workers.pool-size=0",
                        "astropipe.workers.queue-capacity=0")
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure())
                            .hasStackTraceContaining("watch.reconciliationIntervalSeconds must be greater than zero")
                            .hasStackTraceContaining("stability.pollIntervalMillis must be greater than zero")
                            .hasStackTraceContaining("stability.consecutiveStableChecks must be at least 2")
                            .hasStackTraceContaining("workers.poolSize must be greater than zero")
                            .hasStackTraceContaining("workers.queueCapacity must be greater than zero");
                });
    }

    @Test
    void sourceAndArchiveDirectoriesMustBeDifferent() {
        contextRunner
                .withPropertyValues(
                        "astropipe.source-dir=./data",
                        "astropipe.archive-dir=data")
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure())
                            .hasStackTraceContaining("sourceDir and archiveDir must be different directories");
                });
    }

    @Test
    void rejectsUnknownProperties() {
        contextRunner
                .withPropertyValues(
                        "astropipe.source-dir=source",
                        "astropipe.archive-dir=archive",
                        "astropipe.unknown-setting=true")
                .run(context -> assertThat(context).hasFailed());
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(AppConfig.class)
    static class Config { }
}
