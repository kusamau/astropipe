package io.astropipe;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class DataSourceConfigIT {
    @TempDir
    Path temporaryDirectory;

    @Test
    void bootConfiguresJdbcTemplateAndRepositoryWithHikari() {
        Path database = temporaryDirectory.resolve("archive.db");
        new ApplicationContextRunner()
                .withUserConfiguration(Config.class)
                .withPropertyValues(
                        "spring.datasource.url=jdbc:sqlite:" + database,
                        "spring.datasource.hikari.maximum-pool-size=2",
                        "spring.datasource.hikari.data-source-properties.busy_timeout=5000",
                        "spring.datasource.hikari.data-source-properties.synchronous=NORMAL")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(DataSource.class);
                    assertThat(context).hasSingleBean(JdbcTemplate.class);
                    assertThat(context).hasSingleBean(ArchiveRepository.class);

                    HikariDataSource dataSource = context.getBean(HikariDataSource.class);
                    assertThat(dataSource.getMaximumPoolSize()).isEqualTo(2);

                    JdbcTemplate jdbcTemplate = context.getBean(JdbcTemplate.class);
                    context.getBean(ArchiveRepository.class).initialize();
                    assertThat(Files.exists(database)).isTrue();
                    assertThat(jdbcTemplate.queryForObject("PRAGMA busy_timeout", Integer.class))
                            .isEqualTo(5000);
                    assertThat(jdbcTemplate.queryForObject("PRAGMA synchronous", Integer.class))
                            .isEqualTo(1);
                    assertThat(jdbcTemplate.queryForObject("PRAGMA journal_mode", String.class))
                            .isEqualToIgnoringCase("wal");
                });
    }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    @Import(ArchiveRepository.class)
    static class Config { }
}
