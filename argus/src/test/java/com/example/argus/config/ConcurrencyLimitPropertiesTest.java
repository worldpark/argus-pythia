package com.example.argus.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.TestPropertySource;

class ConcurrencyLimitPropertiesTest {

    @Configuration
    @EnableConfigurationProperties(ConcurrencyLimitProperties.class)
    static class TestConfig {
    }

    @Nested
    @DisplayName("프로퍼티 미설정 시 기본값")
    @SpringBootTest(classes = TestConfig.class)
    class DefaultValues {

        @Autowired
        private ConcurrencyLimitProperties properties;

        @Test
        @DisplayName("prometheus 기본값: permits=4, acquireTimeout=200ms, maxAttempts=3")
        void prometheusSpec_defaultValues() {
            ConcurrencyLimitProperties.LimiterSpec prometheus = properties.getPrometheus();

            assertThat(prometheus.getPermits()).isEqualTo(4);
            assertThat(prometheus.getAcquireTimeout()).isEqualTo(Duration.ofMillis(200));
            assertThat(prometheus.getMaxAttempts()).isEqualTo(3);
        }

        @Test
        @DisplayName("kafkaPublish 기본값: permits=8, acquireTimeout=100ms, maxAttempts=3")
        void kafkaPublishSpec_defaultValues() {
            ConcurrencyLimitProperties.LimiterSpec kafkaPublish = properties.getKafkaPublish();

            assertThat(kafkaPublish.getPermits()).isEqualTo(8);
            assertThat(kafkaPublish.getAcquireTimeout()).isEqualTo(Duration.ofMillis(100));
            assertThat(kafkaPublish.getMaxAttempts()).isEqualTo(3);
        }
    }

    @Nested
    @DisplayName("프로퍼티 override 반영")
    @SpringBootTest(classes = TestConfig.class)
    @TestPropertySource(properties = {
        "argus.concurrency.prometheus.permits=10",
        "argus.concurrency.prometheus.acquire-timeout=500ms",
        "argus.concurrency.prometheus.max-attempts=5",
        "argus.concurrency.kafka-publish.permits=20",
        "argus.concurrency.kafka-publish.acquire-timeout=300ms",
        "argus.concurrency.kafka-publish.max-attempts=7"
    })
    class OverrideValues {

        @Autowired
        private ConcurrencyLimitProperties properties;

        @Test
        @DisplayName("prometheus override: permits=10, acquireTimeout=500ms, maxAttempts=5")
        void prometheusSpec_overrideValues() {
            ConcurrencyLimitProperties.LimiterSpec prometheus = properties.getPrometheus();

            assertThat(prometheus.getPermits()).isEqualTo(10);
            assertThat(prometheus.getAcquireTimeout()).isEqualTo(Duration.ofMillis(500));
            assertThat(prometheus.getMaxAttempts()).isEqualTo(5);
        }

        @Test
        @DisplayName("kafkaPublish override: permits=20, acquireTimeout=300ms, maxAttempts=7")
        void kafkaPublishSpec_overrideValues() {
            ConcurrencyLimitProperties.LimiterSpec kafkaPublish = properties.getKafkaPublish();

            assertThat(kafkaPublish.getPermits()).isEqualTo(20);
            assertThat(kafkaPublish.getAcquireTimeout()).isEqualTo(Duration.ofMillis(300));
            assertThat(kafkaPublish.getMaxAttempts()).isEqualTo(7);
        }
    }
}
