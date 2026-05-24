package com.example.argus.config;

import java.time.Duration;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "argus.concurrency")
@Getter
@Setter
public class ConcurrencyLimitProperties {

    private LimiterSpec prometheus = new LimiterSpec(4, Duration.ofMillis(200), 3);
    private LimiterSpec kafkaPublish = new LimiterSpec(8, Duration.ofMillis(100), 3);

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class LimiterSpec {
        private int permits;
        private Duration acquireTimeout;
        private int maxAttempts;
    }
}
