package com.example.argus.config;

import java.time.Duration;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "argus.buffer")
@Getter
@Setter
public class MetricBufferProperties {

    private Duration ttl = Duration.ofMinutes(5);
    private int maxSize = 5000;
    private OverflowPolicy overflowPolicy = OverflowPolicy.DROP_OLDEST;
    private int drainBatchSize = 50;
    private long drainIntervalMs = 5000;
    private String keyPrefix = "argus:buffer:";

    public enum OverflowPolicy {
        DROP_OLDEST,
        DROP_NEWEST,
        REJECT
    }
}
