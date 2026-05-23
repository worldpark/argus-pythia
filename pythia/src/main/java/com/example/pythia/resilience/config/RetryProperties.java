package com.example.pythia.resilience.config;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "resilience4j.retry")
@Getter
@Setter
public class RetryProperties {

  private Map<String, RetryInstanceProperties> instances = new LinkedHashMap<>();

  @Getter
  @Setter
  public static class RetryInstanceProperties {

    private int maxAttempts = 3;
    private Duration waitDuration = Duration.ofMillis(500);
    private boolean enableExponentialBackoff;
    private double exponentialBackoffMultiplier = 2.0d;
    private List<String> retryExceptions = List.of();
    private List<String> ignoreExceptions = List.of();
  }
}
