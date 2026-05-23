package com.example.pythia.resilience.config;

import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig.SlidingWindowType;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "resilience4j.circuitbreaker")
@Getter
@Setter
public class CircuitBreakerProperties {

  private Map<String, CircuitBreakerInstanceProperties> instances = new LinkedHashMap<>();

  @Getter
  @Setter
  public static class CircuitBreakerInstanceProperties {

    private float failureRateThreshold = 50.0f;
    private Duration waitDurationInOpenState = Duration.ofSeconds(60);
    private int slidingWindowSize = 100;
    private SlidingWindowType slidingWindowType = SlidingWindowType.COUNT_BASED;
    private int minimumNumberOfCalls = 100;
    private int permittedNumberOfCallsInHalfOpenState = 10;
    private boolean automaticTransitionFromOpenToHalfOpenEnabled;
    private List<String> ignoreExceptions = List.of();
  }
}
