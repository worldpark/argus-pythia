package com.example.pythia.resilience.config;

import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.micrometer.tagged.TaggedCircuitBreakerMetrics;
import io.github.resilience4j.micrometer.tagged.TaggedRetryMetrics;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Map;
import java.util.stream.StreamSupport;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties({
    RetryProperties.class,
    CircuitBreakerProperties.class
})
public class ResilienceConfig {

  @Bean
  public RetryRegistry retryRegistry(RetryProperties properties) {
    RetryRegistry registry = RetryRegistry.ofDefaults();
    for (Map.Entry<String, RetryProperties.RetryInstanceProperties> entry
        : properties.getInstances().entrySet()) {
      registry.retry(entry.getKey(), toRetryConfig(entry.getValue()));
    }
    return registry;
  }

  @Bean
  public CircuitBreakerRegistry circuitBreakerRegistry(CircuitBreakerProperties properties) {
    CircuitBreakerRegistry registry = CircuitBreakerRegistry.ofDefaults();
    for (Map.Entry<String, CircuitBreakerProperties.CircuitBreakerInstanceProperties> entry
        : properties.getInstances().entrySet()) {
      registry.circuitBreaker(entry.getKey(), toCircuitBreakerConfig(entry.getValue()));
    }
    return registry;
  }

  private RetryConfig toRetryConfig(RetryProperties.RetryInstanceProperties properties) {
    RetryConfig.Builder<Object> builder = RetryConfig.custom()
        .maxAttempts(properties.getMaxAttempts());

    if (properties.isEnableExponentialBackoff()) {
      builder.intervalFunction(IntervalFunction.ofExponentialBackoff(
          properties.getWaitDuration(),
          properties.getExponentialBackoffMultiplier()));
    } else {
      builder.waitDuration(properties.getWaitDuration());
    }

    Class<? extends Throwable>[] retryExceptions = toThrowableClasses(
        properties.getRetryExceptions());
    if (retryExceptions.length > 0) {
      builder.retryExceptions(retryExceptions);
    }

    Class<? extends Throwable>[] ignoreExceptions = toThrowableClasses(
        properties.getIgnoreExceptions());
    if (ignoreExceptions.length > 0) {
      builder.ignoreExceptions(ignoreExceptions);
    }

    return builder.build();
  }

  private CircuitBreakerConfig toCircuitBreakerConfig(
      CircuitBreakerProperties.CircuitBreakerInstanceProperties properties) {
    CircuitBreakerConfig.Builder builder = CircuitBreakerConfig.custom()
        .failureRateThreshold(properties.getFailureRateThreshold())
        .waitDurationInOpenState(properties.getWaitDurationInOpenState())
        .slidingWindowSize(properties.getSlidingWindowSize())
        .slidingWindowType(properties.getSlidingWindowType())
        .minimumNumberOfCalls(properties.getMinimumNumberOfCalls())
        .permittedNumberOfCallsInHalfOpenState(
            properties.getPermittedNumberOfCallsInHalfOpenState())
        .automaticTransitionFromOpenToHalfOpenEnabled(
            properties.isAutomaticTransitionFromOpenToHalfOpenEnabled());

    Class<? extends Throwable>[] ignoreExceptions = toThrowableClasses(
        properties.getIgnoreExceptions());
    if (ignoreExceptions.length > 0) {
      builder.ignoreExceptions(ignoreExceptions);
    }

    return builder.build();
  }

  @Bean
  public TaggedRetryMetrics taggedRetryMetrics(RetryRegistry retryRegistry, MeterRegistry meterRegistry) {
    TaggedRetryMetrics metrics = TaggedRetryMetrics.ofRetryRegistry(retryRegistry);
    metrics.bindTo(meterRegistry);
    return metrics;
  }

  @Bean
  public TaggedCircuitBreakerMetrics taggedCircuitBreakerMetrics(
      CircuitBreakerRegistry circuitBreakerRegistry, MeterRegistry meterRegistry) {
    TaggedCircuitBreakerMetrics metrics =
        TaggedCircuitBreakerMetrics.ofCircuitBreakerRegistry(circuitBreakerRegistry);
    metrics.bindTo(meterRegistry);
    return metrics;
  }

  @SuppressWarnings("unchecked")
  private Class<? extends Throwable>[] toThrowableClasses(Iterable<String> classNames) {
    return StreamSupport.stream(classNames.spliterator(), false)
        .map(this::toThrowableClass)
        .toArray(Class[]::new);
  }

  @SuppressWarnings("unchecked")
  private Class<? extends Throwable> toThrowableClass(String className) {
    try {
      Class<?> resolvedClass = Class.forName(className);
      if (!Throwable.class.isAssignableFrom(resolvedClass)) {
        throw new IllegalArgumentException("Configured class is not a Throwable: " + className);
      }
      return (Class<? extends Throwable>) resolvedClass;
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException(
          "Failed to resolve resilience4j exception class: " + className, e);
    }
  }
}
