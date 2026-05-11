package com.example.pyshia.alert.config;

import com.example.pyshia.alert.exception.ThresholdConfigErrorCode;
import com.example.pyshia.alert.exception.ThresholdConfigException;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import java.math.BigDecimal;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "pyshia.threshold")
public record ThresholdProperties(
    @Valid @NotNull JvmThresholds jvm,
    @Valid @NotNull HttpThresholds http,
    @Valid @NotNull HikariThresholds hikari) {

  public record Limit(BigDecimal warning, BigDecimal critical, int consecutive) {
    public Limit {
      if (warning == null || critical == null) {
        throw new ThresholdConfigException(
            ThresholdConfigErrorCode.MISSING_THRESHOLD,
            "warning and critical must not be null");
      }
      if (warning.compareTo(critical) >= 0) {
        throw new ThresholdConfigException(
            ThresholdConfigErrorCode.NON_MONOTONIC,
            "warning (%s) must be less than critical (%s)".formatted(warning, critical));
      }
    }
  }

  public record JvmThresholds(
      @Valid @NotNull Limit cpuUsagePercent,
      @Valid @NotNull Limit heapUsagePercent,
      @Valid @NotNull Limit oldGenUsagePercent,
      @Valid @NotNull Limit gcAvgPauseSeconds,
      @Valid @NotNull Limit gcCount,
      @Valid @NotNull Limit threadActiveCount,
      @Valid @NotNull Limit threadPeakCount,
      @Valid @NotNull Limit threadDaemonCount) {}

  public record HttpThresholds(
      @Valid @NotNull Limit p99ResponseSeconds,
      @Valid @NotNull Limit errorRatePercent) {}

  public record HikariThresholds(
      @Valid @NotNull Limit activeConnections,
      @Valid @NotNull Limit pendingConnections,
      @Valid @NotNull Limit usageRatioPercent) {}
}
