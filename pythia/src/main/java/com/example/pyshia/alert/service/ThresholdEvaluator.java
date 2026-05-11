package com.example.pyshia.alert.service;

import com.example.pyshia.alert.config.ThresholdProperties;
import com.example.pyshia.alert.config.ThresholdProperties.Limit;
import com.example.pyshia.alert.domain.MetricKind;
import com.example.pyshia.alert.domain.Severity;
import com.example.pyshia.alert.domain.ViolationKey;
import com.example.pyshia.alert.state.ViolationStateStore;
import com.example.pyshia.kafka.dto.MetricStatus;
import com.example.pyshia.kafka.dto.hikari.HikariActiveDto;
import com.example.pyshia.kafka.dto.hikari.HikariMetricSnapshotDto;
import com.example.pyshia.kafka.dto.hikari.HikariPendingDto;
import com.example.pyshia.kafka.dto.hikari.PoolMetricPointDto;
import com.example.pyshia.kafka.dto.http.EndpointMetricPointDto;
import com.example.pyshia.kafka.dto.http.HttpErrorRateDto;
import com.example.pyshia.kafka.dto.http.HttpMetricSnapshotDto;
import com.example.pyshia.kafka.dto.http.HttpResponseTimeDto;
import com.example.pyshia.kafka.dto.jvm.CpuUsageDto;
import com.example.pyshia.kafka.dto.jvm.GcMetricDto;
import com.example.pyshia.kafka.dto.jvm.JvmMetricSnapshotDto;
import com.example.pyshia.kafka.dto.jvm.MemoryUsageDto;
import com.example.pyshia.kafka.dto.jvm.ThreadMetricDto;
import java.math.BigDecimal;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class ThresholdEvaluator {

  private final ThresholdProperties properties;
  private final ViolationStateStore store;
  private final AlertNotifier notifier;

  public ThresholdEvaluator(ThresholdProperties properties, ViolationStateStore store,
      AlertNotifier notifier) {
    this.properties = properties;
    this.store = store;
    this.notifier = notifier;
  }

  public void evaluateJvm(JvmMetricSnapshotDto snapshot) {
    String app = snapshot.application();
    String instance = snapshot.instance();
    if (app == null || instance == null) {
      log.warn("Skipping JVM evaluation: null application or instance");
      return;
    }
    ThresholdProperties.JvmThresholds jvm = properties.jvm();

    CpuUsageDto cpu = snapshot.cpu();
    if (cpu != null) {
      evaluateOne(MetricKind.JVM_CPU, app, instance, null,
          cpu.usagePercent(), cpu.status(), jvm.cpuUsagePercent());
    }

    MemoryUsageDto memory = snapshot.memory();
    if (memory != null) {
      evaluateOne(MetricKind.JVM_HEAP, app, instance, null,
          memory.heapUsagePercent(), memory.status(), jvm.heapUsagePercent());
      evaluateOne(MetricKind.JVM_HEAP_OLD_GEN, app, instance, null,
          memory.oldGenUsagePercent(), memory.status(), jvm.oldGenUsagePercent());
    }

    GcMetricDto gc = snapshot.gc();
    if (gc != null) {
      evaluateOne(MetricKind.JVM_GC_PAUSE, app, instance, null,
          gc.avgDurationSeconds(), gc.status(), jvm.gcAvgPauseSeconds());
      evaluateOne(MetricKind.JVM_GC_COUNT, app, instance, null,
          gc.count(), gc.status(), jvm.gcCount());
    }

    ThreadMetricDto thread = snapshot.thread();
    if (thread != null) {
      evaluateOne(MetricKind.JVM_THREAD_ACTIVE, app, instance, null,
          toBigDecimal(thread.activeCount()), thread.status(), jvm.threadActiveCount());
      evaluateOne(MetricKind.JVM_THREAD_PEAK, app, instance, null,
          toBigDecimal(thread.peakCount()), thread.status(), jvm.threadPeakCount());
      evaluateOne(MetricKind.JVM_THREAD_DAEMON, app, instance, null,
          toBigDecimal(thread.daemonCount()), thread.status(), jvm.threadDaemonCount());
    }
  }

  public void evaluateHttp(HttpMetricSnapshotDto snapshot) {
    String app = snapshot.application();
    String instance = snapshot.instance();
    if (app == null || instance == null) {
      log.warn("Skipping HTTP evaluation: null application or instance");
      return;
    }
    ThresholdProperties.HttpThresholds http = properties.http();

    HttpResponseTimeDto p99 = snapshot.p99();
    if (p99 != null) {
      evaluatePoints(MetricKind.HTTP_P99, app, instance, p99.points(), p99.status(),
          http.p99ResponseSeconds());
    }

    HttpErrorRateDto errorRate = snapshot.errorRate();
    if (errorRate != null) {
      evaluatePoints(MetricKind.HTTP_ERROR_RATE, app, instance, errorRate.points(),
          errorRate.status(), http.errorRatePercent());
    }
  }

  public void evaluateHikari(HikariMetricSnapshotDto snapshot) {
    String app = snapshot.application();
    String instance = snapshot.instance();
    if (app == null || instance == null) {
      log.warn("Skipping Hikari evaluation: null application or instance");
      return;
    }
    ThresholdProperties.HikariThresholds hikari = properties.hikari();

    HikariActiveDto active = snapshot.active();
    if (active != null) {
      evaluatePoolPoints(MetricKind.HIKARI_ACTIVE, app, instance, active.points(),
          active.status(), hikari.activeConnections());
      evaluatePoolPoints(MetricKind.HIKARI_USAGE_RATIO, app, instance, active.usageRatio(),
          active.status(), hikari.usageRatioPercent());
    }

    HikariPendingDto pending = snapshot.pending();
    if (pending != null) {
      evaluatePoolPoints(MetricKind.HIKARI_PENDING, app, instance, pending.points(),
          pending.status(), hikari.pendingConnections());
    }
  }

  private void evaluatePoints(MetricKind kind, String app, String instance,
      List<EndpointMetricPointDto> points, MetricStatus status, Limit limit) {
    if (points == null) return;
    for (EndpointMetricPointDto point : points) {
      evaluateOne(kind, app, instance, point.endpoint(), point.value(), status, limit);
    }
  }

  private void evaluatePoolPoints(MetricKind kind, String app, String instance,
      List<PoolMetricPointDto> points, MetricStatus status, Limit limit) {
    if (points == null) return;
    for (PoolMetricPointDto point : points) {
      evaluateOne(kind, app, instance, point.pool(), point.value(), status, limit);
    }
  }

  private void evaluateOne(MetricKind kind, String app, String instance, String sub,
      BigDecimal value, MetricStatus status, Limit limit) {
    try {
      if (value == null || status != MetricStatus.SUCCESS) {
        log.debug("Skipping {}: app={} instance={} sub={} value={} status={}",
            kind, app, instance, sub, value, status);
        return;
      }
      ViolationKey key = new ViolationKey(kind, app, instance, sub);
      Severity severity = determineSeverity(kind, value, limit);
      if (severity != null) {
        BigDecimal threshold = severity == Severity.CRITICAL ? limit.critical() : limit.warning();
        if (store.shouldSend(key, severity, limit.consecutive())) {
          notifier.notify(kind, severity, key, value, threshold, limit.consecutive());
        }
      } else {
        store.clear(key);
      }
    } catch (RuntimeException e) {
      log.error("Unexpected error evaluating {}: app={} instance={} sub={}", kind, app, instance, sub, e);
    }
  }

  private Severity determineSeverity(MetricKind kind, BigDecimal value, Limit limit) {
    if (kind.getOperator().test(value, limit.critical())) {
      return Severity.CRITICAL;
    }
    if (kind.getOperator().test(value, limit.warning())) {
      return Severity.WARNING;
    }
    return null;
  }

  private BigDecimal toBigDecimal(Integer value) {
    return value == null ? null : BigDecimal.valueOf(value);
  }
}
