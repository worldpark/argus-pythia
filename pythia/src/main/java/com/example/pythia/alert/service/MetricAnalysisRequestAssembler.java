package com.example.pythia.alert.service;

import com.example.pythia.ai.dto.AnalysisTarget;
import com.example.pythia.ai.dto.MetricAnalysisRequest;
import com.example.pythia.ai.dto.MetricSummary;
import com.example.pythia.ai.dto.SummaryAggregation;
import com.example.pythia.ai.dto.TimeSeriesPoint;
import com.example.pythia.ai.exception.AiAnalysisException;
import com.example.pythia.ai.exception.AiErrorCode;
import com.example.pythia.alert.domain.MetricKind;
import com.example.pythia.alert.domain.ViolationKey;
import com.example.pythia.metric.domain.HikariMetricKind;
import com.example.pythia.metric.domain.HikariMetricSnapshotEntity;
import com.example.pythia.metric.domain.HikariPoolMetricPointEntity;
import com.example.pythia.metric.domain.HttpEndpointMetricPointEntity;
import com.example.pythia.metric.domain.HttpMetricKind;
import com.example.pythia.metric.domain.HttpMetricSnapshotEntity;
import com.example.pythia.metric.domain.JvmMetricSnapshotEntity;
import com.example.pythia.metric.repository.HikariMetricSnapshotRepository;
import com.example.pythia.metric.repository.HttpMetricSnapshotRepository;
import com.example.pythia.metric.repository.JvmMetricSnapshotRepository;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class MetricAnalysisRequestAssembler {

  private static final int VALUE_SCALE = 6;

  private final JvmMetricSnapshotRepository jvmRepository;
  private final HttpMetricSnapshotRepository httpRepository;
  private final HikariMetricSnapshotRepository hikariRepository;
  private final Duration analysisWindow;

  public MetricAnalysisRequestAssembler(
      JvmMetricSnapshotRepository jvmRepository,
      HttpMetricSnapshotRepository httpRepository,
      HikariMetricSnapshotRepository hikariRepository,
      @Value("${pythia.alert.analysis-window:PT10M}") Duration analysisWindow) {
    if (analysisWindow == null || analysisWindow.isZero() || analysisWindow.isNegative()) {
      throw new IllegalArgumentException(
          "pythia.alert.analysis-window must be a positive duration, got " + analysisWindow);
    }
    this.jvmRepository = jvmRepository;
    this.httpRepository = httpRepository;
    this.hikariRepository = hikariRepository;
    this.analysisWindow = analysisWindow;
  }

  @Transactional(readOnly = true)
  public MetricAnalysisRequest assemble(MetricKind kind, ViolationKey key) {
    OffsetDateTime to = OffsetDateTime.now();
    OffsetDateTime from = to.minus(analysisWindow);

    List<TimeSeriesPoint> points = switch (kind) {
      case JVM_CPU,
           JVM_HEAP,
           JVM_HEAP_OLD_GEN,
           JVM_GC_PAUSE,
           JVM_GC_COUNT,
           JVM_THREAD_ACTIVE,
           JVM_THREAD_PEAK,
           JVM_THREAD_DAEMON -> collectJvm(kind, key, from, to);
      case HTTP_P99 -> collectHttp(kind, HttpMetricKind.P99, key, from, to);
      case HTTP_ERROR_RATE -> collectHttp(kind, HttpMetricKind.ERROR_RATE, key, from, to);
      case HIKARI_ACTIVE -> collectHikari(kind, HikariMetricKind.ACTIVE, key, from, to);
      case HIKARI_PENDING -> collectHikari(kind, HikariMetricKind.PENDING, key, from, to);
      case HIKARI_USAGE_RATIO -> collectHikari(kind, HikariMetricKind.USAGE_RATIO, key, from, to);
    };

    if (points.isEmpty()) {
      throw new AiAnalysisException(
          AiErrorCode.INVALID_REQUEST,
          "no metric data for kind=" + kind + " key=" + key);
    }

    List<BigDecimal> values = points.stream()
        .map(TimeSeriesPoint::value)
        .toList();
    BigDecimal avg = average(values);
    BigDecimal max = values.stream().max(Comparator.naturalOrder()).orElseThrow();

    List<MetricSummary> summaries = List.of(
        new MetricSummary(kind.name(), SummaryAggregation.AVG, avg, kind.getUnit()),
        new MetricSummary(kind.name(), SummaryAggregation.MAX, max, kind.getUnit()));

    AnalysisTarget target = new AnalysisTarget(key.application(), key.instance(), analysisWindow);
    return new MetricAnalysisRequest(target, summaries, points);
  }

  private List<TimeSeriesPoint> collectJvm(
      MetricKind kind, ViolationKey key, OffsetDateTime from, OffsetDateTime to) {
    List<JvmMetricSnapshotEntity> rows =
        jvmRepository.findByApplicationAndInstanceAndCollectedAtBetweenOrderByCollectedAtAsc(
            key.application(), key.instance(), from, to);

    Function<JvmMetricSnapshotEntity, BigDecimal> extractor = jvmValueExtractor(kind);

    List<TimeSeriesPoint> result = new ArrayList<>(rows.size());
    for (JvmMetricSnapshotEntity row : rows) {
      BigDecimal value = extractor.apply(row);
      if (value == null) {
        continue;
      }
      result.add(new TimeSeriesPoint(row.getCollectedAt(), kind.name(), value));
    }
    return result;
  }

  private Function<JvmMetricSnapshotEntity, BigDecimal> jvmValueExtractor(MetricKind kind) {
    return switch (kind) {
      case JVM_CPU -> JvmMetricSnapshotEntity::getCpuUsagePercent;
      case JVM_HEAP -> JvmMetricSnapshotEntity::getHeapUsagePercent;
      case JVM_HEAP_OLD_GEN -> JvmMetricSnapshotEntity::getOldGenUsagePercent;
      case JVM_GC_PAUSE -> JvmMetricSnapshotEntity::getGcAvgDurationSeconds;
      case JVM_GC_COUNT -> JvmMetricSnapshotEntity::getGcCount;
      case JVM_THREAD_ACTIVE -> row -> toBigDecimal(row.getThreadActiveCount());
      case JVM_THREAD_PEAK -> row -> toBigDecimal(row.getThreadPeakCount());
      case JVM_THREAD_DAEMON -> row -> toBigDecimal(row.getThreadDaemonCount());
      case HTTP_P99, HTTP_ERROR_RATE, HIKARI_ACTIVE, HIKARI_PENDING, HIKARI_USAGE_RATIO ->
          throw new IllegalStateException("not a JVM kind: " + kind);
    };
  }

  private List<TimeSeriesPoint> collectHttp(
      MetricKind kind, HttpMetricKind pointKind, ViolationKey key,
      OffsetDateTime from, OffsetDateTime to) {
    if (key.sub() == null || key.sub().isBlank()) {
      throw new AiAnalysisException(
          AiErrorCode.INVALID_REQUEST, "key.sub() is required for HTTP metrics");
    }
    List<HttpMetricSnapshotEntity> snapshots =
        httpRepository.findByApplicationAndInstanceAndCollectedAtBetweenOrderByCollectedAtAsc(
            key.application(), key.instance(), from, to);

    String endpoint = key.sub();
    List<TimeSeriesPoint> result = new ArrayList<>();
    for (HttpMetricSnapshotEntity snapshot : snapshots) {
      for (HttpEndpointMetricPointEntity point : snapshot.getPoints()) {
        if (point.getKind() != pointKind) {
          continue;
        }
        if (endpoint != null && !endpoint.equals(point.getEndpoint())) {
          continue;
        }
        if (point.getValue() == null) {
          continue;
        }
        OffsetDateTime ts = point.getMeasuredAt() != null
            ? point.getMeasuredAt() : snapshot.getCollectedAt();
        result.add(new TimeSeriesPoint(ts, kind.name(), point.getValue()));
      }
    }
    result.sort(Comparator.comparing(TimeSeriesPoint::timestamp));
    return result;
  }

  private List<TimeSeriesPoint> collectHikari(
      MetricKind kind, HikariMetricKind pointKind, ViolationKey key,
      OffsetDateTime from, OffsetDateTime to) {
    if (key.sub() == null || key.sub().isBlank()) {
      throw new AiAnalysisException(
          AiErrorCode.INVALID_REQUEST, "key.sub() is required for Hikari metrics");
    }
    List<HikariMetricSnapshotEntity> snapshots =
        hikariRepository.findByApplicationAndInstanceAndCollectedAtBetweenOrderByCollectedAtAsc(
            key.application(), key.instance(), from, to);

    String pool = key.sub();
    List<TimeSeriesPoint> result = new ArrayList<>();
    for (HikariMetricSnapshotEntity snapshot : snapshots) {
      for (HikariPoolMetricPointEntity point : snapshot.getPoints()) {
        if (point.getKind() != pointKind) {
          continue;
        }
        if (pool != null && !pool.equals(point.getPool())) {
          continue;
        }
        if (point.getValue() == null) {
          continue;
        }
        OffsetDateTime ts = point.getMeasuredAt() != null
            ? point.getMeasuredAt() : snapshot.getCollectedAt();
        result.add(new TimeSeriesPoint(ts, kind.name(), point.getValue()));
      }
    }
    result.sort(Comparator.comparing(TimeSeriesPoint::timestamp));
    return result;
  }

  private static BigDecimal toBigDecimal(Integer v) {
    return v == null ? null : BigDecimal.valueOf(v);
  }

  private static BigDecimal average(List<BigDecimal> values) {
    BigDecimal sum = BigDecimal.ZERO;
    for (BigDecimal v : values) {
      sum = sum.add(v);
    }
    return sum.divide(BigDecimal.valueOf(values.size()), VALUE_SCALE, RoundingMode.HALF_UP);
  }
}
