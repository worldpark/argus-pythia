package com.example.argus.dto.metric.jvm;

import com.example.argus.dto.metric.MetricStatus;
import com.example.argus.service.metric.mapper.MetricPointMapper.MappingResult;
import java.math.BigDecimal;
import java.time.OffsetDateTime;

public record GcMetricDto(
    BigDecimal avgDurationSeconds,
    BigDecimal count,
    OffsetDateTime measuredAt,
    MetricStatus status,
    String missingReason)
    implements JvmMetricResult {

  public static GcMetricDto from(MappingResult durationResult, MappingResult countResult) {
    boolean durationOk = durationResult instanceof MappingResult.Success;
    boolean countOk = countResult instanceof MappingResult.Success;

    MappingResult.Success ds = durationOk ? (MappingResult.Success) durationResult : null;
    MappingResult.Success cs = countOk ? (MappingResult.Success) countResult : null;

    if (durationOk && countOk) {
      return success(ds.point().value(), cs.point().value(), ds.point().timestamp());
    }
    if (durationOk) {
      return partial(ds.point().value(), null, ds.point().timestamp(),
          prefixedReason("GC_COUNT", countResult));
    }
    if (countOk) {
      return partial(null, cs.point().value(), cs.point().timestamp(),
          prefixedReason("GC_AVG_DURATION", durationResult));
    }

    String combined = joinReasons(
        prefixedReason("GC_AVG_DURATION", durationResult),
        prefixedReason("GC_COUNT", countResult));
    if (durationResult instanceof MappingResult.QueryFailed
        || countResult instanceof MappingResult.QueryFailed) {
      return queryFailed(combined);
    }
    if (durationResult instanceof MappingResult.ParseFailed
        || countResult instanceof MappingResult.ParseFailed) {
      return parseFailed(combined);
    }
    return emptyResult(combined);
  }

  public static GcMetricDto success(
      BigDecimal avgDurationSeconds, BigDecimal count, OffsetDateTime measuredAt) {
    return new GcMetricDto(avgDurationSeconds, count, measuredAt, MetricStatus.SUCCESS, null);
  }

  // duration 또는 count 중 하나만 성공한 경우 null 필드를 허용하는 PARTIAL 상태
  public static GcMetricDto partial(
      BigDecimal avgDurationSeconds, BigDecimal count, OffsetDateTime measuredAt, String missingReason) {
    return new GcMetricDto(avgDurationSeconds, count, measuredAt, MetricStatus.PARTIAL, missingReason);
  }

  public static GcMetricDto emptyResult(String reason) {
    return new GcMetricDto(null, null, null, MetricStatus.EMPTY_RESULT, reason);
  }

  public static GcMetricDto queryFailed(String reason) {
    return new GcMetricDto(null, null, null, MetricStatus.QUERY_FAILED, reason);
  }

  public static GcMetricDto parseFailed(String reason) {
    return new GcMetricDto(null, null, null, MetricStatus.PARSE_FAILED, reason);
  }

  private static String prefixedReason(String prefix, MappingResult result) {
    if (result instanceof MappingResult.Empty) return prefix + ": empty result";
    if (result instanceof MappingResult.ParseFailed pf) return prefix + ": " + pf.reason();
    if (result instanceof MappingResult.QueryFailed qf) return prefix + ": " + qf.reason();
    return prefix + ": unknown";
  }

  private static String joinReasons(String r1, String r2) {
    if (r1 == null) return r2;
    if (r2 == null) return r1;
    return r1 + "; " + r2;
  }
}
