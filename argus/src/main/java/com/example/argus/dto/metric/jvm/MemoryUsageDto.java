package com.example.argus.dto.metric.jvm;

import com.example.argus.dto.metric.MetricStatus;
import com.example.argus.service.metric.mapper.MetricPointMapper.MappingResult;
import java.math.BigDecimal;
import java.time.OffsetDateTime;

public record MemoryUsageDto(
    BigDecimal heapUsagePercent,
    BigDecimal oldGenUsagePercent,
    OffsetDateTime measuredAt,
    MetricStatus status,
    String missingReason)
    implements JvmMetricResult {

  public static MemoryUsageDto from(MappingResult result) {
    return fromSingle(result);
  }

  public static MemoryUsageDto from(MappingResult heapResult, MappingResult oldGenResult) {
    boolean heapOk = heapResult instanceof MappingResult.Success;
    boolean oldGenOk = oldGenResult instanceof MappingResult.Success;

    MappingResult.Success hs = heapOk ? (MappingResult.Success) heapResult : null;
    MappingResult.Success os = oldGenOk ? (MappingResult.Success) oldGenResult : null;

    if (heapOk && oldGenOk) {
      return success(hs.point().value(), os.point().value(), hs.point().timestamp());
    }
    if (heapOk) {
      return partial(hs.point().value(), null, hs.point().timestamp(),
          prefixedReason("HEAP_OLD_GEN_USAGE", oldGenResult));
    }
    if (oldGenOk) {
      return partial(null, os.point().value(), os.point().timestamp(),
          prefixedReason("HEAP_USAGE", heapResult));
    }

    String combined = joinReasons(
        prefixedReason("HEAP_USAGE", heapResult),
        prefixedReason("HEAP_OLD_GEN_USAGE", oldGenResult));
    if (heapResult instanceof MappingResult.QueryFailed || oldGenResult instanceof MappingResult.QueryFailed) {
      return queryFailed(combined);
    }
    if (heapResult instanceof MappingResult.ParseFailed || oldGenResult instanceof MappingResult.ParseFailed) {
      return parseFailed(combined);
    }
    return emptyResult(combined);
  }

  public static MemoryUsageDto fromSingle(MappingResult result) {
    return JvmMetricResult.from(result,
        (value, ts) -> MemoryUsageDto.success(value, null, ts),
        MemoryUsageDto::empty,
        MemoryUsageDto::parseFailed,
        MemoryUsageDto::queryFailed);
  }

  public static MemoryUsageDto success(
      BigDecimal heapUsagePercent, BigDecimal oldGenUsagePercent, OffsetDateTime measuredAt) {
    return new MemoryUsageDto(
        heapUsagePercent, oldGenUsagePercent, measuredAt, MetricStatus.SUCCESS, null);
  }

  public static MemoryUsageDto partial(
      BigDecimal heapUsagePercent,
      BigDecimal oldGenUsagePercent,
      OffsetDateTime measuredAt,
      String missingReason) {
    return new MemoryUsageDto(
        heapUsagePercent, oldGenUsagePercent, measuredAt, MetricStatus.PARTIAL, missingReason);
  }

  public static MemoryUsageDto empty() {
    return emptyResult("empty result");
  }

  public static MemoryUsageDto emptyResult(String reason) {
    return new MemoryUsageDto(null, null, null, MetricStatus.EMPTY_RESULT, reason);
  }

  public static MemoryUsageDto queryFailed(String reason) {
    return new MemoryUsageDto(null, null, null, MetricStatus.QUERY_FAILED, reason);
  }

  public static MemoryUsageDto parseFailed(String reason) {
    return new MemoryUsageDto(null, null, null, MetricStatus.PARSE_FAILED, reason);
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
