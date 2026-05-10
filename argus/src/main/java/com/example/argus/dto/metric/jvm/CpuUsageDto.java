package com.example.argus.dto.metric.jvm;

import com.example.argus.dto.metric.MetricStatus;
import com.example.argus.service.metric.mapper.MetricPointMapper.MappingResult;
import java.math.BigDecimal;
import java.time.OffsetDateTime;

public record CpuUsageDto(
    BigDecimal usagePercent,
    OffsetDateTime measuredAt,
    MetricStatus status,
    String missingReason)
    implements JvmMetricResult {

  public static CpuUsageDto from(MappingResult result) {
    return JvmMetricResult.from(result,
        CpuUsageDto::success,
        CpuUsageDto::empty,
        CpuUsageDto::parseFailed,
        CpuUsageDto::queryFailed);
  }

  public static CpuUsageDto success(BigDecimal usagePercent, OffsetDateTime measuredAt) {
    return new CpuUsageDto(usagePercent, measuredAt, MetricStatus.SUCCESS, null);
  }

  public static CpuUsageDto empty() {
    return new CpuUsageDto(null, null, MetricStatus.EMPTY_RESULT, "empty result");
  }

  public static CpuUsageDto queryFailed(String reason) {
    return new CpuUsageDto(null, null, MetricStatus.QUERY_FAILED, reason);
  }

  public static CpuUsageDto parseFailed(String reason) {
    return new CpuUsageDto(null, null, MetricStatus.PARSE_FAILED, reason);
  }
}
