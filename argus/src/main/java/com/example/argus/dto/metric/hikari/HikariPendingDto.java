package com.example.argus.dto.metric.hikari;

import com.example.argus.dto.metric.MetricStatus;
import com.example.argus.service.metric.mapper.MetricPointMapper.MultiMappingResult;
import java.time.OffsetDateTime;
import java.util.List;

public record HikariPendingDto(
    List<PoolMetricPointDto> points,
    OffsetDateTime measuredAt,
    MetricStatus status,
    String missingReason)
    implements HikariMetricResult {

  public static HikariPendingDto from(MultiMappingResult result) {
    return HikariMetricResult.from(result,
        HikariPendingDto::success,
        HikariPendingDto::empty,
        HikariPendingDto::parseFailed,
        HikariPendingDto::queryFailed);
  }

  public static HikariPendingDto success(List<PoolMetricPointDto> points, OffsetDateTime measuredAt) {
    return new HikariPendingDto(points, measuredAt, MetricStatus.SUCCESS, null);
  }

  public static HikariPendingDto empty() {
    return new HikariPendingDto(List.of(), null, MetricStatus.EMPTY_RESULT, "empty result");
  }

  public static HikariPendingDto queryFailed(String reason) {
    return new HikariPendingDto(List.of(), null, MetricStatus.QUERY_FAILED, reason);
  }

  public static HikariPendingDto parseFailed(String reason) {
    return new HikariPendingDto(List.of(), null, MetricStatus.PARSE_FAILED, reason);
  }
}
