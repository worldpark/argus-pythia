package com.example.argus.dto.metric.hikari;

import com.example.argus.dto.metric.MetricStatus;
import com.example.argus.service.metric.mapper.MetricPointMapper.MultiMappingResult;
import java.time.OffsetDateTime;
import java.util.List;

public record HikariActiveDto(
    List<PoolMetricPointDto> points,
    List<PoolMetricPointDto> usageRatio,
    OffsetDateTime measuredAt,
    MetricStatus status,
    String missingReason)
    implements HikariMetricResult {

  public static HikariActiveDto from(MultiMappingResult result) {
    return HikariMetricResult.from(result,
        (points, measuredAt) -> HikariActiveDto.success(points, List.of(), measuredAt),
        HikariActiveDto::empty,
        HikariActiveDto::parseFailed,
        HikariActiveDto::queryFailed);
  }

  public static HikariActiveDto from(
      MultiMappingResult activeResult, MultiMappingResult usageRatioResult) {
    HikariActiveDto active = from(activeResult);
    HikariActiveDto ratio = from(usageRatioResult);

    if (active.status() == MetricStatus.SUCCESS && ratio.status() == MetricStatus.SUCCESS) {
      return success(active.points(), ratio.points(), active.measuredAt());
    }
    if (active.status() == MetricStatus.SUCCESS || ratio.status() == MetricStatus.SUCCESS) {
      return partial(active.points(), ratio.points(), firstMeasuredAt(active, ratio),
          joinReasons(reason("HIKARI_ACTIVE_CONNECTIONS", active),
              reason("HIKARI_USAGE_RATIO", ratio)));
    }
    String combined = joinReasons(
        reason("HIKARI_ACTIVE_CONNECTIONS", active),
        reason("HIKARI_USAGE_RATIO", ratio));
    if (active.status() == MetricStatus.QUERY_FAILED || ratio.status() == MetricStatus.QUERY_FAILED) {
      return queryFailed(combined);
    }
    if (active.status() == MetricStatus.PARSE_FAILED || ratio.status() == MetricStatus.PARSE_FAILED) {
      return parseFailed(combined);
    }
    return new HikariActiveDto(List.of(), List.of(), null, MetricStatus.EMPTY_RESULT, combined);
  }

  public static HikariActiveDto success(
      List<PoolMetricPointDto> points, List<PoolMetricPointDto> usageRatio, OffsetDateTime measuredAt) {
    return new HikariActiveDto(points, usageRatio, measuredAt, MetricStatus.SUCCESS, null);
  }

  public static HikariActiveDto partial(
      List<PoolMetricPointDto> points,
      List<PoolMetricPointDto> usageRatio,
      OffsetDateTime measuredAt,
      String missingReason) {
    return new HikariActiveDto(
        points, usageRatio, measuredAt, MetricStatus.PARTIAL, missingReason);
  }

  public static HikariActiveDto empty() {
    return new HikariActiveDto(List.of(), List.of(), null, MetricStatus.EMPTY_RESULT, "empty result");
  }

  public static HikariActiveDto queryFailed(String reason) {
    return new HikariActiveDto(List.of(), List.of(), null, MetricStatus.QUERY_FAILED, reason);
  }

  public static HikariActiveDto parseFailed(String reason) {
    return new HikariActiveDto(List.of(), List.of(), null, MetricStatus.PARSE_FAILED, reason);
  }

  private static OffsetDateTime firstMeasuredAt(HikariActiveDto active, HikariActiveDto ratio) {
    return active.measuredAt() != null ? active.measuredAt() : ratio.measuredAt();
  }

  private static String reason(String prefix, HikariActiveDto dto) {
    if (dto.status() == MetricStatus.SUCCESS) {
      return null;
    }
    return prefix + ": " + dto.missingReason();
  }

  private static String joinReasons(String r1, String r2) {
    if (r1 == null) return r2;
    if (r2 == null) return r1;
    return r1 + "; " + r2;
  }
}
