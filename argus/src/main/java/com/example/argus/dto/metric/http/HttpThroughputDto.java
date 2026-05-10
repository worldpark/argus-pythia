package com.example.argus.dto.metric.http;

import com.example.argus.dto.metric.MetricStatus;
import com.example.argus.service.metric.mapper.MetricPointMapper.MultiMappingResult;
import java.time.OffsetDateTime;
import java.util.List;

public record HttpThroughputDto(
    List<EndpointMetricPointDto> points,
    OffsetDateTime measuredAt,
    MetricStatus status,
    String missingReason)
    implements HttpMetricResult {

  public static HttpThroughputDto from(MultiMappingResult result) {
    return HttpMetricResult.from(result,
        HttpThroughputDto::success,
        HttpThroughputDto::empty,
        HttpThroughputDto::parseFailed,
        HttpThroughputDto::queryFailed);
  }

  public static HttpThroughputDto success(List<EndpointMetricPointDto> points, OffsetDateTime measuredAt) {
    return new HttpThroughputDto(points, measuredAt, MetricStatus.SUCCESS, null);
  }

  public static HttpThroughputDto empty() {
    return new HttpThroughputDto(List.of(), null, MetricStatus.EMPTY_RESULT, "empty result");
  }

  public static HttpThroughputDto queryFailed(String reason) {
    return new HttpThroughputDto(List.of(), null, MetricStatus.QUERY_FAILED, reason);
  }

  public static HttpThroughputDto parseFailed(String reason) {
    return new HttpThroughputDto(List.of(), null, MetricStatus.PARSE_FAILED, reason);
  }
}
