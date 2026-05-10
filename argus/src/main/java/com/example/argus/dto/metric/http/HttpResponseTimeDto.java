package com.example.argus.dto.metric.http;

import com.example.argus.dto.metric.MetricStatus;
import com.example.argus.service.metric.mapper.MetricPointMapper.MultiMappingResult;
import java.time.OffsetDateTime;
import java.util.List;

public record HttpResponseTimeDto(
    List<EndpointMetricPointDto> points,
    OffsetDateTime measuredAt,
    MetricStatus status,
    String missingReason)
    implements HttpMetricResult {

  public static HttpResponseTimeDto from(MultiMappingResult result) {
    return HttpMetricResult.from(result,
        HttpResponseTimeDto::success,
        HttpResponseTimeDto::empty,
        HttpResponseTimeDto::parseFailed,
        HttpResponseTimeDto::queryFailed);
  }

  public static HttpResponseTimeDto success(List<EndpointMetricPointDto> points, OffsetDateTime measuredAt) {
    return new HttpResponseTimeDto(points, measuredAt, MetricStatus.SUCCESS, null);
  }

  public static HttpResponseTimeDto empty() {
    return new HttpResponseTimeDto(List.of(), null, MetricStatus.EMPTY_RESULT, "empty result");
  }

  public static HttpResponseTimeDto queryFailed(String reason) {
    return new HttpResponseTimeDto(List.of(), null, MetricStatus.QUERY_FAILED, reason);
  }

  public static HttpResponseTimeDto parseFailed(String reason) {
    return new HttpResponseTimeDto(List.of(), null, MetricStatus.PARSE_FAILED, reason);
  }
}
