package com.example.argus.dto.metric.http;

import com.example.argus.dto.metric.MetricStatus;
import com.example.argus.service.metric.mapper.MetricPointMapper.MultiMappingResult;
import java.time.OffsetDateTime;
import java.util.List;

public record HttpErrorRateDto(
    List<EndpointMetricPointDto> points,
    OffsetDateTime measuredAt,
    MetricStatus status,
    String missingReason)
    implements HttpMetricResult {

  public static HttpErrorRateDto from(MultiMappingResult result) {
    return HttpMetricResult.from(result,
        HttpErrorRateDto::success,
        HttpErrorRateDto::empty,
        HttpErrorRateDto::parseFailed,
        HttpErrorRateDto::queryFailed);
  }

  public static HttpErrorRateDto success(List<EndpointMetricPointDto> points, OffsetDateTime measuredAt) {
    return new HttpErrorRateDto(points, measuredAt, MetricStatus.SUCCESS, null);
  }

  public static HttpErrorRateDto empty() {
    return new HttpErrorRateDto(List.of(), null, MetricStatus.EMPTY_RESULT, "empty result");
  }

  public static HttpErrorRateDto queryFailed(String reason) {
    return new HttpErrorRateDto(List.of(), null, MetricStatus.QUERY_FAILED, reason);
  }

  public static HttpErrorRateDto parseFailed(String reason) {
    return new HttpErrorRateDto(List.of(), null, MetricStatus.PARSE_FAILED, reason);
  }
}
