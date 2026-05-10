package com.example.argus.dto.metric.http;

import com.example.argus.dto.metric.MetricStatus;
import com.example.argus.service.metric.mapper.MetricPointMapper.LabeledPoint;
import com.example.argus.service.metric.mapper.MetricPointMapper.MultiMappingResult;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

public sealed interface HttpMetricResult
    permits HttpResponseTimeDto, HttpThroughputDto, HttpErrorRateDto {

  MetricStatus status();

  OffsetDateTime measuredAt();

  String missingReason();

  static <T extends HttpMetricResult> T from(
      MultiMappingResult r,
      BiFunction<List<EndpointMetricPointDto>, OffsetDateTime, T> onSuccess,
      Supplier<T> onEmpty,
      Function<String, T> onParseFailed,
      Function<String, T> onQueryFailed) {
    if (r instanceof MultiMappingResult.Success s) {
      List<EndpointMetricPointDto> points = s.points().stream()
          .map(p -> new EndpointMetricPointDto(p.identifier(), p.value(), p.timestamp()))
          .toList();
      OffsetDateTime measuredAt = s.points().stream()
          .map(LabeledPoint::timestamp)
          .findFirst()
          .orElse(null);
      return onSuccess.apply(points, measuredAt);
    } else if (r instanceof MultiMappingResult.Empty) {
      return onEmpty.get();
    } else if (r instanceof MultiMappingResult.QueryFailed qf) {
      return onQueryFailed.apply(qf.reason());
    } else {
      return onParseFailed.apply(((MultiMappingResult.ParseFailed) r).reason());
    }
  }
}
