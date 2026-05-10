package com.example.argus.dto.metric.jvm;

import com.example.argus.dto.metric.MetricStatus;
import com.example.argus.service.metric.mapper.MetricPointMapper.MappingResult;
import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

public sealed interface JvmMetricResult
    permits CpuUsageDto, MemoryUsageDto, GcMetricDto, ThreadMetricDto {

  MetricStatus status();

  OffsetDateTime measuredAt();

  String missingReason();

  static <T extends JvmMetricResult> T from(
      MappingResult r,
      BiFunction<BigDecimal, OffsetDateTime, T> onSuccess,
      Supplier<T> onEmpty,
      Function<String, T> onParseFailed,
      Function<String, T> onQueryFailed) {
    if (r instanceof MappingResult.Success s) {
      return onSuccess.apply(s.point().value(), s.point().timestamp());
    } else if (r instanceof MappingResult.Empty) {
      return onEmpty.get();
    } else if (r instanceof MappingResult.QueryFailed qf) {
      return onQueryFailed.apply(qf.reason());
    } else {
      return onParseFailed.apply(((MappingResult.ParseFailed) r).reason());
    }
  }
}
