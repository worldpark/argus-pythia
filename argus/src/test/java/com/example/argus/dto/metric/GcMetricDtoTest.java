package com.example.argus.dto.metric;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.argus.dto.metric.jvm.GcMetricDto;
import com.example.argus.dto.metric.jvm.MetricPointDto;
import com.example.argus.service.metric.mapper.MetricPointMapper.MappingResult;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;

class GcMetricDtoTest {

  private static final OffsetDateTime TS =
      OffsetDateTime.ofInstant(Instant.ofEpochSecond(1714000000L), ZoneOffset.ofHours(9));

  private MappingResult success(String value) {
    MetricPointDto point = new MetricPointDto("app", "inst", TS, new BigDecimal(value));
    return new MappingResult.Success(point);
  }

  @Test
  void from_bothSuccess_returnsSuccessDto() {
    GcMetricDto dto = GcMetricDto.from(success("0.01"), success("5"));

    assertThat(dto.status()).isEqualTo(MetricStatus.SUCCESS);
    assertThat(dto.avgDurationSeconds()).isEqualByComparingTo(new BigDecimal("0.01"));
    assertThat(dto.count()).isEqualByComparingTo(new BigDecimal("5"));
    assertThat(dto.measuredAt()).isEqualTo(TS);
    assertThat(dto.missingReason()).isNull();
  }

  @Test
  void from_durationSuccessCountEmpty_returnsPartialWithDurationOnly() {
    GcMetricDto dto = GcMetricDto.from(success("0.01"), new MappingResult.Empty());

    assertThat(dto.status()).isEqualTo(MetricStatus.PARTIAL);
    assertThat(dto.avgDurationSeconds()).isNotNull();
    assertThat(dto.count()).isNull();
    assertThat(dto.missingReason()).contains("GC_COUNT");
  }

  @Test
  void from_durationEmptyCountSuccess_returnsPartialWithCountOnly() {
    GcMetricDto dto = GcMetricDto.from(new MappingResult.Empty(), success("5"));

    assertThat(dto.status()).isEqualTo(MetricStatus.PARTIAL);
    assertThat(dto.avgDurationSeconds()).isNull();
    assertThat(dto.count()).isNotNull();
    assertThat(dto.missingReason()).contains("GC_AVG_DURATION");
  }

  @Test
  void from_durationSuccessCountQueryFailed_returnsPartialWithDurationOnly() {
    GcMetricDto dto = GcMetricDto.from(success("0.01"), new MappingResult.QueryFailed("count error"));

    assertThat(dto.status()).isEqualTo(MetricStatus.PARTIAL);
    assertThat(dto.avgDurationSeconds()).isNotNull();
    assertThat(dto.count()).isNull();
    assertThat(dto.missingReason()).contains("count error");
  }

  @Test
  void from_durationQueryFailedCountSuccess_returnsPartialWithCountOnly() {
    GcMetricDto dto = GcMetricDto.from(new MappingResult.QueryFailed("dur error"), success("5"));

    assertThat(dto.status()).isEqualTo(MetricStatus.PARTIAL);
    assertThat(dto.avgDurationSeconds()).isNull();
    assertThat(dto.count()).isNotNull();
    assertThat(dto.missingReason()).contains("dur error");
  }

  @Test
  void from_bothQueryFailed_returnsQueryFailed() {
    GcMetricDto dto = GcMetricDto.from(
        new MappingResult.QueryFailed("dur failed"),
        new MappingResult.QueryFailed("cnt failed"));

    assertThat(dto.status()).isEqualTo(MetricStatus.QUERY_FAILED);
    assertThat(dto.missingReason()).contains("dur failed");
    assertThat(dto.missingReason()).contains("cnt failed");
  }

  @Test
  void from_queryFailedAndEmpty_queryFailedTakesPrecedence() {
    GcMetricDto dto = GcMetricDto.from(
        new MappingResult.QueryFailed("dur failed"),
        new MappingResult.Empty());

    assertThat(dto.status()).isEqualTo(MetricStatus.QUERY_FAILED);
    assertThat(dto.missingReason()).contains("dur failed");
  }

  @Test
  void from_bothEmpty_returnsEmptyResultDto() {
    GcMetricDto dto = GcMetricDto.from(new MappingResult.Empty(), new MappingResult.Empty());

    assertThat(dto.status()).isEqualTo(MetricStatus.EMPTY_RESULT);
    assertThat(dto.avgDurationSeconds()).isNull();
    assertThat(dto.count()).isNull();
  }

  @Test
  void from_durationParseFailedCountEmpty_returnsParseFailedDto() {
    GcMetricDto dto = GcMetricDto.from(
        new MappingResult.ParseFailed("bad duration"), new MappingResult.Empty());

    assertThat(dto.status()).isEqualTo(MetricStatus.PARSE_FAILED);
    assertThat(dto.missingReason()).contains("bad duration");
    assertThat(dto.missingReason()).contains("GC_AVG_DURATION");
  }

  @Test
  void from_bothParseFailed_returnsParseFailedWithBothReasons() {
    GcMetricDto dto = GcMetricDto.from(
        new MappingResult.ParseFailed("dur error"),
        new MappingResult.ParseFailed("cnt error"));

    assertThat(dto.status()).isEqualTo(MetricStatus.PARSE_FAILED);
    assertThat(dto.missingReason()).contains("dur error");
    assertThat(dto.missingReason()).contains("cnt error");
  }
}
