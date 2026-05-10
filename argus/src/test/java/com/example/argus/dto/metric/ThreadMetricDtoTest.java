package com.example.argus.dto.metric;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.argus.dto.metric.jvm.MetricPointDto;
import com.example.argus.dto.metric.jvm.ThreadMetricDto;
import com.example.argus.service.metric.mapper.MetricPointMapper.MappingResult;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;

class ThreadMetricDtoTest {

  private static final OffsetDateTime TS =
      OffsetDateTime.ofInstant(Instant.ofEpochSecond(1714000000L), ZoneOffset.ofHours(9));

  @Test
  void from_success_returnsSuccessDtoWithIntValue() {
    MetricPointDto point = new MetricPointDto("app", "inst", TS, new BigDecimal("10"));
    ThreadMetricDto dto = ThreadMetricDto.from(new MappingResult.Success(point));

    assertThat(dto.status()).isEqualTo(MetricStatus.SUCCESS);
    assertThat(dto.activeCount()).isEqualTo(10);
    assertThat(dto.measuredAt()).isEqualTo(TS);
    assertThat(dto.missingReason()).isNull();
  }

  @Test
  void from_successFractionalValue_returnsParseFailedDto() {
    MetricPointDto point = new MetricPointDto("app", "inst", TS, new BigDecimal("10.5"));
    ThreadMetricDto dto = ThreadMetricDto.from(new MappingResult.Success(point));

    assertThat(dto.status()).isEqualTo(MetricStatus.PARSE_FAILED);
    assertThat(dto.missingReason()).contains("cannot convert to int");
    assertThat(dto.activeCount()).isNull();
  }

  @Test
  void from_empty_returnsEmptyResultDto() {
    ThreadMetricDto dto = ThreadMetricDto.from(new MappingResult.Empty());

    assertThat(dto.status()).isEqualTo(MetricStatus.EMPTY_RESULT);
    assertThat(dto.activeCount()).isNull();
  }

  @Test
  void from_parseFailed_returnsParseFailedDto() {
    ThreadMetricDto dto = ThreadMetricDto.from(new MappingResult.ParseFailed("bad format"));

    assertThat(dto.status()).isEqualTo(MetricStatus.PARSE_FAILED);
    assertThat(dto.missingReason()).contains("bad format");
    assertThat(dto.activeCount()).isNull();
  }

  @Test
  void from_queryFailed_returnsQueryFailedDto() {
    ThreadMetricDto dto = ThreadMetricDto.from(new MappingResult.QueryFailed("connection refused"));

    assertThat(dto.status()).isEqualTo(MetricStatus.QUERY_FAILED);
    assertThat(dto.missingReason()).contains("connection refused");
    assertThat(dto.activeCount()).isNull();
  }
}
