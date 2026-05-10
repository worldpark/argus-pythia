package com.example.argus.dto.metric;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.argus.dto.metric.http.HttpResponseTimeDto;
import com.example.argus.service.metric.mapper.MetricPointMapper.LabeledPoint;
import com.example.argus.service.metric.mapper.MetricPointMapper.MultiMappingResult;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import org.junit.jupiter.api.Test;

class HttpResponseTimeDtoTest {

  private static final OffsetDateTime TS =
      OffsetDateTime.ofInstant(Instant.ofEpochSecond(1714000000L), ZoneOffset.ofHours(9));

  @Test
  void from_success_returnsSuccessDto() {
    LabeledPoint p1 = new LabeledPoint("app", "inst", "/api/v1/a", TS, new BigDecimal("0.05"));
    LabeledPoint p2 = new LabeledPoint("app", "inst", "/api/v1/b", TS, new BigDecimal("0.10"));

    HttpResponseTimeDto dto = HttpResponseTimeDto.from(
        new MultiMappingResult.Success(List.of(p1, p2)));

    assertThat(dto.status()).isEqualTo(MetricStatus.SUCCESS);
    assertThat(dto.points()).hasSize(2);
    assertThat(dto.points().get(0).endpoint()).isEqualTo("/api/v1/a");
    assertThat(dto.points().get(0).value()).isEqualByComparingTo(new BigDecimal("0.05"));
    assertThat(dto.measuredAt()).isEqualTo(TS);
    assertThat(dto.missingReason()).isNull();
  }

  @Test
  void from_empty_returnsEmptyResultDto() {
    HttpResponseTimeDto dto = HttpResponseTimeDto.from(new MultiMappingResult.Empty());

    assertThat(dto.status()).isEqualTo(MetricStatus.EMPTY_RESULT);
    assertThat(dto.points()).isEmpty();
    assertThat(dto.measuredAt()).isNull();
  }

  @Test
  void from_parseFailed_returnsParseFailedDto() {
    HttpResponseTimeDto dto = HttpResponseTimeDto.from(new MultiMappingResult.ParseFailed("bad"));

    assertThat(dto.status()).isEqualTo(MetricStatus.PARSE_FAILED);
    assertThat(dto.missingReason()).contains("bad");
    assertThat(dto.points()).isEmpty();
  }

  @Test
  void from_queryFailed_returnsQueryFailedDto() {
    HttpResponseTimeDto dto = HttpResponseTimeDto.from(
        new MultiMappingResult.QueryFailed("network error"));

    assertThat(dto.status()).isEqualTo(MetricStatus.QUERY_FAILED);
    assertThat(dto.missingReason()).contains("network error");
    assertThat(dto.points()).isEmpty();
  }
}
