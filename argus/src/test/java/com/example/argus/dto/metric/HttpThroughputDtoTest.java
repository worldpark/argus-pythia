package com.example.argus.dto.metric;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.argus.dto.metric.http.HttpThroughputDto;
import com.example.argus.service.metric.mapper.MetricPointMapper.LabeledPoint;
import com.example.argus.service.metric.mapper.MetricPointMapper.MultiMappingResult;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import org.junit.jupiter.api.Test;

class HttpThroughputDtoTest {

  private static final OffsetDateTime TS =
      OffsetDateTime.ofInstant(Instant.ofEpochSecond(1714000000L), ZoneOffset.ofHours(9));

  @Test
  void from_success_returnsSuccessDto() {
    LabeledPoint p = new LabeledPoint(null, null, "/api/v1/x", TS, new BigDecimal("12.5"));

    HttpThroughputDto dto = HttpThroughputDto.from(new MultiMappingResult.Success(List.of(p)));

    assertThat(dto.status()).isEqualTo(MetricStatus.SUCCESS);
    assertThat(dto.points()).hasSize(1);
    assertThat(dto.points().get(0).endpoint()).isEqualTo("/api/v1/x");
    assertThat(dto.points().get(0).value()).isEqualByComparingTo(new BigDecimal("12.5"));
  }

  @Test
  void from_empty_returnsEmptyResultDto() {
    HttpThroughputDto dto = HttpThroughputDto.from(new MultiMappingResult.Empty());

    assertThat(dto.status()).isEqualTo(MetricStatus.EMPTY_RESULT);
    assertThat(dto.points()).isEmpty();
  }

  @Test
  void from_parseFailed_returnsParseFailedDto() {
    HttpThroughputDto dto = HttpThroughputDto.from(new MultiMappingResult.ParseFailed("nope"));

    assertThat(dto.status()).isEqualTo(MetricStatus.PARSE_FAILED);
    assertThat(dto.missingReason()).contains("nope");
  }

  @Test
  void from_queryFailed_returnsQueryFailedDto() {
    HttpThroughputDto dto = HttpThroughputDto.from(new MultiMappingResult.QueryFailed("boom"));

    assertThat(dto.status()).isEqualTo(MetricStatus.QUERY_FAILED);
    assertThat(dto.missingReason()).contains("boom");
  }
}
