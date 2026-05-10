package com.example.argus.dto.metric;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.argus.dto.metric.hikari.HikariActiveDto;
import com.example.argus.service.metric.mapper.MetricPointMapper.LabeledPoint;
import com.example.argus.service.metric.mapper.MetricPointMapper.MultiMappingResult;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import org.junit.jupiter.api.Test;

class HikariActiveDtoTest {

  private static final OffsetDateTime TS =
      OffsetDateTime.ofInstant(Instant.ofEpochSecond(1714000000L), ZoneOffset.ofHours(9));

  @Test
  void from_success_returnsSuccessDto() {
    LabeledPoint p = new LabeledPoint("app", "inst", "HikariPool-1", TS, new BigDecimal("3"));

    HikariActiveDto dto = HikariActiveDto.from(new MultiMappingResult.Success(List.of(p)));

    assertThat(dto.status()).isEqualTo(MetricStatus.SUCCESS);
    assertThat(dto.points()).hasSize(1);
    assertThat(dto.points().get(0).pool()).isEqualTo("HikariPool-1");
    assertThat(dto.points().get(0).value()).isEqualByComparingTo(new BigDecimal("3"));
    assertThat(dto.measuredAt()).isEqualTo(TS);
  }

  @Test
  void from_empty_returnsEmptyResultDto() {
    HikariActiveDto dto = HikariActiveDto.from(new MultiMappingResult.Empty());

    assertThat(dto.status()).isEqualTo(MetricStatus.EMPTY_RESULT);
    assertThat(dto.points()).isEmpty();
  }

  @Test
  void from_parseFailed_returnsParseFailedDto() {
    HikariActiveDto dto = HikariActiveDto.from(new MultiMappingResult.ParseFailed("bad"));

    assertThat(dto.status()).isEqualTo(MetricStatus.PARSE_FAILED);
    assertThat(dto.missingReason()).contains("bad");
  }

  @Test
  void from_queryFailed_returnsQueryFailedDto() {
    HikariActiveDto dto = HikariActiveDto.from(new MultiMappingResult.QueryFailed("network"));

    assertThat(dto.status()).isEqualTo(MetricStatus.QUERY_FAILED);
    assertThat(dto.missingReason()).contains("network");
  }
}
