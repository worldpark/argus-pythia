package com.example.argus.dto.metric;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.argus.dto.metric.jvm.MemoryUsageDto;
import com.example.argus.dto.metric.jvm.MetricPointDto;
import com.example.argus.service.metric.mapper.MetricPointMapper.MappingResult;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;

class MemoryUsageDtoTest {

  private static final OffsetDateTime TS =
      OffsetDateTime.ofInstant(Instant.ofEpochSecond(1714000000L), ZoneOffset.ofHours(9));

  @Test
  void from_success_returnsSuccessDto() {
    MetricPointDto point = new MetricPointDto("app", "inst", TS, new BigDecimal("65.0"));
    MemoryUsageDto dto = MemoryUsageDto.from(new MappingResult.Success(point));

    assertThat(dto.status()).isEqualTo(MetricStatus.SUCCESS);
    assertThat(dto.heapUsagePercent()).isEqualByComparingTo(new BigDecimal("65.0"));
    assertThat(dto.measuredAt()).isEqualTo(TS);
    assertThat(dto.missingReason()).isNull();
  }

  @Test
  void from_empty_returnsEmptyResultDto() {
    MemoryUsageDto dto = MemoryUsageDto.from(new MappingResult.Empty());

    assertThat(dto.status()).isEqualTo(MetricStatus.EMPTY_RESULT);
    assertThat(dto.heapUsagePercent()).isNull();
    assertThat(dto.measuredAt()).isNull();
  }

  @Test
  void from_parseFailed_returnsParseFailedDto() {
    MemoryUsageDto dto = MemoryUsageDto.from(new MappingResult.ParseFailed("nan value"));

    assertThat(dto.status()).isEqualTo(MetricStatus.PARSE_FAILED);
    assertThat(dto.missingReason()).contains("nan value");
    assertThat(dto.heapUsagePercent()).isNull();
  }

  @Test
  void from_queryFailed_returnsQueryFailedDto() {
    MemoryUsageDto dto = MemoryUsageDto.from(new MappingResult.QueryFailed("timeout"));

    assertThat(dto.status()).isEqualTo(MetricStatus.QUERY_FAILED);
    assertThat(dto.missingReason()).contains("timeout");
    assertThat(dto.heapUsagePercent()).isNull();
  }
}
