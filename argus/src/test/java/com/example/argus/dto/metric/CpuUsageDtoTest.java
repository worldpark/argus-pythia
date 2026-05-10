package com.example.argus.dto.metric;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.argus.dto.metric.jvm.CpuUsageDto;
import com.example.argus.dto.metric.jvm.MetricPointDto;
import com.example.argus.service.metric.mapper.MetricPointMapper.MappingResult;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;

class CpuUsageDtoTest {

  private static final OffsetDateTime TS =
      OffsetDateTime.ofInstant(Instant.ofEpochSecond(1714000000L), ZoneOffset.ofHours(9));

  @Test
  void from_success_returnsSuccessDto() {
    MetricPointDto point = new MetricPointDto("app", "inst", TS, new BigDecimal("0.5"));
    CpuUsageDto dto = CpuUsageDto.from(new MappingResult.Success(point));

    assertThat(dto.status()).isEqualTo(MetricStatus.SUCCESS);
    assertThat(dto.usagePercent()).isEqualByComparingTo(new BigDecimal("0.5"));
    assertThat(dto.measuredAt()).isEqualTo(TS);
    assertThat(dto.missingReason()).isNull();
  }

  @Test
  void from_empty_returnsEmptyResultDto() {
    CpuUsageDto dto = CpuUsageDto.from(new MappingResult.Empty());

    assertThat(dto.status()).isEqualTo(MetricStatus.EMPTY_RESULT);
    assertThat(dto.usagePercent()).isNull();
    assertThat(dto.measuredAt()).isNull();
  }

  @Test
  void from_parseFailed_returnsParseFailedDto() {
    CpuUsageDto dto = CpuUsageDto.from(new MappingResult.ParseFailed("bad value"));

    assertThat(dto.status()).isEqualTo(MetricStatus.PARSE_FAILED);
    assertThat(dto.missingReason()).contains("bad value");
    assertThat(dto.usagePercent()).isNull();
  }

  @Test
  void from_queryFailed_returnsQueryFailedDto() {
    CpuUsageDto dto = CpuUsageDto.from(new MappingResult.QueryFailed("network error"));

    assertThat(dto.status()).isEqualTo(MetricStatus.QUERY_FAILED);
    assertThat(dto.missingReason()).contains("network error");
    assertThat(dto.usagePercent()).isNull();
  }
}
