package com.example.argus.dto.metric;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.argus.dto.metric.jvm.*;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

class JvmMetricSnapshotDtoTest {

  private final ObjectMapper mapper = JsonMapper.builder().build();

  @Test
  void serialize_allSuccess_containsAllTopLevelFields() throws JacksonException {
    Instant now = Instant.ofEpochSecond(1714000000L);
    OffsetDateTime measuredAt = OffsetDateTime.ofInstant(now, ZoneOffset.ofHours(9));
    OffsetDateTime collectedAt = OffsetDateTime.ofInstant(now, ZoneOffset.ofHours(9));
    JvmMetricSnapshotDto snapshot = new JvmMetricSnapshotDto(
        "myapp",
        "localhost:8080",
        collectedAt,
        CpuUsageDto.success(new BigDecimal("0.5"), measuredAt),
        MemoryUsageDto.success(new BigDecimal("65.0"), new BigDecimal("40.0"), measuredAt),
        GcMetricDto.success(new BigDecimal("0.01"), new BigDecimal("5"), measuredAt),
        ThreadMetricDto.success(10, 12, 3, measuredAt),
        SnapshotStatus.COMPLETE);

    JsonNode node = mapper.readTree(mapper.writeValueAsString(snapshot));

    assertThat(node.has("application")).isTrue();
    assertThat(node.has("instance")).isTrue();
    assertThat(node.has("collectedAt")).isTrue();
    assertThat(node.has("cpu")).isTrue();
    assertThat(node.has("memory")).isTrue();
    assertThat(node.has("gc")).isTrue();
    assertThat(node.has("thread")).isTrue();
    assertThat(node.get("status").asText()).isEqualTo("COMPLETE");
  }

  @Test
  void serialize_metricStatusSerializedAsString() throws JacksonException {
    CpuUsageDto cpu = CpuUsageDto.queryFailed("network error");

    JsonNode node = mapper.readTree(mapper.writeValueAsString(cpu));

    assertThat(node.get("status").asText()).isEqualTo("QUERY_FAILED");
    assertThat(node.get("missingReason").asText()).isEqualTo("network error");
  }

  @Test
  void serialize_failedSnapshot_nullValueFieldsPresent() throws JacksonException {
    Instant now = Instant.ofEpochSecond(1714000000L);
    OffsetDateTime collectedAt = OffsetDateTime.ofInstant(now, ZoneOffset.ofHours(9));
    JvmMetricSnapshotDto snapshot = new JvmMetricSnapshotDto(
        null,
        null,
        collectedAt,
        CpuUsageDto.queryFailed("error"),
        MemoryUsageDto.empty(),
        GcMetricDto.parseFailed("bad value"),
        ThreadMetricDto.queryFailed("error"),
        SnapshotStatus.FAILED);

    JsonNode node = mapper.readTree(mapper.writeValueAsString(snapshot));

    assertThat(node.get("status").asText()).isEqualTo("FAILED");
    assertThat(node.get("cpu").get("status").asText()).isEqualTo("QUERY_FAILED");
    assertThat(node.get("memory").get("status").asText()).isEqualTo("EMPTY_RESULT");
    assertThat(node.get("gc").get("status").asText()).isEqualTo("PARSE_FAILED");
  }

  @Test
  void recordEquality_sameFields_equal() {
    OffsetDateTime now = OffsetDateTime.ofInstant(Instant.ofEpochSecond(1714000000L), ZoneOffset.ofHours(9));
    CpuUsageDto cpu1 = CpuUsageDto.success(new BigDecimal("0.5"), now);
    CpuUsageDto cpu2 = CpuUsageDto.success(new BigDecimal("0.5"), now);

    assertThat(cpu1).isEqualTo(cpu2);
  }

  @Test
  void serialize_gcPartial_countIsNull() throws JacksonException {
    OffsetDateTime now = OffsetDateTime.ofInstant(Instant.ofEpochSecond(1714000000L), ZoneOffset.ofHours(9));
    GcMetricDto gc = GcMetricDto.partial(new BigDecimal("0.01"), null, now, "GC_COUNT: failed");

    JsonNode node = mapper.readTree(mapper.writeValueAsString(gc));

    assertThat(node.get("status").asText()).isEqualTo("PARTIAL");
    assertThat(node.get("avgDurationSeconds").decimalValue())
        .isEqualByComparingTo(new BigDecimal("0.01"));
    assertThat(node.get("count").isNull()).isTrue();
  }
}
