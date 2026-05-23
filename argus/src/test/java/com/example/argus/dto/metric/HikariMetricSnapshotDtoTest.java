package com.example.argus.dto.metric;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.argus.dto.metric.hikari.HikariActiveDto;
import com.example.argus.dto.metric.hikari.HikariMetricSnapshotDto;
import com.example.argus.dto.metric.hikari.HikariPendingDto;
import com.example.argus.dto.metric.hikari.PoolMetricPointDto;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import org.junit.jupiter.api.Test;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

class HikariMetricSnapshotDtoTest {

  private final ObjectMapper mapper = JsonMapper.builder().build();

  @Test
  void serialize_allSuccess_containsAllTopLevelFields() throws JacksonException {
    Instant now = Instant.ofEpochSecond(1714000000L);
    OffsetDateTime measuredAt = OffsetDateTime.ofInstant(now, ZoneOffset.ofHours(9));
    OffsetDateTime collectedAt = OffsetDateTime.ofInstant(now, ZoneOffset.ofHours(9));
    HikariMetricSnapshotDto snapshot = new HikariMetricSnapshotDto(
        "myapp",
        "localhost:8080",
        collectedAt,
        HikariActiveDto.success(
            List.of(new PoolMetricPointDto("HikariPool-1", new BigDecimal("3"), measuredAt)),
            List.of(new PoolMetricPointDto("HikariPool-1", new BigDecimal("0.3"), measuredAt)),
            measuredAt),
        HikariPendingDto.success(
            List.of(new PoolMetricPointDto("HikariPool-1", new BigDecimal("0"), measuredAt)), measuredAt),
        SnapshotStatus.COMPLETE);

    JsonNode node = mapper.readTree(mapper.writeValueAsString(snapshot));

    assertThat(node.has("application")).isTrue();
    assertThat(node.has("instance")).isTrue();
    assertThat(node.has("collectedAt")).isTrue();
    assertThat(node.has("active")).isTrue();
    assertThat(node.has("pending")).isTrue();
    assertThat(node.get("status").asText()).isEqualTo("COMPLETE");
    assertThat(node.get("active").get("points").get(0).get("pool").asText()).isEqualTo("HikariPool-1");
  }

  @Test
  void serialize_partialSnapshot_failedFieldHasEmptyPoints() throws JacksonException {
    Instant now = Instant.ofEpochSecond(1714000000L);
    OffsetDateTime measuredAt = OffsetDateTime.ofInstant(now, ZoneOffset.ofHours(9));
    OffsetDateTime collectedAt = OffsetDateTime.ofInstant(now, ZoneOffset.ofHours(9));
    HikariMetricSnapshotDto snapshot = new HikariMetricSnapshotDto(
        "myapp",
        "localhost:8080",
        collectedAt,
        HikariActiveDto.success(
            List.of(new PoolMetricPointDto("HikariPool-1", new BigDecimal("3"), measuredAt)),
            List.of(new PoolMetricPointDto("HikariPool-1", new BigDecimal("0.3"), measuredAt)),
            measuredAt),
        HikariPendingDto.queryFailed("network error"),
        SnapshotStatus.PARTIAL);

    JsonNode node = mapper.readTree(mapper.writeValueAsString(snapshot));

    assertThat(node.get("status").asText()).isEqualTo("PARTIAL");
    assertThat(node.get("pending").get("status").asText()).isEqualTo("QUERY_FAILED");
    assertThat(node.get("pending").get("points")).isEmpty();
  }
}
