package com.example.argus.dto.metric;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.argus.dto.metric.http.EndpointMetricPointDto;
import com.example.argus.dto.metric.http.HttpErrorRateDto;
import com.example.argus.dto.metric.http.HttpMetricSnapshotDto;
import com.example.argus.dto.metric.http.HttpResponseTimeDto;
import com.example.argus.dto.metric.http.HttpThroughputDto;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import org.junit.jupiter.api.Test;

class HttpMetricSnapshotDtoTest {

  private final ObjectMapper mapper =
      new ObjectMapper()
          .registerModule(new JavaTimeModule())
          .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

  @Test
  void serialize_allSuccess_containsAllTopLevelFields() throws JsonProcessingException {
    Instant now = Instant.ofEpochSecond(1714000000L);
    OffsetDateTime measuredAt = OffsetDateTime.ofInstant(now, ZoneOffset.ofHours(9));
    OffsetDateTime collectedAt = OffsetDateTime.ofInstant(now, ZoneOffset.ofHours(9));
    HttpMetricSnapshotDto snapshot = new HttpMetricSnapshotDto(
        "myapp",
        "localhost:8080",
        collectedAt,
        HttpResponseTimeDto.success(
            List.of(new EndpointMetricPointDto("/api/v1/a", new BigDecimal("0.05"), measuredAt)), measuredAt),
        HttpThroughputDto.success(
            List.of(new EndpointMetricPointDto("/api/v1/a", new BigDecimal("12.5"), measuredAt)), measuredAt),
        HttpErrorRateDto.success(
            List.of(new EndpointMetricPointDto("/api/v1/a", new BigDecimal("0.01"), measuredAt)), measuredAt),
        SnapshotStatus.COMPLETE);

    JsonNode node = mapper.readTree(mapper.writeValueAsString(snapshot));

    assertThat(node.has("application")).isTrue();
    assertThat(node.has("instance")).isTrue();
    assertThat(node.has("collectedAt")).isTrue();
    assertThat(node.has("p99")).isTrue();
    assertThat(node.has("rps")).isTrue();
    assertThat(node.has("errorRate")).isTrue();
    assertThat(node.get("status").asText()).isEqualTo("COMPLETE");
    assertThat(node.get("p99").get("points").get(0).get("endpoint").asText()).isEqualTo("/api/v1/a");
  }

  @Test
  void serialize_failedSnapshot_pointsAreEmpty() throws JsonProcessingException {
    Instant now = Instant.ofEpochSecond(1714000000L);
    OffsetDateTime collectedAt = OffsetDateTime.ofInstant(now, ZoneOffset.ofHours(9));
    HttpMetricSnapshotDto snapshot = new HttpMetricSnapshotDto(
        null,
        null,
        collectedAt,
        HttpResponseTimeDto.queryFailed("network error"),
        HttpThroughputDto.queryFailed("network error"),
        HttpErrorRateDto.queryFailed("network error"),
        SnapshotStatus.FAILED);

    JsonNode node = mapper.readTree(mapper.writeValueAsString(snapshot));

    assertThat(node.get("status").asText()).isEqualTo("FAILED");
    assertThat(node.get("p99").get("status").asText()).isEqualTo("QUERY_FAILED");
    assertThat(node.get("p99").get("points").isArray()).isTrue();
    assertThat(node.get("p99").get("points")).isEmpty();
  }
}
