package com.example.argus.service.metric.snapshot;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.example.argus.dto.PrometheusResponse;
import com.example.argus.dto.metric.hikari.HikariMetricSnapshotDto;
import com.example.argus.dto.metric.MetricStatus;
import com.example.argus.dto.metric.SnapshotStatus;
import com.example.argus.exception.PrometheusQueryException;
import com.example.argus.service.PrometheusQueryService;
import com.example.argus.service.metric.MetricType;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class HikariMetricSnapshotAssemblerTest {

  @Mock private PrometheusQueryService queryService;
  private final Clock clock =
      Clock.fixed(Instant.ofEpochSecond(1714000000L), ZoneId.of("Asia/Seoul"));

  private HikariMetricSnapshotAssembler assembler;

  @BeforeEach
  void setUp() {
    assembler = new HikariMetricSnapshotAssembler(queryService, clock);
  }

  @Test
  void assemble_allSuccess_returnsCompleteSnapshot() {
    stubActive(List.of(poolSeries("myapp", "host:8080", "HikariPool-1", "3")));
    stubPending(List.of(poolSeries("myapp", "host:8080", "HikariPool-1", "0")));

    HikariMetricSnapshotDto snapshot = assembler.assemble();

    assertThat(snapshot.status()).isEqualTo(SnapshotStatus.COMPLETE);
    assertThat(snapshot.active().status()).isEqualTo(MetricStatus.SUCCESS);
    assertThat(snapshot.active().points()).hasSize(1);
    assertThat(snapshot.active().points().get(0).pool()).isEqualTo("HikariPool-1");
    assertThat(snapshot.pending().status()).isEqualTo(MetricStatus.SUCCESS);
    assertThat(snapshot.application()).isEqualTo("myapp");
    assertThat(snapshot.instance()).isEqualTo("host:8080");
  }

  @Test
  void assemble_activeQueryFails_returnsPartialAndKeepsApplicationFromPending() {
    when(queryService.queryByMetric(MetricType.HIKARI_ACTIVE_CONNECTIONS))
        .thenThrow(new PrometheusQueryException("network error"));
    stubPending(List.of(poolSeries("myapp", "host:8080", "HikariPool-1", "0")));

    HikariMetricSnapshotDto snapshot = assembler.assemble();

    assertThat(snapshot.status()).isEqualTo(SnapshotStatus.PARTIAL);
    assertThat(snapshot.active().status()).isEqualTo(MetricStatus.QUERY_FAILED);
    assertThat(snapshot.application()).isEqualTo("myapp");
    assertThat(snapshot.instance()).isEqualTo("host:8080");
  }

  @Test
  void assemble_bothFail_returnsFailedSnapshot() {
    when(queryService.queryByMetric(MetricType.HIKARI_ACTIVE_CONNECTIONS))
        .thenThrow(new PrometheusQueryException("active failed"));
    when(queryService.queryByMetric(MetricType.HIKARI_PENDING_CONNECTIONS))
        .thenThrow(new PrometheusQueryException("pending failed"));

    HikariMetricSnapshotDto snapshot = assembler.assemble();

    assertThat(snapshot.status()).isEqualTo(SnapshotStatus.FAILED);
    assertThat(snapshot.active().status()).isEqualTo(MetricStatus.QUERY_FAILED);
    assertThat(snapshot.pending().status()).isEqualTo(MetricStatus.QUERY_FAILED);
    assertThat(snapshot.application()).isNull();
  }

  @Test
  void assemble_pendingEmptyResult_returnsPartialWithEmptyPending() {
    stubActive(List.of(poolSeries("myapp", "host:8080", "HikariPool-1", "3")));
    stubEmpty(MetricType.HIKARI_PENDING_CONNECTIONS);

    HikariMetricSnapshotDto snapshot = assembler.assemble();

    assertThat(snapshot.pending().status()).isEqualTo(MetricStatus.EMPTY_RESULT);
    assertThat(snapshot.pending().points()).isEmpty();
    assertThat(snapshot.status()).isEqualTo(SnapshotStatus.PARTIAL);
  }

  @Test
  void assemble_multiplePoolsInActive_allRetained() {
    stubActive(List.of(
        poolSeries("myapp", "host:8080", "HikariPool-1", "3"),
        poolSeries("myapp", "host:8080", "ReportingPool", "1")));
    stubPending(List.of(poolSeries("myapp", "host:8080", "HikariPool-1", "0")));

    HikariMetricSnapshotDto snapshot = assembler.assemble();

    assertThat(snapshot.active().points()).hasSize(2);
    assertThat(snapshot.active().points()).extracting(p -> p.pool())
        .containsExactly("HikariPool-1", "ReportingPool");
  }

  // --- helpers ---

  private void stubActive(List<PrometheusResponse.Result> results) {
    when(queryService.queryByMetric(MetricType.HIKARI_ACTIVE_CONNECTIONS))
        .thenReturn(buildResponse(results));
    when(queryService.queryByMetric(MetricType.HIKARI_USAGE_RATIO))
        .thenReturn(buildResponse(results));
  }

  private void stubPending(List<PrometheusResponse.Result> results) {
    when(queryService.queryByMetric(MetricType.HIKARI_PENDING_CONNECTIONS))
        .thenReturn(buildResponse(results));
  }

  private void stubEmpty(MetricType type) {
    when(queryService.queryByMetric(type)).thenReturn(buildResponse(List.of()));
  }

  private PrometheusResponse.Result poolSeries(
      String application, String instance, String pool, String value) {
    Map<String, String> labels = new HashMap<>();
    labels.put("application", application);
    labels.put("instance", instance);
    labels.put("pool", pool);

    PrometheusResponse.Result result = new PrometheusResponse.Result();
    result.setMetric(labels);
    result.setValue(List.of(1714000000L, value));
    return result;
  }

  private PrometheusResponse buildResponse(List<PrometheusResponse.Result> results) {
    PrometheusResponse.Datas data = new PrometheusResponse.Datas();
    data.setResultType("vector");
    data.setResult(results);

    PrometheusResponse response = new PrometheusResponse();
    response.setStatus("success");
    response.setData(data);
    return response;
  }
}
