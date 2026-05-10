package com.example.argus.service.metric.snapshot;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.example.argus.dto.PrometheusResponse;
import com.example.argus.dto.metric.http.HttpMetricSnapshotDto;
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
class HttpMetricSnapshotAssemblerTest {

  @Mock private PrometheusQueryService queryService;
  private final Clock clock =
      Clock.fixed(Instant.ofEpochSecond(1714000000L), ZoneId.of("Asia/Seoul"));

  private HttpMetricSnapshotAssembler assembler;

  @BeforeEach
  void setUp() {
    assembler = new HttpMetricSnapshotAssembler(queryService, clock);
  }

  @Test
  void assemble_allSuccess_returnsCompleteSnapshot() {
    stubP99(List.of(
        endpointSeries("myapp", "host:8080", "/api/v1/a", "0.05"),
        endpointSeries("myapp", "host:8080", "/api/v1/b", "0.10")));
    stubRps(List.of(
        endpointSeriesNoAppInstance("/api/v1/a", "12.5"),
        endpointSeriesNoAppInstance("/api/v1/b", "8.0")));
    stubErrorRate(List.of(endpointSeries("myapp", "host:8080", "/api/v1/a", "0.01")));

    HttpMetricSnapshotDto snapshot = assembler.assemble();

    assertThat(snapshot.status()).isEqualTo(SnapshotStatus.COMPLETE);
    assertThat(snapshot.p99().status()).isEqualTo(MetricStatus.SUCCESS);
    assertThat(snapshot.p99().points()).hasSize(2);
    assertThat(snapshot.rps().status()).isEqualTo(MetricStatus.SUCCESS);
    assertThat(snapshot.rps().points()).hasSize(2);
    assertThat(snapshot.errorRate().status()).isEqualTo(MetricStatus.SUCCESS);
    assertThat(snapshot.application()).isEqualTo("myapp");
    assertThat(snapshot.instance()).isEqualTo("host:8080");
  }

  @Test
  void assemble_p99QueryFails_returnsPartialAndApplicationNull() {
    when(queryService.queryByMetric(MetricType.HTTP_P99_RESPONSE_TIME))
        .thenThrow(new PrometheusQueryException("network error"));
    stubRps(List.of(endpointSeriesNoAppInstance("/api/v1/a", "12.5")));
    stubErrorRate(List.of(endpointSeries("myapp", "host:8080", "/api/v1/a", "0.01")));

    HttpMetricSnapshotDto snapshot = assembler.assemble();

    assertThat(snapshot.status()).isEqualTo(SnapshotStatus.PARTIAL);
    assertThat(snapshot.p99().status()).isEqualTo(MetricStatus.QUERY_FAILED);
    assertThat(snapshot.p99().missingReason()).contains("network error");
    assertThat(snapshot.rps().status()).isEqualTo(MetricStatus.SUCCESS);
    assertThat(snapshot.application()).isEqualTo("myapp");
    assertThat(snapshot.instance()).isEqualTo("host:8080");
  }

  @Test
  void assemble_rpsQueryFails_returnsPartialAndKeepsApplicationFromP99() {
    stubP99(List.of(endpointSeries("myapp", "host:8080", "/api/v1/a", "0.05")));
    when(queryService.queryByMetric(MetricType.HTTP_RPS))
        .thenThrow(new PrometheusQueryException("rps failed"));
    stubErrorRate(List.of(endpointSeries("myapp", "host:8080", "/api/v1/a", "0.01")));

    HttpMetricSnapshotDto snapshot = assembler.assemble();

    assertThat(snapshot.status()).isEqualTo(SnapshotStatus.PARTIAL);
    assertThat(snapshot.rps().status()).isEqualTo(MetricStatus.QUERY_FAILED);
    assertThat(snapshot.application()).isEqualTo("myapp");
  }

  @Test
  void assemble_bothFail_returnsFailedSnapshot() {
    when(queryService.queryByMetric(MetricType.HTTP_P99_RESPONSE_TIME))
        .thenThrow(new PrometheusQueryException("p99 failed"));
    when(queryService.queryByMetric(MetricType.HTTP_RPS))
        .thenThrow(new PrometheusQueryException("rps failed"));
    when(queryService.queryByMetric(MetricType.HTTP_ERROR_RATE))
        .thenThrow(new PrometheusQueryException("error rate failed"));

    HttpMetricSnapshotDto snapshot = assembler.assemble();

    assertThat(snapshot.status()).isEqualTo(SnapshotStatus.FAILED);
    assertThat(snapshot.p99().status()).isEqualTo(MetricStatus.QUERY_FAILED);
    assertThat(snapshot.rps().status()).isEqualTo(MetricStatus.QUERY_FAILED);
    assertThat(snapshot.application()).isNull();
  }

  @Test
  void assemble_p99EmptyResult_returnsPartialWithEmptyP99() {
    stubEmpty(MetricType.HTTP_P99_RESPONSE_TIME);
    stubRps(List.of(endpointSeriesNoAppInstance("/api/v1/a", "12.5")));
    stubErrorRate(List.of(endpointSeries("myapp", "host:8080", "/api/v1/a", "0.01")));

    HttpMetricSnapshotDto snapshot = assembler.assemble();

    assertThat(snapshot.p99().status()).isEqualTo(MetricStatus.EMPTY_RESULT);
    assertThat(snapshot.p99().points()).isEmpty();
    assertThat(snapshot.status()).isEqualTo(SnapshotStatus.PARTIAL);
  }

  // --- helpers ---

  private void stubP99(List<PrometheusResponse.Result> results) {
    when(queryService.queryByMetric(MetricType.HTTP_P99_RESPONSE_TIME))
        .thenReturn(buildResponse(results));
  }

  private void stubRps(List<PrometheusResponse.Result> results) {
    when(queryService.queryByMetric(MetricType.HTTP_RPS))
        .thenReturn(buildResponse(results));
  }

  private void stubErrorRate(List<PrometheusResponse.Result> results) {
    when(queryService.queryByMetric(MetricType.HTTP_ERROR_RATE))
        .thenReturn(buildResponse(results));
  }

  private void stubEmpty(MetricType type) {
    when(queryService.queryByMetric(type)).thenReturn(buildResponse(List.of()));
  }

  private PrometheusResponse.Result endpointSeries(
      String application, String instance, String uri, String value) {
    Map<String, String> labels = new HashMap<>();
    labels.put("application", application);
    labels.put("instance", instance);
    labels.put("uri", uri);
    return seriesWithLabels(labels, value);
  }

  private PrometheusResponse.Result endpointSeriesNoAppInstance(String uri, String value) {
    return seriesWithLabels(Map.of("uri", uri), value);
  }

  private PrometheusResponse.Result seriesWithLabels(Map<String, String> labels, String value) {
    PrometheusResponse.Result result = new PrometheusResponse.Result();
    result.setMetric(new HashMap<>(labels));
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
