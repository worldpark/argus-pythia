package com.example.argus.service.metric.mapper;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.argus.dto.PrometheusResponse;
import com.example.argus.dto.metric.jvm.MetricPointDto;
import com.example.argus.service.metric.MetricType;
import com.example.argus.service.metric.mapper.MetricPointMapper.LabeledPoint;
import com.example.argus.service.metric.mapper.MetricPointMapper.MappingResult;
import com.example.argus.service.metric.mapper.MetricPointMapper.MultiMappingResult;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class MetricPointMapperTest {

  @Test
  void toPoint_validResponse_returnsSuccess() {
    PrometheusResponse response = buildResponse("myapp", "localhost:8080", "0.75");

    MappingResult result = MetricPointMapper.toPoint(response, MetricType.CPU_USAGE);

    assertThat(result).isInstanceOf(MappingResult.Success.class);
    MetricPointDto point = ((MappingResult.Success) result).point();
    assertThat(point.value()).isEqualByComparingTo(new BigDecimal("0.75"));
    assertThat(point.application()).isEqualTo("myapp");
    assertThat(point.instance()).isEqualTo("localhost:8080");
    assertThat(point.timestamp()).isEqualTo(
        OffsetDateTime.ofInstant(Instant.ofEpochSecond(1714000000L), ZoneId.of("Asia/Seoul")));
  }

  @Test
  void toPoint_labelsExtractedToFields() {
    PrometheusResponse response = buildResponse("svc", "host:9090", "1.0");

    MappingResult result = MetricPointMapper.toPoint(response, MetricType.CPU_USAGE);

    assertThat(result).isInstanceOf(MappingResult.Success.class);
    MetricPointDto point = ((MappingResult.Success) result).point();
    assertThat(point.application()).isEqualTo("svc");
    assertThat(point.instance()).isEqualTo("host:9090");
  }

  @Test
  void toPoint_nullData_returnsEmpty() {
    PrometheusResponse response = new PrometheusResponse();
    response.setStatus("success");
    response.setData(null);

    MappingResult result = MetricPointMapper.toPoint(response, MetricType.CPU_USAGE);

    assertThat(result).isInstanceOf(MappingResult.Empty.class);
  }

  @Test
  void toPoint_emptyResultList_returnsEmpty() {
    MappingResult result = MetricPointMapper.toPoint(
        buildResponseWithResults(List.of()), MetricType.CPU_USAGE);

    assertThat(result).isInstanceOf(MappingResult.Empty.class);
  }

  @Test
  void toPoint_nanValue_returnsParseFailed() {
    MappingResult result = MetricPointMapper.toPoint(
        buildResponse(null, null, "NaN"), MetricType.CPU_USAGE);

    assertThat(result).isInstanceOf(MappingResult.ParseFailed.class);
    assertThat(((MappingResult.ParseFailed) result).reason()).contains("NaN");
  }

  @Test
  void toPoint_infValue_returnsParseFailed() {
    MappingResult result = MetricPointMapper.toPoint(
        buildResponse(null, null, "Inf"), MetricType.CPU_USAGE);

    assertThat(result).isInstanceOf(MappingResult.ParseFailed.class);
  }

  @Test
  void toPoint_negativeInfValue_returnsParseFailed() {
    MappingResult result = MetricPointMapper.toPoint(
        buildResponse(null, null, "-Inf"), MetricType.CPU_USAGE);

    assertThat(result).isInstanceOf(MappingResult.ParseFailed.class);
  }

  @Test
  void toPoint_malformedValue_returnsParseFailed() {
    MappingResult result = MetricPointMapper.toPoint(
        buildResponse(null, null, "not-a-number"), MetricType.CPU_USAGE);

    assertThat(result).isInstanceOf(MappingResult.ParseFailed.class);
  }

  @Test
  void toPoint_noApplicationLabel_applicationAndInstanceAreNull() {
    MappingResult result = MetricPointMapper.toPoint(
        buildResponseWithMetricLabels(Map.of(), "42.0"), MetricType.HEAP_USAGE);

    assertThat(result).isInstanceOf(MappingResult.Success.class);
    MetricPointDto point = ((MappingResult.Success) result).point();
    assertThat(point.application()).isNull();
    assertThat(point.instance()).isNull();
  }

  @Test
  void toPoint_extraLabelsInResponse_parsedSuccessfully() {
    Map<String, String> labels = new HashMap<>();
    labels.put("application", "app");
    labels.put("instance", "host:8080");
    labels.put("uri", "/api/v1/health");

    MappingResult result = MetricPointMapper.toPoint(
        buildResponseWithMetricLabels(labels, "0.99"), MetricType.HTTP_P99_RESPONSE_TIME);

    assertThat(result).isInstanceOf(MappingResult.Success.class);
    MetricPointDto point = ((MappingResult.Success) result).point();
    assertThat(point.application()).isEqualTo("app");
    assertThat(point.instance()).isEqualTo("host:8080");
  }

  @Test
  void toPoint_fractionalTimestamp_preservesNanos() {
    // 1714000000.5 → sec=1714000000, nanos=500_000_000 (0.5 is exactly representable in double)
    PrometheusResponse.Result prometheusResult = new PrometheusResponse.Result();
    prometheusResult.setMetric(Map.of());
    prometheusResult.setValue(List.of(1714000000.5, "0.75"));

    PrometheusResponse.Datas data = new PrometheusResponse.Datas();
    data.setResultType("vector");
    data.setResult(List.of(prometheusResult));

    PrometheusResponse response = new PrometheusResponse();
    response.setStatus("success");
    response.setData(data);

    MappingResult mappingResult = MetricPointMapper.toPoint(response, MetricType.CPU_USAGE);

    assertThat(mappingResult).isInstanceOf(MappingResult.Success.class);
    MetricPointDto point = ((MappingResult.Success) mappingResult).point();
    assertThat(point.timestamp().toInstant().getEpochSecond()).isEqualTo(1714000000L);
    assertThat(point.timestamp().toInstant().getNano()).isEqualTo(500_000_000);
  }

  // --- toPoints ---

  @Test
  void toPoints_multipleResults_returnsAllPointsWithIdentifier() {
    PrometheusResponse response = buildResponseWithResults(List.of(
        endpointResult("/api/v1/a", "0.05"),
        endpointResult("/api/v1/b", "0.10"),
        endpointResult("/api/v1/c", "0.20")));

    MultiMappingResult result = MetricPointMapper.toPoints(
        response, MetricType.HTTP_P99_RESPONSE_TIME, "uri");

    assertThat(result).isInstanceOf(MultiMappingResult.Success.class);
    List<LabeledPoint> points = ((MultiMappingResult.Success) result).points();
    assertThat(points).hasSize(3);
    assertThat(points.get(0).identifier()).isEqualTo("/api/v1/a");
    assertThat(points.get(0).value()).isEqualByComparingTo(new BigDecimal("0.05"));
    assertThat(points.get(0).application()).isEqualTo("app");
    assertThat(points.get(0).instance()).isEqualTo("host:8080");
  }

  @Test
  void toPoints_emptyResultList_returnsEmpty() {
    MultiMappingResult result = MetricPointMapper.toPoints(
        buildResponseWithResults(List.of()), MetricType.HTTP_RPS, "uri");

    assertThat(result).isInstanceOf(MultiMappingResult.Empty.class);
  }

  @Test
  void toPoints_nullData_returnsEmpty() {
    PrometheusResponse response = new PrometheusResponse();
    response.setStatus("success");
    response.setData(null);

    MultiMappingResult result = MetricPointMapper.toPoints(
        response, MetricType.HTTP_RPS, "uri");

    assertThat(result).isInstanceOf(MultiMappingResult.Empty.class);
  }

  @Test
  void toPoints_someResultsNonFinite_skipsAndKeepsValid() {
    PrometheusResponse response = buildResponseWithResults(List.of(
        endpointResult("/api/v1/a", "0.05"),
        endpointResult("/api/v1/b", "NaN"),
        endpointResult("/api/v1/c", "0.20")));

    MultiMappingResult result = MetricPointMapper.toPoints(
        response, MetricType.HTTP_P99_RESPONSE_TIME, "uri");

    assertThat(result).isInstanceOf(MultiMappingResult.Success.class);
    List<LabeledPoint> points = ((MultiMappingResult.Success) result).points();
    assertThat(points).hasSize(2);
    assertThat(points).extracting(LabeledPoint::identifier)
        .containsExactly("/api/v1/a", "/api/v1/c");
  }

  @Test
  void toPoints_allResultsMissingIdentifierLabel_returnsParseFailed() {
    PrometheusResponse response = buildResponseWithResults(List.of(
        labeledResult(Map.of("application", "app"), "0.05"),
        labeledResult(Map.of("application", "app"), "0.10")));

    MultiMappingResult result = MetricPointMapper.toPoints(
        response, MetricType.HTTP_P99_RESPONSE_TIME, "uri");

    assertThat(result).isInstanceOf(MultiMappingResult.ParseFailed.class);
    assertThat(((MultiMappingResult.ParseFailed) result).reason()).contains("uri");
  }

  @Test
  void toPoints_someMissingIdentifierLabel_skipsAndKeepsValid() {
    PrometheusResponse response = buildResponseWithResults(List.of(
        endpointResult("/api/v1/a", "0.05"),
        labeledResult(Map.of("application", "app"), "0.10")));

    MultiMappingResult result = MetricPointMapper.toPoints(
        response, MetricType.HTTP_P99_RESPONSE_TIME, "uri");

    assertThat(result).isInstanceOf(MultiMappingResult.Success.class);
    List<LabeledPoint> points = ((MultiMappingResult.Success) result).points();
    assertThat(points).hasSize(1);
    assertThat(points.get(0).identifier()).isEqualTo("/api/v1/a");
  }

  @Test
  void toPoints_rpsResultWithoutApplicationInstance_pointsHaveNullLabels() {
    PrometheusResponse response = buildResponseWithResults(List.of(
        labeledResult(Map.of("uri", "/api/v1/a"), "12.5")));

    MultiMappingResult result = MetricPointMapper.toPoints(
        response, MetricType.HTTP_RPS, "uri");

    assertThat(result).isInstanceOf(MultiMappingResult.Success.class);
    LabeledPoint p = ((MultiMappingResult.Success) result).points().get(0);
    assertThat(p.application()).isNull();
    assertThat(p.instance()).isNull();
    assertThat(p.identifier()).isEqualTo("/api/v1/a");
  }

  // --- helpers ---

  private PrometheusResponse.Result endpointResult(String uri, String value) {
    Map<String, String> labels = new HashMap<>();
    labels.put("application", "app");
    labels.put("instance", "host:8080");
    labels.put("uri", uri);
    return labeledResult(labels, value);
  }

  private PrometheusResponse.Result labeledResult(Map<String, String> labels, String value) {
    PrometheusResponse.Result result = new PrometheusResponse.Result();
    result.setMetric(new HashMap<>(labels));
    List<Object> v = new ArrayList<>();
    v.add(1714000000L);
    v.add(value);
    result.setValue(v);
    return result;
  }

  private PrometheusResponse buildResponse(String application, String instance, String value) {
    Map<String, String> labels = new HashMap<>();
    if (application != null) labels.put("application", application);
    if (instance != null) labels.put("instance", instance);
    return buildResponseWithMetricLabels(labels, value);
  }

  private PrometheusResponse buildResponseWithMetricLabels(Map<String, String> labels, String value) {
    PrometheusResponse.Result result = new PrometheusResponse.Result();
    result.setMetric(labels);
    result.setValue(List.of(1714000000L, value));
    return buildResponseWithResults(List.of(result));
  }

  private PrometheusResponse buildResponseWithResults(List<PrometheusResponse.Result> results) {
    PrometheusResponse.Datas data = new PrometheusResponse.Datas();
    data.setResultType("vector");
    data.setResult(results);

    PrometheusResponse response = new PrometheusResponse();
    response.setStatus("success");
    response.setData(data);
    return response;
  }
}
