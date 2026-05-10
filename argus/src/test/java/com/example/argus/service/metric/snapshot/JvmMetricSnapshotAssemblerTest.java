package com.example.argus.service.metric.snapshot;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.example.argus.dto.PrometheusResponse;
import com.example.argus.dto.metric.jvm.JvmMetricSnapshotDto;
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
class JvmMetricSnapshotAssemblerTest {

  @Mock private PrometheusQueryService queryService;
  private final Clock clock =
      Clock.fixed(Instant.ofEpochSecond(1714000000L), ZoneId.of("Asia/Seoul"));

  private JvmMetricSnapshotAssembler assembler;

  @BeforeEach
  void setUp() {
    assembler = new JvmMetricSnapshotAssembler(queryService, clock);
  }

  @Test
  void assemble_allSuccess_returnsCompleteSnapshot() {
    stubSuccess(MetricType.CPU_USAGE, "myapp", "host:8080", "0.5");
    stubSuccess(MetricType.HEAP_USAGE, "myapp", "host:8080", "65.0");
    stubSuccess(MetricType.GC_AVG_DURATION, "myapp", "host:8080", "0.01");
    stubSuccess(MetricType.GC_COUNT, "myapp", "host:8080", "5");
    stubSuccess(MetricType.ACTIVE_THREADS, "myapp", "host:8080", "10");

    JvmMetricSnapshotDto snapshot = assembler.assemble();

    assertThat(snapshot.status()).isEqualTo(SnapshotStatus.COMPLETE);
    assertThat(snapshot.cpu().status()).isEqualTo(MetricStatus.SUCCESS);
    assertThat(snapshot.memory().status()).isEqualTo(MetricStatus.SUCCESS);
    assertThat(snapshot.gc().status()).isEqualTo(MetricStatus.SUCCESS);
    assertThat(snapshot.thread().status()).isEqualTo(MetricStatus.SUCCESS);
    assertThat(snapshot.application()).isEqualTo("myapp");
    assertThat(snapshot.instance()).isEqualTo("host:8080");
  }

  @Test
  void assemble_cpuQueryFails_returnsPartialWithCpuQueryFailed() {
    when(queryService.queryByMetric(MetricType.CPU_USAGE))
        .thenThrow(new PrometheusQueryException("network error"));
    stubSuccess(MetricType.HEAP_USAGE, "myapp", "host:8080", "65.0");
    stubSuccess(MetricType.GC_AVG_DURATION, "myapp", "host:8080", "0.01");
    stubSuccess(MetricType.GC_COUNT, "myapp", "host:8080", "5");
    stubSuccess(MetricType.ACTIVE_THREADS, "myapp", "host:8080", "10");

    JvmMetricSnapshotDto snapshot = assembler.assemble();

    assertThat(snapshot.status()).isEqualTo(SnapshotStatus.PARTIAL);
    assertThat(snapshot.cpu().status()).isEqualTo(MetricStatus.QUERY_FAILED);
    assertThat(snapshot.cpu().missingReason()).contains("network error");
    assertThat(snapshot.application()).isEqualTo("myapp");
  }

  @Test
  void assemble_allFail_returnsFailedSnapshot() {
    when(queryService.queryByMetric(MetricType.CPU_USAGE))
        .thenThrow(new PrometheusQueryException("network error"));
    when(queryService.queryByMetric(MetricType.HEAP_USAGE))
        .thenThrow(new PrometheusQueryException("network error"));
    when(queryService.queryByMetric(MetricType.GC_AVG_DURATION))
        .thenThrow(new PrometheusQueryException("network error"));
    when(queryService.queryByMetric(MetricType.GC_COUNT))
        .thenThrow(new PrometheusQueryException("network error"));
    when(queryService.queryByMetric(MetricType.ACTIVE_THREADS))
        .thenThrow(new PrometheusQueryException("network error"));

    JvmMetricSnapshotDto snapshot = assembler.assemble();

    assertThat(snapshot.status()).isEqualTo(SnapshotStatus.FAILED);
    assertThat(snapshot.cpu().status()).isEqualTo(MetricStatus.QUERY_FAILED);
    assertThat(snapshot.memory().status()).isEqualTo(MetricStatus.QUERY_FAILED);
    assertThat(snapshot.gc().status()).isEqualTo(MetricStatus.QUERY_FAILED);
    assertThat(snapshot.thread().status()).isEqualTo(MetricStatus.QUERY_FAILED);
    assertThat(snapshot.application()).isNull();
  }

  @Test
  void assemble_gcDurationSuccessCountFails_returnsGcPartialAndSnapshotPartial() {
    stubSuccess(MetricType.CPU_USAGE, "myapp", "host:8080", "0.5");
    stubSuccess(MetricType.HEAP_USAGE, "myapp", "host:8080", "65.0");
    stubSuccess(MetricType.GC_AVG_DURATION, "myapp", "host:8080", "0.01");
    when(queryService.queryByMetric(MetricType.GC_COUNT))
        .thenThrow(new PrometheusQueryException("gc count failed"));
    stubSuccess(MetricType.ACTIVE_THREADS, "myapp", "host:8080", "10");

    JvmMetricSnapshotDto snapshot = assembler.assemble();

    assertThat(snapshot.gc().status()).isEqualTo(MetricStatus.PARTIAL);
    assertThat(snapshot.gc().avgDurationSeconds()).isNotNull();
    assertThat(snapshot.gc().count()).isNull();
    assertThat(snapshot.gc().missingReason()).contains("gc count failed");
    assertThat(snapshot.status()).isEqualTo(SnapshotStatus.PARTIAL);
  }

  @Test
  void assemble_memoryEmptyResult_memoryStatusIsEmptyResult() {
    stubSuccess(MetricType.CPU_USAGE, "myapp", "host:8080", "0.5");
    stubEmptyResult(MetricType.HEAP_USAGE);
    stubSuccess(MetricType.GC_AVG_DURATION, "myapp", "host:8080", "0.01");
    stubSuccess(MetricType.GC_COUNT, "myapp", "host:8080", "5");
    stubSuccess(MetricType.ACTIVE_THREADS, "myapp", "host:8080", "10");

    JvmMetricSnapshotDto snapshot = assembler.assemble();

    assertThat(snapshot.memory().status()).isEqualTo(MetricStatus.EMPTY_RESULT);
    assertThat(snapshot.memory().heapUsagePercent()).isNull();
    assertThat(snapshot.status()).isEqualTo(SnapshotStatus.PARTIAL);
  }

  @Test
  void assemble_cpuAndMemoryFail_gcSucceedsWithLabels_applicationFromGc() {
    when(queryService.queryByMetric(MetricType.CPU_USAGE))
        .thenThrow(new PrometheusQueryException("cpu failed"));
    when(queryService.queryByMetric(MetricType.HEAP_USAGE))
        .thenThrow(new PrometheusQueryException("memory failed"));
    stubSuccess(MetricType.GC_AVG_DURATION, "gcapp", "gchost:8080", "0.01");
    stubSuccess(MetricType.GC_COUNT, "gcapp", "gchost:8080", "5");
    stubSuccess(MetricType.ACTIVE_THREADS, "gcapp", "gchost:8080", "10");

    JvmMetricSnapshotDto snapshot = assembler.assemble();

    assertThat(snapshot.application()).isEqualTo("gcapp");
    assertThat(snapshot.instance()).isEqualTo("gchost:8080");
    assertThat(snapshot.status()).isEqualTo(SnapshotStatus.PARTIAL);
  }

  // CPU 실패 + Memory 성공(라벨 없음) + GC 성공(라벨 있음) → GC 라벨로 교체되어야 한다
  @Test
  void assemble_cpuFail_memorySuccessNoLabels_gcSucceedsWithLabels_applicationFromGc() {
    when(queryService.queryByMetric(MetricType.CPU_USAGE))
        .thenThrow(new PrometheusQueryException("cpu failed"));
    stubSuccessNoLabels(MetricType.HEAP_USAGE, "65.0"); // application=null, instance=null
    stubSuccess(MetricType.GC_AVG_DURATION, "myapp", "host:8080", "0.01");
    stubSuccess(MetricType.GC_COUNT, "myapp", "host:8080", "5");
    stubSuccess(MetricType.ACTIVE_THREADS, "myapp", "host:8080", "10");

    JvmMetricSnapshotDto snapshot = assembler.assemble();

    assertThat(snapshot.memory().status()).isEqualTo(MetricStatus.SUCCESS);
    assertThat(snapshot.application()).isEqualTo("myapp");
    assertThat(snapshot.instance()).isEqualTo("host:8080");
  }

  @Test
  void assemble_gcBothQueriesFail_gcStatusIsQueryFailed() {
    stubSuccess(MetricType.CPU_USAGE, "myapp", "host:8080", "0.5");
    stubSuccess(MetricType.HEAP_USAGE, "myapp", "host:8080", "65.0");
    when(queryService.queryByMetric(MetricType.GC_AVG_DURATION))
        .thenThrow(new PrometheusQueryException("duration failed"));
    when(queryService.queryByMetric(MetricType.GC_COUNT))
        .thenThrow(new PrometheusQueryException("count failed"));
    stubSuccess(MetricType.ACTIVE_THREADS, "myapp", "host:8080", "10");

    JvmMetricSnapshotDto snapshot = assembler.assemble();

    assertThat(snapshot.gc().status()).isEqualTo(MetricStatus.QUERY_FAILED);
    assertThat(snapshot.gc().missingReason()).contains("duration failed");
    assertThat(snapshot.gc().missingReason()).contains("count failed");
    assertThat(snapshot.status()).isEqualTo(SnapshotStatus.PARTIAL);
  }

  @Test
  void assemble_gcBothEmpty_gcStatusIsEmptyResult() {
    stubSuccess(MetricType.CPU_USAGE, "myapp", "host:8080", "0.5");
    stubSuccess(MetricType.HEAP_USAGE, "myapp", "host:8080", "65.0");
    stubEmptyResult(MetricType.GC_AVG_DURATION);
    stubEmptyResult(MetricType.GC_COUNT);
    stubSuccess(MetricType.ACTIVE_THREADS, "myapp", "host:8080", "10");

    JvmMetricSnapshotDto snapshot = assembler.assemble();

    assertThat(snapshot.gc().status()).isEqualTo(MetricStatus.EMPTY_RESULT);
    assertThat(snapshot.gc().avgDurationSeconds()).isNull();
    assertThat(snapshot.gc().count()).isNull();
    assertThat(snapshot.status()).isEqualTo(SnapshotStatus.PARTIAL);
  }

  @Test
  void assemble_threadFractionalValue_threadStatusIsParseFailed() {
    stubSuccess(MetricType.CPU_USAGE, "myapp", "host:8080", "0.5");
    stubSuccess(MetricType.HEAP_USAGE, "myapp", "host:8080", "65.0");
    stubSuccess(MetricType.GC_AVG_DURATION, "myapp", "host:8080", "0.01");
    stubSuccess(MetricType.GC_COUNT, "myapp", "host:8080", "5");
    stubSuccess(MetricType.ACTIVE_THREADS, "myapp", "host:8080", "10.5");

    JvmMetricSnapshotDto snapshot = assembler.assemble();

    assertThat(snapshot.thread().status()).isEqualTo(MetricStatus.PARSE_FAILED);
    assertThat(snapshot.thread().missingReason()).contains("cannot convert to int");
    assertThat(snapshot.status()).isEqualTo(SnapshotStatus.PARTIAL);
  }

  // --- helpers ---

  private void stubSuccess(MetricType type, String application, String instance, String value) {
    Map<String, String> labels = new HashMap<>();
    labels.put("application", application);
    labels.put("instance", instance);

    PrometheusResponse.Result result = new PrometheusResponse.Result();
    result.setMetric(labels);
    result.setValue(List.of(1714000000L, value));

    PrometheusResponse.Datas data = new PrometheusResponse.Datas();
    data.setResultType("vector");
    data.setResult(List.of(result));

    PrometheusResponse response = new PrometheusResponse();
    response.setStatus("success");
    response.setData(data);

    when(queryService.queryByMetric(type)).thenReturn(response);
    if (type == MetricType.HEAP_USAGE) {
      when(queryService.queryByMetric(MetricType.HEAP_OLD_GEN_USAGE)).thenReturn(response);
    }
    if (type == MetricType.ACTIVE_THREADS) {
      when(queryService.queryByMetric(MetricType.PEAK_THREADS)).thenReturn(response);
      when(queryService.queryByMetric(MetricType.DAEMON_THREADS)).thenReturn(response);
    }
  }

  /** application/instance 라벨이 없는 응답 (HEAP_USAGE 등 by 절 없는 메트릭 시뮬레이션). */
  private void stubSuccessNoLabels(MetricType type, String value) {
    PrometheusResponse.Result result = new PrometheusResponse.Result();
    result.setMetric(Map.of());
    result.setValue(List.of(1714000000L, value));

    PrometheusResponse.Datas data = new PrometheusResponse.Datas();
    data.setResultType("vector");
    data.setResult(List.of(result));

    PrometheusResponse response = new PrometheusResponse();
    response.setStatus("success");
    response.setData(data);

    when(queryService.queryByMetric(type)).thenReturn(response);
    if (type == MetricType.HEAP_USAGE) {
      when(queryService.queryByMetric(MetricType.HEAP_OLD_GEN_USAGE)).thenReturn(response);
    }
  }

  private void stubEmptyResult(MetricType type) {
    PrometheusResponse.Datas data = new PrometheusResponse.Datas();
    data.setResultType("vector");
    data.setResult(List.of());

    PrometheusResponse response = new PrometheusResponse();
    response.setStatus("success");
    response.setData(data);

    when(queryService.queryByMetric(type)).thenReturn(response);
    if (type == MetricType.HEAP_USAGE) {
      when(queryService.queryByMetric(MetricType.HEAP_OLD_GEN_USAGE)).thenReturn(response);
    }
    if (type == MetricType.ACTIVE_THREADS) {
      when(queryService.queryByMetric(MetricType.PEAK_THREADS)).thenReturn(response);
      when(queryService.queryByMetric(MetricType.DAEMON_THREADS)).thenReturn(response);
    }
  }
}
