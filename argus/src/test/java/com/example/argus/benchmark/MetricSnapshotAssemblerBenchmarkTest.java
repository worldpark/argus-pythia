package com.example.argus.benchmark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.example.argus.dto.PrometheusResponse;
import com.example.argus.service.PrometheusQueryService;
import com.example.argus.service.metric.MetricType;
import com.example.argus.service.metric.snapshot.HikariMetricSnapshotAssembler;
import com.example.argus.service.metric.snapshot.HttpMetricSnapshotAssembler;
import com.example.argus.service.metric.snapshot.JvmMetricSnapshotAssembler;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class MetricSnapshotAssemblerBenchmarkTest {

  private static final Logger log =
      LoggerFactory.getLogger(MetricSnapshotAssemblerBenchmarkTest.class);
  private static final Clock CLOCK =
      Clock.fixed(Instant.ofEpochSecond(1714000000L), ZoneId.of("Asia/Seoul"));

  @BeforeAll
  static void requireOptIn() {
    BenchmarkSupport.requireOptIn();
  }

  @Test
  @DisplayName("assembler benchmark: 100ms 지연 stub 에서 fixed fanout vs virtual fanout 비교")
  void benchmark_snapshotCycleWith100msStubDelay() throws Exception {
    log.info("benchmark start: benchmark_snapshotCycleWith100msStubDelay");
    warmUp(20);

    log.info("scenario start: fixed-fanout-8 taskCount=8 outerConcurrency=2 delayMs=50");
    BenchmarkSupport.BenchmarkResult fixed =
        runSnapshotCycleScenario("snapshot-cycle-100ms", "fixed-fanout-8", 20, 4, 8, 100);
    log.info("scenario end: fixed-fanout-8 totalMs={}", fixed.totalMs());

    log.info("scenario start: virtual-fanout taskCount=8 outerConcurrency=2 delayMs=50");
    BenchmarkSupport.BenchmarkResult virtual =
        runSnapshotCycleScenario("snapshot-cycle-100ms", "virtual-fanout", 20, 4, 8, 100);
    log.info("scenario end: virtual-fanout totalMs={}", virtual.totalMs());

    BenchmarkSupport.printResult(fixed);
    BenchmarkSupport.printResult(virtual);
    BenchmarkSupport.printComparison(fixed, virtual);

    assertThat(fixed.failures()).isZero();
    assertThat(virtual.failures()).isZero();
  }

  @Test
  @DisplayName("assembler benchmark: 250ms 지연 stub 에서 fixed fanout vs virtual fanout 비교")
  void benchmark_snapshotCycleWith250msStubDelay() throws Exception {
    log.info("benchmark start: benchmark_snapshotCycleWith250msStubDelay");
    warmUp(50);

    log.info("scenario start: fixed-fanout-8 taskCount=12 outerConcurrency=3 delayMs=250");
    BenchmarkSupport.BenchmarkResult fixed =
        runSnapshotCycleScenario("snapshot-cycle-250ms", "fixed-fanout-8", 12, 3, 8, 250);
    log.info("scenario end: fixed-fanout-8 totalMs={}", fixed.totalMs());

    log.info("scenario start: virtual-fanout taskCount=12 outerConcurrency=3 delayMs=250");
    BenchmarkSupport.BenchmarkResult virtual =
        runSnapshotCycleScenario("snapshot-cycle-250ms", "virtual-fanout", 12, 3, 8, 250);
    log.info("scenario end: virtual-fanout totalMs={}", virtual.totalMs());

    BenchmarkSupport.printResult(fixed);
    BenchmarkSupport.printResult(virtual);
    BenchmarkSupport.printComparison(fixed, virtual);

    assertThat(fixed.failures()).isZero();
    assertThat(virtual.failures()).isZero();
  }

  @Test
  @DisplayName("assembler benchmark quick: fixed fanout 단독 확인")
  void benchmark_snapshotCycleQuick_fixedOnly() throws Exception {
    log.info("benchmark start: benchmark_snapshotCycleQuick_fixedOnly");
    BenchmarkSupport.BenchmarkResult fixed =
        runSnapshotCycleScenario("snapshot-cycle-quick", "fixed-fanout-2", 1, 1, 2, 10);
    BenchmarkSupport.printResult(fixed);
    log.info("benchmark end: benchmark_snapshotCycleQuick_fixedOnly totalMs={}", fixed.totalMs());
    assertThat(fixed.failures()).isZero();
  }

  @Test
  @DisplayName("assembler benchmark quick: virtual fanout 단독 확인")
  void benchmark_snapshotCycleQuick_virtualOnly() throws Exception {
    log.info("benchmark start: benchmark_snapshotCycleQuick_virtualOnly");
    BenchmarkSupport.BenchmarkResult virtual =
        runSnapshotCycleScenario("snapshot-cycle-quick", "virtual-fanout", 1, 1, 2, 10);
    BenchmarkSupport.printResult(virtual);
    log.info(
        "benchmark end: benchmark_snapshotCycleQuick_virtualOnly totalMs={}",
        virtual.totalMs());
    assertThat(virtual.failures()).isZero();
  }

  private void warmUp(long delayMs) throws Exception {
    log.info("warmup start: delayMs={}", delayMs);
    runSnapshotCycleScenario("snapshot-cycle-warmup", "fixed-fanout-4", 4, 2, 4, delayMs);
    log.info("warmup end: delayMs={}", delayMs);
  }

  private BenchmarkSupport.BenchmarkResult runSnapshotCycleScenario(
      String scenario,
      String executionModel,
      int taskCount,
      int outerConcurrency,
      int fixedFanoutSize,
      long delayMs)
      throws Exception {

    log.info(
        "runScenario start: scenario={} executor={} taskCount={} outerConcurrency={} fixedFanoutSize={} delayMs={}",
        scenario,
        executionModel,
        taskCount,
        outerConcurrency,
        fixedFanoutSize,
        delayMs);
    PrometheusQueryService queryService = delayedQueryService(delayMs);

    try (ExecutorService fanoutExecutor =
        executionModel.startsWith("virtual")
            ? Executors.newVirtualThreadPerTaskExecutor()
            : Executors.newFixedThreadPool(fixedFanoutSize)) {

      JvmMetricSnapshotAssembler jvmAssembler =
          new JvmMetricSnapshotAssembler(queryService, CLOCK, fanoutExecutor);
      HttpMetricSnapshotAssembler httpAssembler =
          new HttpMetricSnapshotAssembler(queryService, CLOCK, fanoutExecutor);
      HikariMetricSnapshotAssembler hikariAssembler =
          new HikariMetricSnapshotAssembler(queryService, CLOCK, fanoutExecutor);

      return BenchmarkSupport.runScenario(
          scenario,
          executionModel,
          taskCount,
          outerConcurrency,
          () -> Executors.newFixedThreadPool(outerConcurrency),
          () -> {
            jvmAssembler.assemble();
            httpAssembler.assemble();
            hikariAssembler.assemble();
          });
    } finally {
      log.info("runScenario end: scenario={} executor={}", scenario, executionModel);
    }
  }

  private PrometheusQueryService delayedQueryService(long delayMs) throws Exception {
    PrometheusQueryService queryService = mock(PrometheusQueryService.class);
    when(queryService.queryByMetric(any(MetricType.class)))
        .thenAnswer(
            invocation -> {
              MetricType type = invocation.getArgument(0);
              TimeUnit.MILLISECONDS.sleep(delayMs);
              return responseFor(type);
            });
    return queryService;
  }

  private PrometheusResponse responseFor(MetricType type) {
    return switch (type) {
      case HTTP_P99_RESPONSE_TIME ->
          buildResponse(List.of(series(Map.of("application", "argus", "instance", "localhost:8080", "uri", "/api/v1/a"), "0.12")));
      case HTTP_RPS ->
          buildResponse(List.of(series(Map.of("uri", "/api/v1/a"), "17.5")));
      case HTTP_ERROR_RATE ->
          buildResponse(List.of(series(Map.of("application", "argus", "instance", "localhost:8080", "uri", "/api/v1/a"), "0.01")));
      case HIKARI_ACTIVE_CONNECTIONS, HIKARI_USAGE_RATIO, HIKARI_PENDING_CONNECTIONS ->
          buildResponse(List.of(series(Map.of("application", "argus", "instance", "localhost:8080", "pool", "HikariPool-1"), "3")));
      case CPU_USAGE ->
          buildResponse(List.of(series(baseLabels(), "0.45")));
      case HEAP_USAGE ->
          buildResponse(List.of(series(baseLabels(), "65.0")));
      case HEAP_OLD_GEN_USAGE ->
          buildResponse(List.of(series(baseLabels(), "44.0")));
      case GC_AVG_DURATION ->
          buildResponse(List.of(series(baseLabels(), "0.03")));
      case GC_COUNT ->
          buildResponse(List.of(series(baseLabels(), "8")));
      case ACTIVE_THREADS ->
          buildResponse(List.of(series(baseLabels(), "17")));
      case PEAK_THREADS ->
          buildResponse(List.of(series(baseLabels(), "22")));
      case DAEMON_THREADS ->
          buildResponse(List.of(series(baseLabels(), "11")));
    };
  }

  private Map<String, String> baseLabels() {
    return Map.of("application", "argus", "instance", "localhost:8080");
  }

  private PrometheusResponse.Result series(Map<String, String> labels, String value) {
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
