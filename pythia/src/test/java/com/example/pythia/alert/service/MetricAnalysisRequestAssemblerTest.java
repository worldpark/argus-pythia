package com.example.pythia.alert.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import com.example.pythia.ai.dto.MetricAnalysisRequest;
import com.example.pythia.ai.dto.SummaryAggregation;
import com.example.pythia.ai.exception.AiAnalysisException;
import com.example.pythia.ai.exception.AiErrorCode;
import com.example.pythia.ai.prompt.MetricAnalysisPromptFactory;
import com.example.pythia.alert.domain.MetricKind;
import com.example.pythia.alert.domain.ViolationKey;
import com.example.pythia.metric.domain.HikariMetricKind;
import com.example.pythia.metric.domain.HikariMetricSnapshotEntity;
import com.example.pythia.metric.domain.HikariPoolMetricPointEntity;
import com.example.pythia.metric.domain.HttpEndpointMetricPointEntity;
import com.example.pythia.metric.domain.HttpMetricKind;
import com.example.pythia.metric.domain.HttpMetricSnapshotEntity;
import com.example.pythia.metric.domain.JvmMetricSnapshotEntity;
import com.example.pythia.metric.repository.HikariMetricSnapshotRepository;
import com.example.pythia.metric.repository.HttpMetricSnapshotRepository;
import com.example.pythia.metric.repository.JvmMetricSnapshotRepository;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.core.io.ClassPathResource;

@ExtendWith(MockitoExtension.class)
class MetricAnalysisRequestAssemblerTest {

  @Mock
  private JvmMetricSnapshotRepository jvmRepository;

  @Mock
  private HttpMetricSnapshotRepository httpRepository;

  @Mock
  private HikariMetricSnapshotRepository hikariRepository;

  private MetricAnalysisRequestAssembler assembler;
  private ViolationKey jvmKey;
  private ViolationKey httpKey;
  private ViolationKey hikariKey;

  @BeforeEach
  void setUp() {
    assembler = new MetricAnalysisRequestAssembler(
        jvmRepository, httpRepository, hikariRepository, Duration.ofMinutes(10));
    jvmKey = new ViolationKey(MetricKind.JVM_CPU, "argus", "localhost:8080", null);
    httpKey = new ViolationKey(MetricKind.HTTP_P99, "argus", "localhost:8080", "/api/orders");
    hikariKey = new ViolationKey(MetricKind.HIKARI_USAGE_RATIO, "argus", "localhost:8080", "main-pool");
  }

  private JvmMetricSnapshotEntity jvmRow(OffsetDateTime at, BigDecimal cpu, BigDecimal heap,
      BigDecimal oldGen, BigDecimal gcAvg, BigDecimal gcCount,
      Integer threadActive, Integer threadPeak, Integer threadDaemon) {
    return JvmMetricSnapshotEntity.builder()
        .application("argus")
        .instance("localhost:8080")
        .collectedAt(at)
        .cpuUsagePercent(cpu)
        .heapUsagePercent(heap)
        .oldGenUsagePercent(oldGen)
        .gcAvgDurationSeconds(gcAvg)
        .gcCount(gcCount)
        .threadActiveCount(threadActive)
        .threadPeakCount(threadPeak)
        .threadDaemonCount(threadDaemon)
        .build();
  }

  @Test
  @DisplayName("JVM_CPU: 5건의 스냅샷에서 cpuUsagePercent를 추출하여 AVG/MAX/시계열을 만든다")
  void assemble_JVM_CPU_정상() {
    OffsetDateTime base = OffsetDateTime.now();
    List<JvmMetricSnapshotEntity> rows = List.of(
        jvmRow(base.minusMinutes(4), new BigDecimal("10"), null, null, null, null, null, null, null),
        jvmRow(base.minusMinutes(3), new BigDecimal("20"), null, null, null, null, null, null, null),
        jvmRow(base.minusMinutes(2), new BigDecimal("30"), null, null, null, null, null, null, null),
        jvmRow(base.minusMinutes(1), new BigDecimal("40"), null, null, null, null, null, null, null),
        jvmRow(base, new BigDecimal("50"), null, null, null, null, null, null, null));
    when(jvmRepository.findByApplicationAndInstanceAndCollectedAtBetweenOrderByCollectedAtAsc(
        anyString(), anyString(), any(), any())).thenReturn(rows);

    MetricAnalysisRequest req = assembler.assemble(MetricKind.JVM_CPU, jvmKey);

    assertThat(req.target().application()).isEqualTo("argus");
    assertThat(req.target().instance()).isEqualTo("localhost:8080");
    assertThat(req.target().range()).isEqualTo(Duration.ofMinutes(10));
    assertThat(req.summaries()).hasSize(2);
    assertThat(req.summaries().get(0).aggregation()).isEqualTo(SummaryAggregation.AVG);
    assertThat(req.summaries().get(0).metricName()).isEqualTo("JVM_CPU");
    assertThat(req.summaries().get(0).unit()).isEqualTo("%");
    assertThat(req.summaries().get(0).value()).isEqualByComparingTo("30");
    assertThat(req.summaries().get(1).aggregation()).isEqualTo(SummaryAggregation.MAX);
    assertThat(req.summaries().get(1).value()).isEqualByComparingTo("50");
    assertThat(req.timeSeries()).hasSize(5);
    assertThat(req.timeSeries().get(0).metricName()).isEqualTo("JVM_CPU");
  }

  @Test
  @DisplayName("JVM_HEAP: heapUsagePercent 컬럼이 추출된다")
  void assemble_JVM_HEAP_컬럼_추출() {
    OffsetDateTime base = OffsetDateTime.now();
    List<JvmMetricSnapshotEntity> rows = List.of(
        jvmRow(base, null, new BigDecimal("70"), null, null, null, null, null, null));
    when(jvmRepository.findByApplicationAndInstanceAndCollectedAtBetweenOrderByCollectedAtAsc(
        anyString(), anyString(), any(), any())).thenReturn(rows);

    MetricAnalysisRequest req = assembler.assemble(MetricKind.JVM_HEAP,
        new ViolationKey(MetricKind.JVM_HEAP, "argus", "localhost:8080", null));

    assertThat(req.timeSeries()).hasSize(1);
    assertThat(req.timeSeries().get(0).value()).isEqualByComparingTo("70");
    assertThat(req.summaries().get(0).metricName()).isEqualTo("JVM_HEAP");
  }

  @Test
  @DisplayName("JVM_THREAD_ACTIVE: Integer 컬럼이 BigDecimal로 변환되어 추출된다")
  void assemble_JVM_THREAD_ACTIVE_Integer_변환() {
    OffsetDateTime base = OffsetDateTime.now();
    List<JvmMetricSnapshotEntity> rows = List.of(
        jvmRow(base.minusMinutes(1), null, null, null, null, null, 10, null, null),
        jvmRow(base, null, null, null, null, null, 30, null, null));
    when(jvmRepository.findByApplicationAndInstanceAndCollectedAtBetweenOrderByCollectedAtAsc(
        anyString(), anyString(), any(), any())).thenReturn(rows);

    MetricAnalysisRequest req = assembler.assemble(MetricKind.JVM_THREAD_ACTIVE,
        new ViolationKey(MetricKind.JVM_THREAD_ACTIVE, "argus", "localhost:8080", null));

    assertThat(req.timeSeries()).hasSize(2);
    assertThat(req.timeSeries().get(0).value()).isEqualByComparingTo("10");
    assertThat(req.timeSeries().get(1).value()).isEqualByComparingTo("30");
    assertThat(req.summaries().get(0).value()).isEqualByComparingTo("20");
    assertThat(req.summaries().get(0).unit()).isEqualTo("개");
  }

  private HttpMetricSnapshotEntity httpSnapshot(OffsetDateTime at) {
    return HttpMetricSnapshotEntity.builder()
        .application("argus")
        .instance("localhost:8080")
        .collectedAt(at)
        .build();
  }

  private HikariMetricSnapshotEntity hikariSnapshot(OffsetDateTime at) {
    return HikariMetricSnapshotEntity.builder()
        .application("argus")
        .instance("localhost:8080")
        .collectedAt(at)
        .build();
  }

  @Test
  @DisplayName("HTTP_P99: 동일 인스턴스의 P99 + 다른 endpoint 혼재 시 sub(endpoint) 일치 항목만 추출된다")
  void assemble_HTTP_P99_endpoint_필터링() {
    OffsetDateTime base = OffsetDateTime.now();
    HttpMetricSnapshotEntity snapshot = httpSnapshot(base);
    snapshot.addPoint(HttpEndpointMetricPointEntity.of(
        HttpMetricKind.P99, "/api/orders", new BigDecimal("1.5"), base));
    snapshot.addPoint(HttpEndpointMetricPointEntity.of(
        HttpMetricKind.P99, "/api/users", new BigDecimal("0.3"), base));
    snapshot.addPoint(HttpEndpointMetricPointEntity.of(
        HttpMetricKind.RPS, "/api/orders", new BigDecimal("100"), base));
    snapshot.addPoint(HttpEndpointMetricPointEntity.of(
        HttpMetricKind.ERROR_RATE, "/api/orders", new BigDecimal("5"), base));

    when(httpRepository.findByApplicationAndInstanceAndCollectedAtBetweenOrderByCollectedAtAsc(
        anyString(), anyString(), any(), any())).thenReturn(List.of(snapshot));

    MetricAnalysisRequest req = assembler.assemble(MetricKind.HTTP_P99, httpKey);

    assertThat(req.timeSeries()).hasSize(1);
    assertThat(req.timeSeries().get(0).value()).isEqualByComparingTo("1.5");
    assertThat(req.summaries().get(0).metricName()).isEqualTo("HTTP_P99");
    assertThat(req.summaries().get(0).unit()).isEqualTo("초");
  }

  @Test
  @DisplayName("HIKARI_USAGE_RATIO: pool 일치 항목만 추출된다")
  void assemble_HIKARI_USAGE_RATIO_pool_필터링() {
    OffsetDateTime base = OffsetDateTime.now();
    HikariMetricSnapshotEntity snapshot = hikariSnapshot(base);
    snapshot.addPoint(HikariPoolMetricPointEntity.of(
        HikariMetricKind.USAGE_RATIO, "main-pool", new BigDecimal("80"), base));
    snapshot.addPoint(HikariPoolMetricPointEntity.of(
        HikariMetricKind.USAGE_RATIO, "other-pool", new BigDecimal("10"), base));
    snapshot.addPoint(HikariPoolMetricPointEntity.of(
        HikariMetricKind.ACTIVE, "main-pool", new BigDecimal("8"), base));

    when(hikariRepository.findByApplicationAndInstanceAndCollectedAtBetweenOrderByCollectedAtAsc(
        anyString(), anyString(), any(), any())).thenReturn(List.of(snapshot));

    MetricAnalysisRequest req = assembler.assemble(MetricKind.HIKARI_USAGE_RATIO, hikariKey);

    assertThat(req.timeSeries()).hasSize(1);
    assertThat(req.timeSeries().get(0).value()).isEqualByComparingTo("80");
    assertThat(req.summaries().get(0).metricName()).isEqualTo("HIKARI_USAGE_RATIO");
    assertThat(req.summaries().get(0).unit()).isEqualTo("%");
  }

  @Test
  @DisplayName("데이터 0건이면 AiAnalysisException(INVALID_REQUEST)이 발생한다")
  void assemble_데이터_0건_예외() {
    when(jvmRepository.findByApplicationAndInstanceAndCollectedAtBetweenOrderByCollectedAtAsc(
        anyString(), anyString(), any(), any())).thenReturn(List.of());

    assertThatThrownBy(() -> assembler.assemble(MetricKind.JVM_CPU, jvmKey))
        .isInstanceOf(AiAnalysisException.class)
        .extracting(e -> ((AiAnalysisException) e).getErrorCode())
        .isEqualTo(AiErrorCode.INVALID_REQUEST);
  }

  @Test
  @DisplayName("일부 row의 해당 컬럼이 null이면 null만 제외하고 나머지로 정상 생성된다")
  void assemble_일부_null_컬럼_제외() {
    OffsetDateTime base = OffsetDateTime.now();
    List<JvmMetricSnapshotEntity> rows = List.of(
        jvmRow(base.minusMinutes(2), new BigDecimal("10"), null, null, null, null, null, null, null),
        jvmRow(base.minusMinutes(1), null, null, null, null, null, null, null, null),
        jvmRow(base, new BigDecimal("30"), null, null, null, null, null, null, null));
    when(jvmRepository.findByApplicationAndInstanceAndCollectedAtBetweenOrderByCollectedAtAsc(
        anyString(), anyString(), any(), any())).thenReturn(rows);

    MetricAnalysisRequest req = assembler.assemble(MetricKind.JVM_CPU, jvmKey);

    assertThat(req.timeSeries()).hasSize(2);
    assertThat(req.summaries().get(0).value()).isEqualByComparingTo("20");
    assertThat(req.summaries().get(1).value()).isEqualByComparingTo("30");
  }

  @Test
  @DisplayName("JVM rows 전체가 해당 컬럼 null이면 데이터 0건과 동일하게 예외")
  void assemble_모든_컬럼_null_예외() {
    OffsetDateTime base = OffsetDateTime.now();
    List<JvmMetricSnapshotEntity> rows = List.of(
        jvmRow(base, null, null, null, null, null, null, null, null));
    when(jvmRepository.findByApplicationAndInstanceAndCollectedAtBetweenOrderByCollectedAtAsc(
        anyString(), anyString(), any(), any())).thenReturn(rows);

    assertThatThrownBy(() -> assembler.assemble(MetricKind.JVM_CPU, jvmKey))
        .isInstanceOf(AiAnalysisException.class);
  }

  @Test
  @DisplayName("HTTP point의 measuredAt이 null이면 부모의 collectedAt이 timestamp로 사용된다")
  void assemble_HTTP_measuredAt_null_fallback() {
    OffsetDateTime base = OffsetDateTime.now();
    HttpMetricSnapshotEntity snapshot = httpSnapshot(base);
    snapshot.addPoint(HttpEndpointMetricPointEntity.of(
        HttpMetricKind.P99, "/api/orders", new BigDecimal("1.5"), null));

    when(httpRepository.findByApplicationAndInstanceAndCollectedAtBetweenOrderByCollectedAtAsc(
        anyString(), anyString(), any(), any())).thenReturn(List.of(snapshot));

    MetricAnalysisRequest req = assembler.assemble(MetricKind.HTTP_P99, httpKey);

    assertThat(req.timeSeries()).hasSize(1);
    assertThat(req.timeSeries().get(0).timestamp()).isEqualTo(base);
  }

  @Test
  @DisplayName("HIKARI_ACTIVE: ACTIVE와 PENDING이 혼재할 때 ACTIVE만 추출되고 다른 pool은 제외된다")
  void assemble_HIKARI_ACTIVE_kind_및_pool_필터링() {
    OffsetDateTime base = OffsetDateTime.now();
    ViolationKey activeKey = new ViolationKey(MetricKind.HIKARI_ACTIVE, "argus", "localhost:8080", "main-pool");
    HikariMetricSnapshotEntity snapshot = hikariSnapshot(base);
    snapshot.addPoint(HikariPoolMetricPointEntity.of(
        HikariMetricKind.ACTIVE, "main-pool", new BigDecimal("5"), base));
    snapshot.addPoint(HikariPoolMetricPointEntity.of(
        HikariMetricKind.PENDING, "main-pool", new BigDecimal("2"), base));
    snapshot.addPoint(HikariPoolMetricPointEntity.of(
        HikariMetricKind.ACTIVE, "other-pool", new BigDecimal("3"), base));

    when(hikariRepository.findByApplicationAndInstanceAndCollectedAtBetweenOrderByCollectedAtAsc(
        anyString(), anyString(), any(), any())).thenReturn(List.of(snapshot));

    MetricAnalysisRequest req = assembler.assemble(MetricKind.HIKARI_ACTIVE, activeKey);

    assertThat(req.timeSeries()).hasSize(1);
    assertThat(req.timeSeries().get(0).value()).isEqualByComparingTo("5");
    assertThat(req.summaries().get(0).metricName()).isEqualTo("HIKARI_ACTIVE");
    assertThat(req.summaries().get(0).unit()).isEqualTo("개");
  }

  @Test
  @DisplayName("HTTP_ERROR_RATE: 동일 endpoint에 P99와 ERROR_RATE가 혼재할 때 ERROR_RATE만 추출된다")
  void assemble_HTTP_ERROR_RATE_kind_필터링() {
    OffsetDateTime base = OffsetDateTime.now();
    ViolationKey errorRateKey = new ViolationKey(MetricKind.HTTP_ERROR_RATE, "argus", "localhost:8080", "/api/orders");
    HttpMetricSnapshotEntity snapshot = httpSnapshot(base);
    snapshot.addPoint(HttpEndpointMetricPointEntity.of(
        HttpMetricKind.P99, "/api/orders", new BigDecimal("1.5"), base));
    snapshot.addPoint(HttpEndpointMetricPointEntity.of(
        HttpMetricKind.ERROR_RATE, "/api/orders", new BigDecimal("3.2"), base));

    when(httpRepository.findByApplicationAndInstanceAndCollectedAtBetweenOrderByCollectedAtAsc(
        anyString(), anyString(), any(), any())).thenReturn(List.of(snapshot));

    MetricAnalysisRequest req = assembler.assemble(MetricKind.HTTP_ERROR_RATE, errorRateKey);

    assertThat(req.timeSeries()).hasSize(1);
    assertThat(req.timeSeries().get(0).value()).isEqualByComparingTo("3.2");
    assertThat(req.summaries().get(0).metricName()).isEqualTo("HTTP_ERROR_RATE");
    assertThat(req.summaries().get(0).unit()).isEqualTo("%");
  }

  @Test
  @DisplayName("assemble 결과가 MetricAnalysisPromptFactory.build를 예외 없이 통과한다 (통합 검증)")
  void assemble_결과가_PromptFactory_validate를_통과한다() {
    OffsetDateTime base = OffsetDateTime.now();
    List<JvmMetricSnapshotEntity> rows = List.of(
        jvmRow(base.minusMinutes(1), new BigDecimal("40"), null, null, null, null, null, null, null),
        jvmRow(base, new BigDecimal("50"), null, null, null, null, null, null, null));
    when(jvmRepository.findByApplicationAndInstanceAndCollectedAtBetweenOrderByCollectedAtAsc(
        anyString(), anyString(), any(), any())).thenReturn(rows);

    MetricAnalysisRequest req = assembler.assemble(MetricKind.JVM_CPU, jvmKey);

    MetricAnalysisPromptFactory promptFactory =
        new MetricAnalysisPromptFactory(new ClassPathResource("prompts/metric-analysis.st"));
    assertThatNoException().isThrownBy(() -> promptFactory.build(req));
  }

  @Test
  @DisplayName("주입된 analysisWindow가 AnalysisTarget.range로 그대로 사용된다")
  void analysisWindow가_AnalysisTarget_range에_반영된다() {
    Duration customWindow = Duration.ofMinutes(30);
    MetricAnalysisRequestAssembler customAssembler = new MetricAnalysisRequestAssembler(
        jvmRepository, httpRepository, hikariRepository, customWindow);

    OffsetDateTime base = OffsetDateTime.now();
    when(jvmRepository.findByApplicationAndInstanceAndCollectedAtBetweenOrderByCollectedAtAsc(
        anyString(), anyString(), any(), any()))
        .thenReturn(List.of(
            jvmRow(base, new BigDecimal("50"), null, null, null, null, null, null, null)));

    MetricAnalysisRequest req = customAssembler.assemble(MetricKind.JVM_CPU, jvmKey);

    assertThat(req.target().range()).isEqualTo(customWindow);
  }

  @Test
  @DisplayName("analysisWindow가 null/zero/negative이면 IllegalArgumentException")
  void analysisWindow_invalid_값이면_생성자에서_예외() {
    assertThatThrownBy(() -> new MetricAnalysisRequestAssembler(
        jvmRepository, httpRepository, hikariRepository, null))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new MetricAnalysisRequestAssembler(
        jvmRepository, httpRepository, hikariRepository, Duration.ZERO))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new MetricAnalysisRequestAssembler(
        jvmRepository, httpRepository, hikariRepository, Duration.ofMinutes(-5)))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
