package com.example.pyshia.ai.prompt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.example.pyshia.ai.dto.AnalysisTarget;
import com.example.pyshia.ai.dto.MetricAnalysisRequest;
import com.example.pyshia.ai.dto.MetricSummary;
import com.example.pyshia.ai.dto.SummaryAggregation;
import com.example.pyshia.ai.dto.TimeSeriesPoint;
import com.example.pyshia.ai.exception.AiAnalysisException;
import com.example.pyshia.ai.exception.AiErrorCode;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.ai.chat.prompt.Prompt;
import org.springframework.core.io.ClassPathResource;

class MetricAnalysisPromptFactoryTest {

  private MetricAnalysisPromptFactory factory;

  @BeforeEach
  void setUp() {
    factory = new MetricAnalysisPromptFactory(new ClassPathResource("prompts/metric-analysis.st"));
  }

  private MetricAnalysisRequest sampleRequest() {
    AnalysisTarget target = new AnalysisTarget("argus", "localhost:8080", Duration.ofMinutes(15));
    List<MetricSummary> summaries = List.of(
        new MetricSummary("process_cpu_usage", SummaryAggregation.AVG, new BigDecimal("0.72"), null),
        new MetricSummary("jvm_gc_pause_seconds_sum", SummaryAggregation.MAX, new BigDecimal("0.45"), "s")
    );
    OffsetDateTime t0 = OffsetDateTime.of(2026, 5, 10, 10, 0, 0, 0, ZoneOffset.UTC);
    List<TimeSeriesPoint> points = List.of(
        new TimeSeriesPoint(t0.plusMinutes(2), "process_cpu_usage", new BigDecimal("0.80")),
        new TimeSeriesPoint(t0, "process_cpu_usage", new BigDecimal("0.65")),
        new TimeSeriesPoint(t0.plusMinutes(1), "jvm_gc_pause_seconds_sum", new BigDecimal("0.30"))
    );
    return new MetricAnalysisRequest(target, summaries, points);
  }

  @Test
  @DisplayName("정상 입력 시 4-블록 헤더가 모두 포함된다")
  void 정상입력시_4블록_헤더가_모두_포함된다() {
    Prompt prompt = factory.build(sampleRequest());
    String text = prompt.getContents();

    assertThat(text).contains("# 분석 대상");
    assertThat(text).contains("# 메트릭 요약");
    assertThat(text).contains("# 시계열 데이터");
    assertThat(text).contains("# 분석 요청");
  }

  @Test
  @DisplayName("application/instance/range 변수가 정확히 치환된다")
  void application_instance_range_변수가_정확히_치환된다() {
    Prompt prompt = factory.build(sampleRequest());
    String text = prompt.getContents();

    assertThat(text).contains("application: argus");
    assertThat(text).contains("instance: localhost:8080");
    assertThat(text).contains("range: 최근 15분");
  }

  @Test
  @DisplayName("4-항목 분석 요청 문장이 그대로 포함된다")
  void 분석_요청_4항목이_그대로_포함된다() {
    Prompt prompt = factory.build(sampleRequest());
    String text = prompt.getContents();

    assertThat(text).contains("이상 징후를 판단하라");
    assertThat(text).contains("원인 후보를 우선순위로 제시하라");
    assertThat(text).contains("추가 확인할 지표를 제안하라");
    assertThat(text).contains("조치 방안을 제시하라");
  }

  @Test
  @DisplayName("메트릭 요약이 라인별로 렌더링되고 unit이 있으면 함께 표시된다")
  void 메트릭_요약이_라인별로_렌더링된다() {
    Prompt prompt = factory.build(sampleRequest());
    String text = prompt.getContents();

    assertThat(text).contains("- process_cpu_usage (avg): 0.72");
    assertThat(text).contains("- jvm_gc_pause_seconds_sum (max): 0.45 s");
  }

  @Test
  @DisplayName("시계열 표는 헤더 1행과 timestamp 오름차순으로 정렬된 데이터 행을 포함한다")
  void 시계열_표가_헤더와_정렬된_행을_포함한다() {
    Prompt prompt = factory.build(sampleRequest());
    String text = prompt.getContents();

    assertThat(text).contains("timestamp\tmetric\tvalue");
    int idx0 = text.indexOf("0.65");
    int idx1 = text.indexOf("0.30");
    int idx2 = text.indexOf("0.80");
    assertThat(idx0).isGreaterThan(0);
    assertThat(idx1).isGreaterThan(idx0);
    assertThat(idx2).isGreaterThan(idx1);
  }

  @Test
  @DisplayName("request null이면 INVALID_REQUEST 예외")
  void request_null이면_INVALID_REQUEST() {
    assertThatThrownBy(() -> factory.build(null))
        .isInstanceOf(AiAnalysisException.class)
        .matches(e -> ((AiAnalysisException) e).getErrorCode() == AiErrorCode.INVALID_REQUEST);
  }

  @Test
  @DisplayName("target null이면 INVALID_REQUEST 예외")
  void target_null이면_INVALID_REQUEST() {
    MetricAnalysisRequest req = new MetricAnalysisRequest(null,
        List.of(new MetricSummary("m", SummaryAggregation.AVG, BigDecimal.ONE, null)),
        List.of(new TimeSeriesPoint(OffsetDateTime.now(), "m", BigDecimal.ONE)));

    assertThatThrownBy(() -> factory.build(req))
        .isInstanceOf(AiAnalysisException.class)
        .matches(e -> ((AiAnalysisException) e).getErrorCode() == AiErrorCode.INVALID_REQUEST);
  }

  @Test
  @DisplayName("application blank이면 INVALID_REQUEST 예외")
  void application_blank이면_INVALID_REQUEST() {
    AnalysisTarget badTarget = new AnalysisTarget("  ", "localhost", Duration.ofMinutes(15));
    MetricAnalysisRequest req = new MetricAnalysisRequest(badTarget,
        List.of(new MetricSummary("m", SummaryAggregation.AVG, BigDecimal.ONE, null)),
        List.of(new TimeSeriesPoint(OffsetDateTime.now(), "m", BigDecimal.ONE)));

    assertThatThrownBy(() -> factory.build(req))
        .isInstanceOf(AiAnalysisException.class)
        .matches(e -> ((AiAnalysisException) e).getErrorCode() == AiErrorCode.INVALID_REQUEST);
  }

  @Test
  @DisplayName("summaries empty면 INVALID_REQUEST 예외")
  void summaries_empty면_INVALID_REQUEST() {
    AnalysisTarget target = new AnalysisTarget("argus", "localhost", Duration.ofMinutes(15));
    MetricAnalysisRequest req = new MetricAnalysisRequest(target, List.of(),
        List.of(new TimeSeriesPoint(OffsetDateTime.now(), "m", BigDecimal.ONE)));

    assertThatThrownBy(() -> factory.build(req))
        .isInstanceOf(AiAnalysisException.class)
        .matches(e -> ((AiAnalysisException) e).getErrorCode() == AiErrorCode.INVALID_REQUEST);
  }

  @Test
  @DisplayName("timeSeries empty면 INVALID_REQUEST 예외")
  void timeSeries_empty면_INVALID_REQUEST() {
    AnalysisTarget target = new AnalysisTarget("argus", "localhost", Duration.ofMinutes(15));
    MetricAnalysisRequest req = new MetricAnalysisRequest(target,
        List.of(new MetricSummary("m", SummaryAggregation.AVG, BigDecimal.ONE, null)),
        List.of());

    assertThatThrownBy(() -> factory.build(req))
        .isInstanceOf(AiAnalysisException.class)
        .matches(e -> ((AiAnalysisException) e).getErrorCode() == AiErrorCode.INVALID_REQUEST);
  }
}
