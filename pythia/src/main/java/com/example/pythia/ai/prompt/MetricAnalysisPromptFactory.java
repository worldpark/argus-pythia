package com.example.pythia.ai.prompt;

import com.example.pythia.ai.dto.AnalysisTarget;
import com.example.pythia.ai.dto.MetricAnalysisRequest;
import com.example.pythia.ai.dto.MetricSummary;
import com.example.pythia.ai.dto.TimeSeriesPoint;
import com.example.pythia.ai.exception.AiAnalysisException;
import com.example.pythia.ai.exception.AiErrorCode;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.springframework.ai.chat.prompt.Prompt;
import org.springframework.ai.chat.prompt.PromptTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;

/**
 * {@link MetricAnalysisRequest}를 Spring AI {@link Prompt}로 변환하는 팩토리.
 *
 * <p>입력 검증 → 요약/시계열 텍스트 렌더링 → 외부 템플릿(.st) 변수 치환 순서로 동작한다. 명세 4-블록 구조(분석 대상 / 메트릭
 * 요약 / 시계열 데이터 / 분석 요청)는 외부 템플릿이 보장한다.
 *
 * <p>외부 입력(application, instance, metricName, unit)은 {@link #sanitizeForPrompt(String)}로
 * 프롬프트 인젝션 방어 처리된다. 시스템 생성 값(숫자, 타임스탬프, range)은 미적용.
 */
@Component
public class MetricAnalysisPromptFactory {

  private static final String TIME_SERIES_HEADER = "timestamp\tmetric\tvalue";
  private static final int MAX_SANITIZE_LEN = 256;

  // 제어문자 (\r, \n, \t 제외) 제거
  private static final Pattern CONTROL_CHARS = Pattern.compile("[\\p{Cntrl}&&[^\\t]]");
  // Markdown 코드 펜스
  private static final Pattern MARKDOWN_FENCE = Pattern.compile("(?m)^\\s{0,3}(```|~~~).*$");
  // Markdown 헤딩/수평선
  private static final Pattern MARKDOWN_HEADING_OR_RULE =
      Pattern.compile("(?m)^\\s{0,3}(#{1,6}\\s|-{3,}|\\*{3,}|_{3,})");
  // 프롬프트 인젝션 키워드
  private static final Pattern INJECTION_KEYWORDS =
      Pattern.compile("(?i)(ignore\\s+previous|disregard\\s+previous|system\\s*:|assistant\\s*:|user\\s*:)");

  private final PromptTemplate promptTemplate;

  public MetricAnalysisPromptFactory(
      @Value("classpath:prompts/metric-analysis.st") Resource templateResource) {
    this.promptTemplate = new PromptTemplate(loadTemplate(templateResource));
  }

  public Prompt build(MetricAnalysisRequest request) {
    validate(request);

    AnalysisTarget target = request.target();
    Map<String, Object> variables = new HashMap<>();
    variables.put("application", sanitizeForPrompt(target.application()));
    variables.put("instance", sanitizeForPrompt(target.instance()));
    variables.put("range", renderRange(target.range()));
    variables.put("metricSummary", renderSummaryBlock(request.summaries()));
    variables.put("timeSeriesTable", renderTimeSeriesTable(request.timeSeries()));

    return promptTemplate.create(variables);
  }

  /**
   * 외부 입력 문자열에서 프롬프트 인젝션 위협을 제거한다.
   *
   * <ul>
   *   <li>제어문자(\r, \n 포함, \t 제외) → 공백
   *   <li>Markdown 코드 펜스/헤딩/수평선 → 공백
   *   <li>인젝션 키워드(ignore previous 등) → [redacted]
   *   <li>백틱 → 공백
   *   <li>256자 초과 → 절단
   * </ul>
   */
  private static String sanitizeForPrompt(String input) {
    if (input == null) {
      return "";
    }
    String value = input;
    // Markdown 구조 먼저 처리 (줄 단위 패턴이 \n 기반이므로 제어문자 변환 전에 수행)
    value = MARKDOWN_FENCE.matcher(value).replaceAll(" ");
    value = MARKDOWN_HEADING_OR_RULE.matcher(value).replaceAll(" ");
    // 인젝션 키워드
    value = INJECTION_KEYWORDS.matcher(value).replaceAll("[redacted]");
    // 제어문자 처리 (\t 유지, \n 포함 나머지 제거 -> 공백으로)
    value = CONTROL_CHARS.matcher(value).replaceAll(" ");
    // 백틱
    value = value.replace("`", " ");
    value = value.replaceAll("\\s+", " ").trim();
    if (value.length() > MAX_SANITIZE_LEN) {
      value = value.substring(0, MAX_SANITIZE_LEN);
    }
    return value;
  }

  private void validate(MetricAnalysisRequest request) {
    if (request == null) {
      throw new AiAnalysisException(
          AiErrorCode.INVALID_REQUEST, "MetricAnalysisRequest must not be null");
    }
    AnalysisTarget target = request.target();
    if (target == null) {
      throw new AiAnalysisException(
          AiErrorCode.INVALID_REQUEST, "AnalysisTarget must not be null");
    }
    if (target.application() == null || target.application().isBlank()) {
      throw new AiAnalysisException(
          AiErrorCode.INVALID_REQUEST, "AnalysisTarget.application must not be blank");
    }
    if (target.instance() == null || target.instance().isBlank()) {
      throw new AiAnalysisException(
          AiErrorCode.INVALID_REQUEST, "AnalysisTarget.instance must not be blank");
    }
    if (target.range() == null) {
      throw new AiAnalysisException(
          AiErrorCode.INVALID_REQUEST, "AnalysisTarget.range must not be null");
    }
    if (request.summaries() == null || request.summaries().isEmpty()) {
      throw new AiAnalysisException(
          AiErrorCode.INVALID_REQUEST, "summaries must not be empty");
    }
    if (request.timeSeries() == null || request.timeSeries().isEmpty()) {
      throw new AiAnalysisException(
          AiErrorCode.INVALID_REQUEST, "timeSeries must not be empty");
    }
  }

  private String renderSummaryBlock(List<MetricSummary> summaries) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < summaries.size(); i++) {
      MetricSummary s = summaries.get(i);
      sb.append("- ")
          .append(sanitizeForPrompt(s.metricName()))
          .append(" (")
          .append(s.aggregation().name().toLowerCase())
          .append("): ")
          .append(s.value() != null ? s.value().toPlainString() : "null");
      if (s.unit() != null && !s.unit().isBlank()) {
        sb.append(' ').append(sanitizeForPrompt(s.unit()));
      }
      if (i < summaries.size() - 1) {
        sb.append('\n');
      }
    }
    return sb.toString();
  }

  private String renderTimeSeriesTable(List<TimeSeriesPoint> points) {
    List<TimeSeriesPoint> sorted = points.stream()
        .sorted(Comparator.comparing(TimeSeriesPoint::timestamp))
        .toList();
    StringBuilder sb = new StringBuilder(TIME_SERIES_HEADER);
    for (TimeSeriesPoint p : sorted) {
      sb.append('\n')
          .append(p.timestamp())
          .append('\t')
          .append(sanitizeForPrompt(p.metricName()))
          .append('\t')
          .append(p.value() != null ? p.value().toPlainString() : "null");
    }
    return sb.toString();
  }

  private String renderRange(Duration range) {
    long minutes = range.toMinutes();
    if (minutes > 0 && range.toSecondsPart() == 0 && range.toMillisPart() == 0) {
      return "최근 " + minutes + "분";
    }
    long seconds = range.toSeconds();
    if (seconds > 0) {
      return "최근 " + seconds + "초";
    }
    return range.toString();
  }

  private String loadTemplate(Resource templateResource) {
    try {
      return templateResource.getContentAsString(StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new AiAnalysisException(
          AiErrorCode.INVALID_REQUEST, "failed to load prompt template", e);
    }
  }
}
