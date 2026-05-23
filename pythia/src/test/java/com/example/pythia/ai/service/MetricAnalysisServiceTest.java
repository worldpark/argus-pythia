package com.example.pythia.ai.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.example.pythia.ai.config.AiAnalysisProperties;
import com.example.pythia.ai.dto.AnalysisTarget;
import com.example.pythia.ai.dto.MetricAnalysisRequest;
import com.example.pythia.ai.dto.MetricSummary;
import com.example.pythia.ai.dto.SummaryAggregation;
import com.example.pythia.ai.dto.TimeSeriesPoint;
import com.example.pythia.ai.exception.AiAnalysisException;
import com.example.pythia.ai.exception.AiErrorCode;
import com.example.pythia.ai.prompt.MetricAnalysisPromptFactory;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.prompt.Prompt;

class MetricAnalysisServiceTest {

  private ChatClient chatClient;
  private MetricAnalysisPromptFactory promptFactory;
  private MetricAnalysisService service;

  @BeforeEach
  void setUp() {
    chatClient = mock(ChatClient.class, RETURNS_DEEP_STUBS);
    promptFactory = mock(MetricAnalysisPromptFactory.class);
    AiAnalysisProperties props = new AiAnalysisProperties();
    props.setMaxResponseChars(5000);

    RetryRegistry retryRegistry = RetryRegistry.of(RetryConfig.custom()
        .maxAttempts(1)
        .build());
    retryRegistry.retry("llmAnalysis");
    CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.ofDefaults();
    circuitBreakerRegistry.circuitBreaker("llmAnalysis");

    service = new MetricAnalysisService(
        chatClient,
        promptFactory,
        props,
        retryRegistry,
        circuitBreakerRegistry);
  }

  private MetricAnalysisRequest sampleRequest() {
    AnalysisTarget target = new AnalysisTarget("argus", "localhost:8080", Duration.ofMinutes(15));
    return new MetricAnalysisRequest(
        target,
        List.of(new MetricSummary("process_cpu_usage", SummaryAggregation.AVG, new BigDecimal("0.7"), null)),
        List.of(new TimeSeriesPoint(OffsetDateTime.now(), "process_cpu_usage", new BigDecimal("0.7")))
    );
  }

  @Test
  @DisplayName("정상 응답 시 content 문자열을 반환한다")
  void returnsContentOnSuccess() {
    Prompt prompt = mock(Prompt.class);
    MetricAnalysisRequest request = sampleRequest();
    when(promptFactory.build(request)).thenReturn(prompt);
    when(chatClient.prompt(prompt).call().content()).thenReturn("분석 결과 본문");

    String result = service.analyze(request);

    assertThat(result).isEqualTo("분석 결과 본문");
  }

  @Test
  @DisplayName("응답이 null 이면 EMPTY_RESPONSE 예외")
  void throwsEmptyResponseWhenNull() {
    Prompt prompt = mock(Prompt.class);
    MetricAnalysisRequest request = sampleRequest();
    when(promptFactory.build(request)).thenReturn(prompt);
    when(chatClient.prompt(prompt).call().content()).thenReturn(null);

    assertThatThrownBy(() -> service.analyze(request))
        .isInstanceOf(AiAnalysisException.class)
        .matches(e -> ((AiAnalysisException) e).getErrorCode() == AiErrorCode.EMPTY_RESPONSE);
  }

  @Test
  @DisplayName("응답이 blank 이면 EMPTY_RESPONSE 예외")
  void throwsEmptyResponseWhenBlank() {
    Prompt prompt = mock(Prompt.class);
    MetricAnalysisRequest request = sampleRequest();
    when(promptFactory.build(request)).thenReturn(prompt);
    when(chatClient.prompt(prompt).call().content()).thenReturn("   ");

    assertThatThrownBy(() -> service.analyze(request))
        .isInstanceOf(AiAnalysisException.class)
        .matches(e -> ((AiAnalysisException) e).getErrorCode() == AiErrorCode.EMPTY_RESPONSE);
  }

  @Test
  @DisplayName("ChatClient 런타임 예외는 LLM_CALL_FAILURE 로 변환되고 cause 를 유지한다")
  void wrapsRuntimeException() {
    Prompt prompt = mock(Prompt.class);
    MetricAnalysisRequest request = sampleRequest();
    when(promptFactory.build(request)).thenReturn(prompt);
    RuntimeException cause = new RuntimeException("network timeout");
    when(chatClient.prompt(prompt).call().content()).thenThrow(cause);

    assertThatThrownBy(() -> service.analyze(request))
        .isInstanceOf(AiAnalysisException.class)
        .matches(e -> ((AiAnalysisException) e).getErrorCode() == AiErrorCode.LLM_CALL_FAILURE)
        .hasCause(cause);
  }

  @Test
  @DisplayName("PromptFactory 예외는 그대로 전파한다")
  void propagatesPromptFactoryException() {
    MetricAnalysisRequest request = sampleRequest();
    AiAnalysisException original =
        new AiAnalysisException(AiErrorCode.INVALID_REQUEST, "summaries must not be empty");
    when(promptFactory.build(any(MetricAnalysisRequest.class))).thenThrow(original);

    assertThatThrownBy(() -> service.analyze(request))
        .isSameAs(original);
  }
}
