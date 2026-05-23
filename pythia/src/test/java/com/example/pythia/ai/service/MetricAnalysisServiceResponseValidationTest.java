package com.example.pythia.ai.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
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
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
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
import org.springframework.ai.retry.TransientAiException;

class MetricAnalysisServiceResponseValidationTest {

  private ChatClient chatClient;
  private MetricAnalysisPromptFactory promptFactory;
  private MetricAnalysisService service;

  @BeforeEach
  void setUp() {
    chatClient = mock(ChatClient.class, RETURNS_DEEP_STUBS);
    promptFactory = mock(MetricAnalysisPromptFactory.class);
    AiAnalysisProperties properties = new AiAnalysisProperties();
    properties.setMaxResponseChars(5000);

    RetryRegistry retryRegistry = RetryRegistry.ofDefaults();
    retryRegistry.retry("llmAnalysis", RetryConfig.custom()
        .maxAttempts(3)
        .waitDuration(Duration.ofMillis(5))
        .retryExceptions(TransientAiException.class)
        .ignoreExceptions(AiAnalysisException.class)
        .build());

    CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.ofDefaults();
    circuitBreakerRegistry.circuitBreaker("llmAnalysis");

    service = new MetricAnalysisService(
        chatClient,
        promptFactory,
        properties,
        retryRegistry,
        circuitBreakerRegistry);
    when(promptFactory.build(any())).thenReturn(mock(Prompt.class));
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
  @DisplayName("정상 응답은 그대로 반환된다")
  void returnsValidResponse() {
    when(chatClient.prompt(any(Prompt.class)).call().content()).thenReturn("정상 분석 결과");

    String result = service.analyze(sampleRequest());

    assertThat(result).isEqualTo("정상 분석 결과");
  }

  @Test
  @DisplayName("응답 길이가 5000자를 넘으면 RESPONSE_TOO_LARGE")
  void rejectsTooLargeResponse() {
    when(chatClient.prompt(any(Prompt.class)).call().content()).thenReturn("a".repeat(5001));

    assertThatThrownBy(() -> service.analyze(sampleRequest()))
        .isInstanceOf(AiAnalysisException.class)
        .matches(e -> ((AiAnalysisException) e).getErrorCode() == AiErrorCode.RESPONSE_TOO_LARGE);
  }

  @Test
  @DisplayName("개행과 탭을 제외한 제어문자는 제거된다")
  void stripsControlCharacters() {
    when(chatClient.prompt(any(Prompt.class)).call().content())
        .thenReturn("결과\u0001내용\n\t포함");

    String result = service.analyze(sampleRequest());

    assertThat(result).doesNotContain("\u0001");
    assertThat(result).contains("\n");
    assertThat(result).contains("\t");
  }

  @Test
  @DisplayName("TransientAiException 2회 후 성공하면 총 3회 호출된다")
  void retriesTransientFailures() {
    Prompt prompt = mock(Prompt.class);
    when(promptFactory.build(any())).thenReturn(prompt);
    when(chatClient.prompt(prompt).call().content())
        .thenThrow(new TransientAiException("timeout"))
        .thenThrow(new TransientAiException("timeout"))
        .thenReturn("분석 결과");

    String result = service.analyze(sampleRequest());

    assertThat(result).isEqualTo("분석 결과");
    verify(chatClient.prompt(prompt).call(), times(3)).content();
  }

  @Test
  @DisplayName("AiAnalysisException 은 retry 대상이 아니다")
  void doesNotRetryAiAnalysisException() {
    Prompt prompt = mock(Prompt.class);
    when(promptFactory.build(any())).thenReturn(prompt);
    when(chatClient.prompt(prompt).call().content()).thenReturn(null);

    assertThatThrownBy(() -> service.analyze(sampleRequest()))
        .isInstanceOf(AiAnalysisException.class)
        .matches(e -> ((AiAnalysisException) e).getErrorCode() == AiErrorCode.EMPTY_RESPONSE);

    verify(chatClient.prompt(prompt).call(), times(1)).content();
  }

  @Test
  @DisplayName("CircuitBreaker 가 open 상태면 CallNotPermittedException 이 발생한다")
  void failsWhenCircuitBreakerOpen() {
    AiAnalysisProperties properties = new AiAnalysisProperties();
    properties.setMaxResponseChars(5000);

    RetryRegistry retryRegistry = RetryRegistry.ofDefaults();
    retryRegistry.retry("llmAnalysis", RetryConfig.custom()
        .maxAttempts(1)
        .build());

    CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(
        CircuitBreakerConfig.custom()
            .slidingWindowSize(2)
            .minimumNumberOfCalls(2)
            .failureRateThreshold(100.0f)
            .waitDurationInOpenState(Duration.ofSeconds(30))
            .build());
    circuitBreakerRegistry.circuitBreaker("llmAnalysis").transitionToOpenState();

    MetricAnalysisService circuitOpenService = new MetricAnalysisService(
        chatClient,
        promptFactory,
        properties,
        retryRegistry,
        circuitBreakerRegistry);

    assertThatThrownBy(() -> circuitOpenService.analyze(sampleRequest()))
        .isInstanceOf(CallNotPermittedException.class);
  }
}
