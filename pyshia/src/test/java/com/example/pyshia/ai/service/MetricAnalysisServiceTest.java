package com.example.pyshia.ai.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.example.pyshia.ai.dto.AnalysisTarget;
import com.example.pyshia.ai.dto.MetricAnalysisRequest;
import com.example.pyshia.ai.dto.MetricSummary;
import com.example.pyshia.ai.dto.SummaryAggregation;
import com.example.pyshia.ai.dto.TimeSeriesPoint;
import com.example.pyshia.ai.exception.AiAnalysisException;
import com.example.pyshia.ai.exception.AiErrorCode;
import com.example.pyshia.ai.prompt.MetricAnalysisPromptFactory;
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
    service = new MetricAnalysisService(chatClient, promptFactory);
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
  void 정상_응답시_content_문자열을_반환한다() {
    Prompt prompt = mock(Prompt.class);
    MetricAnalysisRequest request = sampleRequest();
    when(promptFactory.build(request)).thenReturn(prompt);
    when(chatClient.prompt(prompt).call().content()).thenReturn("분석 결과 본문");

    String result = service.analyze(request);

    assertThat(result).isEqualTo("분석 결과 본문");
  }

  @Test
  @DisplayName("응답이 null이면 EMPTY_RESPONSE 예외")
  void 응답이_null이면_EMPTY_RESPONSE() {
    Prompt prompt = mock(Prompt.class);
    MetricAnalysisRequest request = sampleRequest();
    when(promptFactory.build(request)).thenReturn(prompt);
    when(chatClient.prompt(prompt).call().content()).thenReturn(null);

    assertThatThrownBy(() -> service.analyze(request))
        .isInstanceOf(AiAnalysisException.class)
        .matches(e -> ((AiAnalysisException) e).getErrorCode() == AiErrorCode.EMPTY_RESPONSE);
  }

  @Test
  @DisplayName("응답이 blank이면 EMPTY_RESPONSE 예외")
  void 응답이_blank이면_EMPTY_RESPONSE() {
    Prompt prompt = mock(Prompt.class);
    MetricAnalysisRequest request = sampleRequest();
    when(promptFactory.build(request)).thenReturn(prompt);
    when(chatClient.prompt(prompt).call().content()).thenReturn("   ");

    assertThatThrownBy(() -> service.analyze(request))
        .isInstanceOf(AiAnalysisException.class)
        .matches(e -> ((AiAnalysisException) e).getErrorCode() == AiErrorCode.EMPTY_RESPONSE);
  }

  @Test
  @DisplayName("ChatClient 호출 중 RuntimeException 발생 시 LLM_CALL_FAILURE로 변환되고 cause 보존")
  void ChatClient_RuntimeException은_LLM_CALL_FAILURE로_변환된다() {
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
  @DisplayName("PromptFactory에서 던진 AiAnalysisException은 그대로 전파된다")
  void PromptFactory_예외는_그대로_전파된다() {
    MetricAnalysisRequest request = sampleRequest();
    AiAnalysisException original =
        new AiAnalysisException(AiErrorCode.INVALID_REQUEST, "summaries must not be empty");
    when(promptFactory.build(any(MetricAnalysisRequest.class))).thenThrow(original);

    assertThatThrownBy(() -> service.analyze(request))
        .isSameAs(original);
  }
}
