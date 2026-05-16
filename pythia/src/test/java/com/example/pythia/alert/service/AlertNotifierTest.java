package com.example.pythia.alert.service;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.AdditionalMatchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.example.pythia.ai.dto.AnalysisTarget;
import com.example.pythia.ai.dto.MetricAnalysisRequest;
import com.example.pythia.ai.dto.MetricSummary;
import com.example.pythia.ai.dto.SummaryAggregation;
import com.example.pythia.ai.dto.TimeSeriesPoint;
import com.example.pythia.ai.exception.AiAnalysisException;
import com.example.pythia.ai.exception.AiErrorCode;
import com.example.pythia.ai.service.MetricAnalysisService;
import com.example.pythia.alert.domain.MetricKind;
import com.example.pythia.alert.domain.Severity;
import com.example.pythia.alert.domain.ViolationKey;
import com.example.pythia.email.EmailService;
import com.example.pythia.email.exception.EmailErrorCode;
import com.example.pythia.email.exception.EmailSendException;
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

@ExtendWith(MockitoExtension.class)
class AlertNotifierTest {

  @Mock
  private EmailService emailService;

  @Mock
  private MetricAnalysisService analysisService;

  @Mock
  private MetricAnalysisRequestAssembler requestAssembler;

  private AlertNotifier notifier;
  private ViolationKey key;
  private MetricAnalysisRequest sampleRequest;

  @BeforeEach
  void setUp() {
    notifier = new AlertNotifier(
        emailService, new AlertMessageFormatter(), analysisService, requestAssembler);
    key = new ViolationKey(MetricKind.JVM_CPU, "argus", "localhost:8080", null);
    sampleRequest = new MetricAnalysisRequest(
        new AnalysisTarget("argus", "localhost:8080", Duration.ofMinutes(10)),
        List.of(new MetricSummary("JVM_CPU", SummaryAggregation.AVG, BigDecimal.valueOf(80), "%")),
        List.of(new TimeSeriesPoint(OffsetDateTime.now(), "JVM_CPU", BigDecimal.valueOf(80))));
  }

  @Test
  @DisplayName("notify 호출 시 subject와 body를 담아 sendToOperator가 호출된다")
  void notify_호출시_sendToOperator가_호출된다() {
    when(requestAssembler.assemble(eq(MetricKind.JVM_CPU), eq(key))).thenReturn(sampleRequest);
    when(analysisService.analyze(sampleRequest)).thenReturn("분석결과");

    notifier.notify(MetricKind.JVM_CPU, Severity.CRITICAL, key,
        BigDecimal.valueOf(92.0), BigDecimal.valueOf(85.0), 3);

    verify(emailService).sendToOperator(
        contains("[CRITICAL]"),
        contains("JVM CPU 사용률"));
  }

  @Test
  @DisplayName("subject에 severity와 애플리케이션 정보가 포함된다")
  void subject에_severity와_app_정보가_포함된다() {
    when(requestAssembler.assemble(eq(MetricKind.JVM_HEAP), eq(key))).thenReturn(sampleRequest);
    when(analysisService.analyze(sampleRequest)).thenReturn("분석결과");

    notifier.notify(MetricKind.JVM_HEAP, Severity.WARNING, key,
        BigDecimal.valueOf(80.0), BigDecimal.valueOf(75.0), 2);

    verify(emailService).sendToOperator(
        contains("[WARNING]"),
        anyString());
    verify(emailService).sendToOperator(
        contains("argus"),
        anyString());
  }

  @Test
  @DisplayName("EmailSendException 발생 시 예외를 전파하지 않는다")
  void EmailSendException_발생시_예외를_전파하지_않는다() {
    when(requestAssembler.assemble(eq(MetricKind.JVM_CPU), eq(key))).thenReturn(sampleRequest);
    when(analysisService.analyze(sampleRequest)).thenReturn("분석결과");
    doThrow(new EmailSendException(EmailErrorCode.SMTP_FAILURE, "SMTP error"))
        .when(emailService).sendToOperator(anyString(), anyString());

    assertThatNoException().isThrownBy(() ->
        notifier.notify(MetricKind.JVM_CPU, Severity.CRITICAL, key,
            BigDecimal.valueOf(92.0), BigDecimal.valueOf(85.0), 3));
  }

  @Test
  @DisplayName("sub가 null이 아닌 경우 body에 대상 정보가 포함된다")
  void sub가_있으면_body에_대상정보가_포함된다() {
    ViolationKey httpKey = new ViolationKey(MetricKind.HTTP_P99, "argus", "localhost:8080", "/api/orders");
    when(requestAssembler.assemble(eq(MetricKind.HTTP_P99), eq(httpKey))).thenReturn(sampleRequest);
    when(analysisService.analyze(sampleRequest)).thenReturn("분석결과");

    notifier.notify(MetricKind.HTTP_P99, Severity.WARNING, httpKey,
        BigDecimal.valueOf(1.5), BigDecimal.valueOf(1.0), 2);

    verify(emailService).sendToOperator(anyString(), contains("/api/orders"));
  }

  @Test
  @DisplayName("LLM 분석 성공 시 본문에 LLM 분석 섹션이 포함된다")
  void LLM_분석_성공시_본문에_분석_섹션_포함() {
    when(requestAssembler.assemble(eq(MetricKind.JVM_CPU), eq(key))).thenReturn(sampleRequest);
    when(analysisService.analyze(sampleRequest)).thenReturn("LLM이 작성한 분석 내용");

    notifier.notify(MetricKind.JVM_CPU, Severity.CRITICAL, key,
        BigDecimal.valueOf(92.0), BigDecimal.valueOf(85.0), 3);

    verify(emailService).sendToOperator(anyString(), contains("## LLM 분석"));
    verify(emailService).sendToOperator(anyString(), contains("LLM이 작성한 분석 내용"));
  }

  @Test
  @DisplayName("assembler가 AiAnalysisException을 던지면 분석 섹션 없이 이메일이 발송된다")
  void assembler_예외시_분석_없이_이메일_발송() {
    when(requestAssembler.assemble(eq(MetricKind.JVM_CPU), eq(key)))
        .thenThrow(new AiAnalysisException(AiErrorCode.INVALID_REQUEST, "no data"));

    notifier.notify(MetricKind.JVM_CPU, Severity.CRITICAL, key,
        BigDecimal.valueOf(92.0), BigDecimal.valueOf(85.0), 3);

    verify(emailService).sendToOperator(anyString(), contains("JVM CPU 사용률"));
    verify(emailService).sendToOperator(anyString(), not(contains("## LLM 분석")));
    verify(analysisService, never()).analyze(any());
  }

  @Test
  @DisplayName("analysisService가 RuntimeException을 던져도 분석 섹션 없이 이메일이 발송된다")
  void analysisService_예외시_분석_없이_이메일_발송() {
    when(requestAssembler.assemble(eq(MetricKind.JVM_CPU), eq(key))).thenReturn(sampleRequest);
    when(analysisService.analyze(sampleRequest))
        .thenThrow(new AiAnalysisException(AiErrorCode.LLM_CALL_FAILURE, "boom"));

    notifier.notify(MetricKind.JVM_CPU, Severity.CRITICAL, key,
        BigDecimal.valueOf(92.0), BigDecimal.valueOf(85.0), 3);

    verify(emailService).sendToOperator(anyString(), contains("JVM CPU 사용률"));
    verify(emailService).sendToOperator(anyString(), not(contains("## LLM 분석")));
  }
}
