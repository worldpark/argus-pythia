package com.example.pythia.alert.service;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

import com.example.pythia.alert.domain.MetricKind;
import com.example.pythia.alert.domain.Severity;
import com.example.pythia.alert.domain.ViolationKey;
import com.example.pythia.email.EmailService;
import com.example.pythia.email.exception.EmailErrorCode;
import com.example.pythia.email.exception.EmailSendException;
import java.math.BigDecimal;
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

  private AlertNotifier notifier;
  private ViolationKey key;

  @BeforeEach
  void setUp() {
    notifier = new AlertNotifier(emailService, new AlertMessageFormatter());
    key = new ViolationKey(MetricKind.JVM_CPU, "argus", "localhost:8080", null);
  }

  @Test
  @DisplayName("notify 호출 시 subject와 body를 담아 sendToOperator가 호출된다")
  void notify_호출시_sendToOperator가_호출된다() {
    notifier.notify(MetricKind.JVM_CPU, Severity.CRITICAL, key,
        BigDecimal.valueOf(92.0), BigDecimal.valueOf(85.0), 3);

    verify(emailService).sendToOperator(
        contains("[CRITICAL]"),
        contains("JVM CPU 사용률"));
  }

  @Test
  @DisplayName("subject에 severity와 애플리케이션 정보가 포함된다")
  void subject에_severity와_app_정보가_포함된다() {
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

    notifier.notify(MetricKind.HTTP_P99, Severity.WARNING, httpKey,
        BigDecimal.valueOf(1.5), BigDecimal.valueOf(1.0), 2);

    verify(emailService).sendToOperator(anyString(), contains("/api/orders"));
  }
}
