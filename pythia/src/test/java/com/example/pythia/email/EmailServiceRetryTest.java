package com.example.pythia.email;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.example.pythia.email.config.EmailProperties;
import com.example.pythia.email.dto.EmailRequest;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.mail.MailException;
import org.springframework.mail.MailSendException;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;

class EmailServiceRetryTest {

  private JavaMailSender mailSender;
  private EmailProperties emailProperties;
  private Retry retry;
  private RetryRegistry retryRegistry;

  @BeforeEach
  void setUp() {
    mailSender = mock(JavaMailSender.class);
    emailProperties = mock(EmailProperties.class);
    when(emailProperties.from()).thenReturn("from@example.com");
    when(emailProperties.operatorRecipients()).thenReturn(List.of("ops@example.com"));

    RetryConfig config = RetryConfig.custom()
        .maxAttempts(3)
        .waitDuration(Duration.ofMillis(10))
        .retryExceptions(MailException.class)
        .build();
    retryRegistry = RetryRegistry.of(config);
    retry = retryRegistry.retry("emailSender");
  }

  private EmailRequest validRequest() {
    return new EmailRequest(List.of("to@example.com"), "Subject", "Body");
  }

  @Test
  @DisplayName("첫 시도에 성공하면 mailSender.send 는 1회 호출된다")
  void firstAttemptSuccessCallsSendOnce() {
    EmailService service = new EmailService(mailSender, emailProperties, retryRegistry);

    retry.executeRunnable(() -> service.send(validRequest()));

    verify(mailSender, times(1)).send(any(SimpleMailMessage.class));
  }

  @Test
  @DisplayName("MailException 2회 후 3번째에 성공하면 총 3회 호출된다")
  void retriesTwiceBeforeSuccess() {
    EmailService service = new EmailService(mailSender, emailProperties, retryRegistry);

    org.mockito.Mockito.doThrow(new MailSendException("fail1"))
        .doThrow(new MailSendException("fail2"))
        .doNothing()
        .when(mailSender).send(any(SimpleMailMessage.class));

    retry.executeRunnable(() -> service.send(validRequest()));

    verify(mailSender, times(3)).send(any(SimpleMailMessage.class));
  }

  @Test
  @DisplayName("MailException 이 3회 모두 발생하면 마지막 예외가 전파된다")
  void propagatesExceptionAfterMaxAttempts() {
    EmailService service = new EmailService(mailSender, emailProperties, retryRegistry);

    org.mockito.Mockito.doThrow(new MailSendException("always fail"))
        .when(mailSender).send(any(SimpleMailMessage.class));

    assertThatThrownBy(() -> retry.executeRunnable(() -> service.send(validRequest())))
        .isInstanceOf(MailException.class);

    verify(mailSender, times(3)).send(any(SimpleMailMessage.class));
  }

  @Test
  @DisplayName("최대 시도 수 소진 시 retry 메트릭이 기록된다")
  void recordsRetryMetricsOnFailure() {
    EmailService service = new EmailService(mailSender, emailProperties, retryRegistry);

    org.mockito.Mockito.doThrow(new MailSendException("always fail"))
        .when(mailSender).send(any(SimpleMailMessage.class));

    try {
      retry.executeRunnable(() -> service.send(validRequest()));
    } catch (Exception ignored) {
    }

    assertThat(retry.getMetrics().getNumberOfFailedCallsWithRetryAttempt()).isEqualTo(1);
  }

  @Test
  @DisplayName("sendToOperator 는 retryRegistry 설정에 따라 재시도한다")
  void sendToOperatorUsesRetryRegistry() {
    EmailService service = new EmailService(mailSender, emailProperties, retryRegistry);

    org.mockito.Mockito.doThrow(new MailSendException("fail1"))
        .doThrow(new MailSendException("fail2"))
        .doNothing()
        .when(mailSender).send(any(SimpleMailMessage.class));

    service.sendToOperator("Test Subject", "Test Body");

    verify(mailSender, times(3)).send(any(SimpleMailMessage.class));
  }
}
