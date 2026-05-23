package com.example.pythia.email;

import com.example.pythia.email.config.EmailProperties;
import com.example.pythia.email.dto.EmailRequest;
import com.example.pythia.email.exception.EmailErrorCode;
import com.example.pythia.email.exception.EmailSendException;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class EmailService {

  private final JavaMailSender mailSender;
  private final EmailProperties emailProperties;
  private final RetryRegistry retryRegistry;

  public void send(EmailRequest request) {
    validateRequest(request);
    SimpleMailMessage message = new SimpleMailMessage();
    message.setFrom(emailProperties.from());
    message.setTo(request.to().toArray(String[]::new));
    message.setSubject(request.subject());
    message.setText(request.body());
    mailSender.send(message);
    log.debug("Email sent: to={} subject={}", request.to(), request.subject());
  }

  public void sendToOperator(String subject, String body) {
    Retry retry = retryRegistry.retry("emailSender");
    Retry.decorateRunnable(
        retry,
        () -> send(new EmailRequest(emailProperties.operatorRecipients(), subject, body)))
        .run();
  }

  private void validateRequest(EmailRequest request) {
    if (request.to() == null || request.to().isEmpty()) {
      throw new EmailSendException(EmailErrorCode.INVALID_RECIPIENT, "Recipient list must not be empty");
    }
    if (request.subject() == null || request.subject().isBlank()) {
      throw new EmailSendException(EmailErrorCode.INVALID_PAYLOAD, "Email subject must not be blank");
    }
    if (request.body() == null || request.body().isBlank()) {
      throw new EmailSendException(EmailErrorCode.INVALID_PAYLOAD, "Email body must not be blank");
    }
  }
}
