package com.example.pyshia.email;

import com.example.pyshia.email.config.EmailProperties;
import com.example.pyshia.email.dto.EmailRequest;
import com.example.pyshia.email.exception.EmailErrorCode;
import com.example.pyshia.email.exception.EmailSendException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.mail.MailException;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class EmailService {

  private final JavaMailSender mailSender;
  private final EmailProperties emailProperties;

  public EmailService(JavaMailSender mailSender, EmailProperties emailProperties) {
    this.mailSender = mailSender;
    this.emailProperties = emailProperties;
  }

  public void send(EmailRequest request) {
    validateRequest(request);
    try {
      SimpleMailMessage message = new SimpleMailMessage();
      message.setFrom(emailProperties.from());
      message.setTo(request.to().toArray(String[]::new));
      message.setSubject(request.subject());
      message.setText(request.body());
      mailSender.send(message);
      log.debug("Email sent: to={} subject={}", request.to(), request.subject());
    } catch (MailException e) {
      log.error("SMTP failure: to={} subject={} error={}", request.to(), request.subject(), e.getMessage());
      throw new EmailSendException(EmailErrorCode.SMTP_FAILURE, e.getMessage(), e);
    }
  }

  public void sendToOperator(String subject, String body) {
    send(new EmailRequest(emailProperties.operatorRecipients(), subject, body));
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
