package com.example.pyshia.alert.service;

import com.example.pyshia.alert.domain.MetricKind;
import com.example.pyshia.alert.domain.Severity;
import com.example.pyshia.alert.domain.ViolationKey;
import com.example.pyshia.email.EmailService;
import com.example.pyshia.email.exception.EmailSendException;
import java.math.BigDecimal;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class AlertNotifier {

  private final EmailService emailService;
  private final AlertMessageFormatter formatter;

  public AlertNotifier(EmailService emailService, AlertMessageFormatter formatter) {
    this.emailService = emailService;
    this.formatter = formatter;
  }

  @Async
  public void notify(MetricKind kind, Severity severity, ViolationKey key,
      BigDecimal value, BigDecimal threshold, int consecutive) {
    try {
      String subject = formatter.subject(kind, severity, key);
      String body = formatter.body(kind, severity, key, value, threshold, consecutive);
      emailService.sendToOperator(subject, body);
      log.debug("Alert sent: kind={} severity={} app={} instance={}",
          kind, severity, key.application(), key.instance());
    } catch (EmailSendException e) {
      log.error("Failed to send alert: kind={} severity={} app={} instance={} error={}",
          kind, severity, key.application(), key.instance(), e.getMessage());
    }
  }
}
