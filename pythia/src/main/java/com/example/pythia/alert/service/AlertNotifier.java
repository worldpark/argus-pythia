package com.example.pythia.alert.service;

import com.example.pythia.ai.service.MetricAnalysisService;
import com.example.pythia.alert.domain.MetricKind;
import com.example.pythia.alert.domain.Severity;
import com.example.pythia.alert.domain.ViolationKey;
import com.example.pythia.email.EmailService;
import com.example.pythia.email.exception.EmailSendException;
import java.math.BigDecimal;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.mail.MailException;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class AlertNotifier {

  private final EmailService emailService;
  private final AlertMessageFormatter formatter;
  private final MetricAnalysisService analysisService;
  private final MetricAnalysisRequestAssembler requestAssembler;

  @Async
  public void notify(MetricKind kind, Severity severity, ViolationKey key,
      BigDecimal value, BigDecimal threshold, int consecutive) {
    String analysis = analyzeQuietly(kind, key);
    try {
      String subject = formatter.subject(kind, severity, key);
      String body = formatter.body(kind, severity, key, value, threshold, consecutive, analysis);
      emailService.sendToOperator(subject, body);
      log.debug("Alert sent: kind={} severity={} app={} instance={}",
          kind, severity, key.application(), key.instance());
    } catch (EmailSendException e) {
      log.error("Failed to send alert: kind={} severity={} app={} instance={} error={}",
          kind, severity, key.application(), key.instance(), e.getMessage());
    } catch (MailException e) {
      // Resilience4j Retry 모두 소진 후 최종 MailException propagate 처리
      log.error("Email send failed after retries: kind={} severity={} app={} instance={} error={}",
          kind, severity, key.application(), key.instance(), e.getMessage());
    }
  }

  private String analyzeQuietly(MetricKind kind, ViolationKey key) {
    try {
      return analysisService.analyze(requestAssembler.assemble(kind, key));
    } catch (RuntimeException e) {
      log.warn("LLM analysis skipped: kind={} app={} instance={} error={}",
          kind, key.application(), key.instance(), e.getMessage());
      return null;
    }
  }
}
