package com.example.pythia.alert.service;

import com.example.pythia.alert.domain.MetricKind;
import com.example.pythia.alert.domain.Severity;
import com.example.pythia.alert.domain.ViolationKey;
import java.math.BigDecimal;
import org.springframework.stereotype.Component;

@Component
public class AlertMessageFormatter {

  public String subject(MetricKind kind, Severity severity, ViolationKey key) {
    return "[%s] %s 위반 (%s / %s)".formatted(
        severity.name(), kind.getDisplayName(), key.application(), key.instance());
  }

  public String body(MetricKind kind, Severity severity, ViolationKey key,
      BigDecimal value, BigDecimal threshold, int consecutive, String analysis) {
    StringBuilder sb = new StringBuilder();
    sb.append("임계값 위반이 감지되었습니다.\n\n");
    sb.append("메트릭: ").append(kind.getDisplayName()).append("\n");
    sb.append("심각도: ").append(severity.name()).append("\n");
    sb.append("애플리케이션: ").append(key.application()).append("\n");
    sb.append("인스턴스: ").append(key.instance()).append("\n");
    if (key.sub() != null) {
      sb.append("대상: ").append(key.sub()).append("\n");
    }
    sb.append("현재값: ").append(value).append(" ").append(kind.getUnit()).append("\n");
    sb.append("임계값: ").append(threshold).append(" ").append(kind.getUnit()).append("\n");
    sb.append("연속 위반: ").append(consecutive).append("회\n");
    if (analysis != null && !analysis.isBlank()) {
      sb.append("\n## LLM 분석\n").append(analysis).append("\n");
    }
    return sb.toString();
  }
}
