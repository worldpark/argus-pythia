package com.example.pythia.alert.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.pythia.alert.domain.MetricKind;
import com.example.pythia.alert.domain.Severity;
import com.example.pythia.alert.domain.ViolationKey;
import java.math.BigDecimal;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class AlertMessageFormatterTest {

  private AlertMessageFormatter formatter;

  @BeforeEach
  void setUp() {
    formatter = new AlertMessageFormatter();
  }

  @Test
  @DisplayName("subject에 [CRITICAL] severity와 displayName, 앱 정보가 포함된다")
  void subject_CRITICAL_포맷_검증() {
    ViolationKey key = new ViolationKey(MetricKind.JVM_CPU, "argus", "localhost:8080", null);
    String subject = formatter.subject(MetricKind.JVM_CPU, Severity.CRITICAL, key);
    assertThat(subject)
        .contains("[CRITICAL]")
        .contains("JVM CPU 사용률")
        .contains("argus")
        .contains("localhost:8080");
  }

  @Test
  @DisplayName("subject에 [WARNING] severity가 포함된다")
  void subject_WARNING_포맷_검증() {
    ViolationKey key = new ViolationKey(MetricKind.JVM_HEAP, "myapp", "host:9090", null);
    String subject = formatter.subject(MetricKind.JVM_HEAP, Severity.WARNING, key);
    assertThat(subject).startsWith("[WARNING]");
  }

  @Test
  @DisplayName("body에 현재값과 임계값이 포함된다")
  void body_현재값_임계값_포함() {
    ViolationKey key = new ViolationKey(MetricKind.JVM_CPU, "argus", "localhost:8080", null);
    String body = formatter.body(MetricKind.JVM_CPU, Severity.CRITICAL, key,
        BigDecimal.valueOf(92), BigDecimal.valueOf(85), 3, null);
    assertThat(body).contains("92").contains("85");
  }

  @Test
  @DisplayName("body에 CPU 메트릭의 단위(%)가 포함된다")
  void body_CPU_단위_퍼센트_포함() {
    ViolationKey key = new ViolationKey(MetricKind.JVM_CPU, "argus", "localhost:8080", null);
    String body = formatter.body(MetricKind.JVM_CPU, Severity.CRITICAL, key,
        BigDecimal.valueOf(92), BigDecimal.valueOf(85), 3, null);
    assertThat(body).contains("%");
  }

  @Test
  @DisplayName("body에 GC_PAUSE 메트릭의 단위(초)가 포함된다")
  void body_GC_PAUSE_단위_초_포함() {
    ViolationKey key = new ViolationKey(MetricKind.JVM_GC_PAUSE, "argus", "localhost:8080", null);
    String body = formatter.body(MetricKind.JVM_GC_PAUSE, Severity.WARNING, key,
        BigDecimal.valueOf(0.3), BigDecimal.valueOf(0.2), 1, null);
    assertThat(body).contains("초");
  }

  @Test
  @DisplayName("sub가 null이면 body에 대상 정보가 포함되지 않는다")
  void sub_null이면_body에_대상_미포함() {
    ViolationKey key = new ViolationKey(MetricKind.JVM_CPU, "argus", "localhost:8080", null);
    String body = formatter.body(MetricKind.JVM_CPU, Severity.CRITICAL, key,
        BigDecimal.valueOf(92), BigDecimal.valueOf(85), 3, null);
    assertThat(body).doesNotContain("대상:");
  }

  @Test
  @DisplayName("sub가 있으면 body에 대상 정보가 포함된다")
  void sub_있으면_body에_대상_포함() {
    ViolationKey key = new ViolationKey(MetricKind.HTTP_P99, "argus", "localhost:8080", "/api/orders");
    String body = formatter.body(MetricKind.HTTP_P99, Severity.WARNING, key,
        BigDecimal.valueOf(1.5), BigDecimal.valueOf(1.0), 2, null);
    assertThat(body).contains("대상:").contains("/api/orders");
  }

  @Test
  @DisplayName("analysis가 null이면 LLM 분석 섹션이 본문에 포함되지 않는다")
  void analysis_null이면_LLM_분석_섹션_미포함() {
    ViolationKey key = new ViolationKey(MetricKind.JVM_CPU, "argus", "localhost:8080", null);
    String body = formatter.body(MetricKind.JVM_CPU, Severity.CRITICAL, key,
        BigDecimal.valueOf(92), BigDecimal.valueOf(85), 3, null);
    assertThat(body).doesNotContain("## LLM 분석");
  }

  @Test
  @DisplayName("analysis가 blank이면 LLM 분석 섹션이 본문에 포함되지 않는다")
  void analysis_blank이면_LLM_분석_섹션_미포함() {
    ViolationKey key = new ViolationKey(MetricKind.JVM_CPU, "argus", "localhost:8080", null);
    String body = formatter.body(MetricKind.JVM_CPU, Severity.CRITICAL, key,
        BigDecimal.valueOf(92), BigDecimal.valueOf(85), 3, "   ");
    assertThat(body).doesNotContain("## LLM 분석");
  }

  @Test
  @DisplayName("analysis가 있으면 LLM 분석 섹션과 본문이 포함된다")
  void analysis_있으면_LLM_분석_섹션_포함() {
    ViolationKey key = new ViolationKey(MetricKind.JVM_CPU, "argus", "localhost:8080", null);
    String body = formatter.body(MetricKind.JVM_CPU, Severity.CRITICAL, key,
        BigDecimal.valueOf(92), BigDecimal.valueOf(85), 3, "분석결과 본문");
    assertThat(body).contains("## LLM 분석").contains("분석결과 본문");
  }
}
