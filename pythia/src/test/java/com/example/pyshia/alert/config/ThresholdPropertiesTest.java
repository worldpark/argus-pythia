package com.example.pyshia.alert.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.example.pyshia.alert.exception.ThresholdConfigException;
import java.math.BigDecimal;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

class ThresholdPropertiesTest {

  @Configuration
  @EnableConfigurationProperties(ThresholdProperties.class)
  static class Config {}

  private static final String[] ALL_THRESHOLD_PROPERTIES = {
      "pyshia.threshold.jvm.cpu-usage-percent.warning=70",
      "pyshia.threshold.jvm.cpu-usage-percent.critical=85",
      "pyshia.threshold.jvm.cpu-usage-percent.consecutive=3",
      "pyshia.threshold.jvm.heap-usage-percent.warning=75",
      "pyshia.threshold.jvm.heap-usage-percent.critical=90",
      "pyshia.threshold.jvm.heap-usage-percent.consecutive=2",
      "pyshia.threshold.jvm.old-gen-usage-percent.warning=80",
      "pyshia.threshold.jvm.old-gen-usage-percent.critical=90",
      "pyshia.threshold.jvm.old-gen-usage-percent.consecutive=2",
      "pyshia.threshold.jvm.gc-avg-pause-seconds.warning=0.2",
      "pyshia.threshold.jvm.gc-avg-pause-seconds.critical=0.5",
      "pyshia.threshold.jvm.gc-avg-pause-seconds.consecutive=1",
      "pyshia.threshold.jvm.gc-count.warning=10",
      "pyshia.threshold.jvm.gc-count.critical=30",
      "pyshia.threshold.jvm.gc-count.consecutive=1",
      "pyshia.threshold.jvm.thread-active-count.warning=200",
      "pyshia.threshold.jvm.thread-active-count.critical=500",
      "pyshia.threshold.jvm.thread-active-count.consecutive=2",
      "pyshia.threshold.jvm.thread-peak-count.warning=300",
      "pyshia.threshold.jvm.thread-peak-count.critical=800",
      "pyshia.threshold.jvm.thread-peak-count.consecutive=1",
      "pyshia.threshold.jvm.thread-daemon-count.warning=200",
      "pyshia.threshold.jvm.thread-daemon-count.critical=500",
      "pyshia.threshold.jvm.thread-daemon-count.consecutive=2",
      "pyshia.threshold.http.p99-response-seconds.warning=1.0",
      "pyshia.threshold.http.p99-response-seconds.critical=3.0",
      "pyshia.threshold.http.p99-response-seconds.consecutive=2",
      "pyshia.threshold.http.error-rate-percent.warning=1.0",
      "pyshia.threshold.http.error-rate-percent.critical=5.0",
      "pyshia.threshold.http.error-rate-percent.consecutive=2",
      "pyshia.threshold.hikari.active-connections.warning=8",
      "pyshia.threshold.hikari.active-connections.critical=10",
      "pyshia.threshold.hikari.active-connections.consecutive=2",
      "pyshia.threshold.hikari.pending-connections.warning=1",
      "pyshia.threshold.hikari.pending-connections.critical=5",
      "pyshia.threshold.hikari.pending-connections.consecutive=1",
      "pyshia.threshold.hikari.usage-ratio-percent.warning=80",
      "pyshia.threshold.hikari.usage-ratio-percent.critical=95",
      "pyshia.threshold.hikari.usage-ratio-percent.consecutive=2",
  };

  @Test
  @DisplayName("yml 값이 ThresholdProperties에 정상적으로 바인딩된다")
  void yml_값이_ThresholdProperties에_정상적으로_바인딩된다() {
    new ApplicationContextRunner()
        .withUserConfiguration(Config.class)
        .withPropertyValues(ALL_THRESHOLD_PROPERTIES)
        .run(ctx -> {
          assertThat(ctx).hasNotFailed();
          ThresholdProperties props = ctx.getBean(ThresholdProperties.class);
          assertThat(props.jvm().cpuUsagePercent().warning())
              .isEqualByComparingTo(BigDecimal.valueOf(70));
          assertThat(props.jvm().cpuUsagePercent().critical())
              .isEqualByComparingTo(BigDecimal.valueOf(85));
          assertThat(props.jvm().cpuUsagePercent().consecutive()).isEqualTo(3);
          assertThat(props.http().p99ResponseSeconds().warning())
              .isEqualByComparingTo(BigDecimal.valueOf(1.0));
          assertThat(props.hikari().activeConnections().critical())
              .isEqualByComparingTo(BigDecimal.valueOf(10));
        });
  }

  @Test
  @DisplayName("warning이 critical 미만이면 Limit 생성에 성공한다")
  void warning이_critical_미만이면_Limit_생성에_성공한다() {
    assertThatNoException().isThrownBy(() ->
        new ThresholdProperties.Limit(BigDecimal.valueOf(70), BigDecimal.valueOf(85), 3));
  }

  @Test
  @DisplayName("warning이 critical보다 크면 ThresholdConfigException이 발생한다")
  void warning이_critical보다_크면_ThresholdConfigException이_발생한다() {
    assertThatThrownBy(() ->
        new ThresholdProperties.Limit(BigDecimal.valueOf(90), BigDecimal.valueOf(70), 2))
        .isInstanceOf(ThresholdConfigException.class);
  }

  @Test
  @DisplayName("warning이 critical과 같으면 ThresholdConfigException이 발생한다")
  void warning이_critical과_같으면_ThresholdConfigException이_발생한다() {
    assertThatThrownBy(() ->
        new ThresholdProperties.Limit(BigDecimal.valueOf(85), BigDecimal.valueOf(85), 2))
        .isInstanceOf(ThresholdConfigException.class);
  }

  @Test
  @DisplayName("단조성 위반 임계값으로 컨텍스트 로딩에 실패한다")
  void 단조성_위반_임계값으로_컨텍스트_로딩에_실패한다() {
    String[] invalidProps = ALL_THRESHOLD_PROPERTIES.clone();
    for (int i = 0; i < invalidProps.length; i++) {
      if (invalidProps[i].equals("pyshia.threshold.jvm.cpu-usage-percent.warning=70")) {
        invalidProps[i] = "pyshia.threshold.jvm.cpu-usage-percent.warning=90";
      }
    }
    new ApplicationContextRunner()
        .withUserConfiguration(Config.class)
        .withPropertyValues(invalidProps)
        .run(ctx -> assertThat(ctx).hasFailed());
  }
}
