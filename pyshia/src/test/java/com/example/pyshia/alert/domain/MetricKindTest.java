package com.example.pyshia.alert.domain;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.pyshia.alert.domain.MetricKind.ComparisonOperator;
import java.math.BigDecimal;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class MetricKindTest {

  @Test
  @DisplayName("GT: value > threshold이면 true")
  void GT_초과이면_true() {
    assertThat(ComparisonOperator.GT.test(BigDecimal.valueOf(86), BigDecimal.valueOf(85))).isTrue();
  }

  @Test
  @DisplayName("GT: value == threshold이면 false (경계값)")
  void GT_경계값이면_false() {
    assertThat(ComparisonOperator.GT.test(BigDecimal.valueOf(85), BigDecimal.valueOf(85))).isFalse();
  }

  @Test
  @DisplayName("GT: value < threshold이면 false")
  void GT_미만이면_false() {
    assertThat(ComparisonOperator.GT.test(BigDecimal.valueOf(84), BigDecimal.valueOf(85))).isFalse();
  }

  @Test
  @DisplayName("GTE: value > threshold이면 true")
  void GTE_초과이면_true() {
    assertThat(ComparisonOperator.GTE.test(BigDecimal.valueOf(9), BigDecimal.valueOf(8))).isTrue();
  }

  @Test
  @DisplayName("GTE: value == threshold이면 true (경계값)")
  void GTE_경계값이면_true() {
    assertThat(ComparisonOperator.GTE.test(BigDecimal.valueOf(8), BigDecimal.valueOf(8))).isTrue();
  }

  @Test
  @DisplayName("GTE: value < threshold이면 false")
  void GTE_미만이면_false() {
    assertThat(ComparisonOperator.GTE.test(BigDecimal.valueOf(7), BigDecimal.valueOf(8))).isFalse();
  }
}
