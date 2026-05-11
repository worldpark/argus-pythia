package com.example.pythia.alert.domain;

import java.math.BigDecimal;

public enum MetricKind {

  JVM_CPU("JVM CPU 사용률", "%", ComparisonOperator.GT),
  JVM_HEAP("JVM 힙 사용률", "%", ComparisonOperator.GT),
  JVM_HEAP_OLD_GEN("JVM Old Gen 사용률", "%", ComparisonOperator.GT),
  JVM_GC_PAUSE("JVM GC 평균 일시정지 시간", "초", ComparisonOperator.GT),
  JVM_GC_COUNT("JVM GC 빈도", "회", ComparisonOperator.GT),
  JVM_THREAD_ACTIVE("JVM 활성 스레드 수", "개", ComparisonOperator.GT),
  JVM_THREAD_PEAK("JVM 최대 스레드 수", "개", ComparisonOperator.GT),
  JVM_THREAD_DAEMON("JVM 데몬 스레드 수", "개", ComparisonOperator.GT),
  HTTP_P99("HTTP P99 응답시간", "초", ComparisonOperator.GT),
  HTTP_ERROR_RATE("HTTP 오류율", "%", ComparisonOperator.GT),
  HIKARI_ACTIVE("Hikari 활성 커넥션 수", "개", ComparisonOperator.GTE),
  HIKARI_PENDING("Hikari 대기 커넥션 수", "개", ComparisonOperator.GTE),
  HIKARI_USAGE_RATIO("Hikari 커넥션 사용률", "%", ComparisonOperator.GT);

  private final String displayName;
  private final String unit;
  private final ComparisonOperator operator;

  MetricKind(String displayName, String unit, ComparisonOperator operator) {
    this.displayName = displayName;
    this.unit = unit;
    this.operator = operator;
  }

  public String getDisplayName() {
    return displayName;
  }

  public String getUnit() {
    return unit;
  }

  public ComparisonOperator getOperator() {
    return operator;
  }

  public enum ComparisonOperator {
    GT {
      @Override
      public boolean test(BigDecimal value, BigDecimal threshold) {
        return value.compareTo(threshold) > 0;
      }
    },
    GTE {
      @Override
      public boolean test(BigDecimal value, BigDecimal threshold) {
        return value.compareTo(threshold) >= 0;
      }
    };

    public abstract boolean test(BigDecimal value, BigDecimal threshold);
  }
}
