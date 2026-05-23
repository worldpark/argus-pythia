package com.example.pythia.alert.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.example.pythia.alert.config.ThresholdProperties;
import com.example.pythia.alert.config.ThresholdProperties.HikariThresholds;
import com.example.pythia.alert.config.ThresholdProperties.HttpThresholds;
import com.example.pythia.alert.config.ThresholdProperties.JvmThresholds;
import com.example.pythia.alert.config.ThresholdProperties.Limit;
import com.example.pythia.alert.domain.MetricKind;
import com.example.pythia.alert.domain.Severity;
import com.example.pythia.alert.exception.ViolationStateErrorCode;
import com.example.pythia.alert.exception.ViolationStateException;
import com.example.pythia.alert.state.ViolationStateStore;
import com.example.pythia.kafka.dto.MetricStatus;
import com.example.pythia.kafka.dto.SnapshotStatus;
import com.example.pythia.kafka.dto.jvm.CpuUsageDto;
import com.example.pythia.kafka.dto.jvm.JvmMetricSnapshotDto;
import com.example.pythia.kafka.dto.jvm.MemoryUsageDto;
import java.math.BigDecimal;
import java.time.OffsetDateTime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * ThresholdEvaluator 의 catch 블록 분리(N8) 검증 테스트.
 * ViolationStateException 및 기타 RuntimeException 발생 시
 * 해당 메트릭만 skip하고 다음 메트릭은 계속 처리하는지 검증한다.
 */
@ExtendWith(MockitoExtension.class)
class ThresholdEvaluatorExceptionHandlingTest {

  @Mock
  private ViolationStateStore store;

  @Mock
  private AlertNotifier notifier;

  private ThresholdEvaluator evaluator;

  @BeforeEach
  void setUp() {
    ThresholdProperties properties = buildTestProperties();
    evaluator = new ThresholdEvaluator(properties, store, notifier);
  }

  @Test
  @DisplayName("ViolationStateException 발생 시 해당 메트릭을 skip하고 다음 메트릭은 계속 처리된다")
  void ViolationStateException_발생시_해당_메트릭_skip_후_다음_메트릭_정상_처리() {
    // CPU 평가에서 ViolationStateException 발생, Memory 평가는 정상
    when(store.shouldSend(any(), eq(Severity.WARNING), anyInt()))
        .thenThrow(new ViolationStateException(ViolationStateErrorCode.REDIS_ACCESS_FAILED))
        .thenReturn(true);

    JvmMetricSnapshotDto snapshot = jvmSnapshotWithCpuAndMemory(
        BigDecimal.valueOf(75),  // CPU: WARNING 범위
        BigDecimal.valueOf(80)   // HEAP: WARNING 범위
    );

    evaluator.evaluateJvm(snapshot);

    // CPU는 ViolationStateException으로 skip, notify 미호출
    verify(notifier, never()).notify(eq(MetricKind.JVM_CPU), any(), any(), any(), any(), anyInt());
    // HEAP은 정상 처리, notify 호출
    verify(notifier, times(1)).notify(eq(MetricKind.JVM_HEAP), eq(Severity.WARNING), any(), any(), any(), anyInt());
  }

  @Test
  @DisplayName("기타 RuntimeException 발생 시 해당 메트릭을 skip하고 다음 메트릭은 계속 처리된다")
  void RuntimeException_발생시_해당_메트릭_skip_후_다음_메트릭_정상_처리() {
    // CPU 평가에서 IllegalStateException 발생, Memory 평가는 정상
    when(store.shouldSend(any(), eq(Severity.WARNING), anyInt()))
        .thenThrow(new IllegalStateException("Unexpected state"))
        .thenReturn(true);

    JvmMetricSnapshotDto snapshot = jvmSnapshotWithCpuAndMemory(
        BigDecimal.valueOf(75),  // CPU: WARNING 범위
        BigDecimal.valueOf(80)   // HEAP: WARNING 범위
    );

    evaluator.evaluateJvm(snapshot);

    // CPU는 IllegalStateException으로 skip, notify 미호출
    verify(notifier, never()).notify(eq(MetricKind.JVM_CPU), any(), any(), any(), any(), anyInt());
    // HEAP은 정상 처리, notify 호출
    verify(notifier, times(1)).notify(eq(MetricKind.JVM_HEAP), eq(Severity.WARNING), any(), any(), any(), anyInt());
  }

  private ThresholdProperties buildTestProperties() {
    Limit cpu = new Limit(BigDecimal.valueOf(70), BigDecimal.valueOf(85), 3);
    Limit heap = new Limit(BigDecimal.valueOf(75), BigDecimal.valueOf(90), 2);
    Limit oldGen = new Limit(BigDecimal.valueOf(80), BigDecimal.valueOf(90), 2);
    Limit gcPause = new Limit(BigDecimal.valueOf(0.2), BigDecimal.valueOf(0.5), 1);
    Limit gcCount = new Limit(BigDecimal.valueOf(10), BigDecimal.valueOf(30), 1);
    Limit threadActive = new Limit(BigDecimal.valueOf(200), BigDecimal.valueOf(500), 2);
    Limit threadPeak = new Limit(BigDecimal.valueOf(300), BigDecimal.valueOf(800), 1);
    Limit threadDaemon = new Limit(BigDecimal.valueOf(200), BigDecimal.valueOf(500), 2);
    JvmThresholds jvm = new JvmThresholds(cpu, heap, oldGen, gcPause, gcCount,
        threadActive, threadPeak, threadDaemon);

    Limit p99 = new Limit(BigDecimal.valueOf(1.0), BigDecimal.valueOf(3.0), 2);
    Limit errorRate = new Limit(BigDecimal.valueOf(1.0), BigDecimal.valueOf(5.0), 2);
    HttpThresholds http = new HttpThresholds(p99, errorRate);

    Limit active = new Limit(BigDecimal.valueOf(8), BigDecimal.valueOf(10), 2);
    Limit pending = new Limit(BigDecimal.valueOf(1), BigDecimal.valueOf(5), 1);
    Limit usageRatio = new Limit(BigDecimal.valueOf(80), BigDecimal.valueOf(95), 2);
    HikariThresholds hikari = new HikariThresholds(active, pending, usageRatio);

    return new ThresholdProperties(jvm, http, hikari);
  }

  private JvmMetricSnapshotDto jvmSnapshotWithCpuAndMemory(BigDecimal cpuValue, BigDecimal heapValue) {
    CpuUsageDto cpu = new CpuUsageDto(cpuValue, OffsetDateTime.now(), MetricStatus.SUCCESS, null);
    MemoryUsageDto memory = new MemoryUsageDto(
        heapValue, BigDecimal.valueOf(50), OffsetDateTime.now(), MetricStatus.SUCCESS, null);
    return new JvmMetricSnapshotDto("argus", "localhost:8080", OffsetDateTime.now(),
        cpu, memory, null, null, SnapshotStatus.COMPLETE);
  }
}
