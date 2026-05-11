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
import com.example.pythia.alert.domain.ViolationKey;
import com.example.pythia.alert.state.ViolationStateStore;
import com.example.pythia.kafka.dto.MetricStatus;
import com.example.pythia.kafka.dto.SnapshotStatus;
import com.example.pythia.kafka.dto.hikari.HikariActiveDto;
import com.example.pythia.kafka.dto.hikari.HikariMetricSnapshotDto;
import com.example.pythia.kafka.dto.hikari.HikariPendingDto;
import com.example.pythia.kafka.dto.hikari.PoolMetricPointDto;
import com.example.pythia.kafka.dto.http.EndpointMetricPointDto;
import com.example.pythia.kafka.dto.http.HttpErrorRateDto;
import com.example.pythia.kafka.dto.http.HttpMetricSnapshotDto;
import com.example.pythia.kafka.dto.http.HttpResponseTimeDto;
import com.example.pythia.kafka.dto.http.HttpThroughputDto;
import com.example.pythia.kafka.dto.jvm.CpuUsageDto;
import com.example.pythia.kafka.dto.jvm.GcMetricDto;
import com.example.pythia.kafka.dto.jvm.JvmMetricSnapshotDto;
import com.example.pythia.kafka.dto.jvm.MemoryUsageDto;
import com.example.pythia.kafka.dto.jvm.ThreadMetricDto;
import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ThresholdEvaluatorTest {

  @Mock
  private ViolationStateStore store;

  @Mock
  private AlertNotifier notifier;

  private ThresholdEvaluator evaluator;
  private ThresholdProperties properties;

  @BeforeEach
  void setUp() {
    properties = buildTestProperties();
    evaluator = new ThresholdEvaluator(properties, store, notifier);
  }

  // ───────────────── JVM CPU ─────────────────

  @Test
  @DisplayName("JVM CPU가 정상 범위이면 notify가 호출되지 않는다")
  void JVM_CPU_정상값이면_notify_미호출() {
    evaluator.evaluateJvm(jvmSnapshot(cpu(BigDecimal.valueOf(50), MetricStatus.SUCCESS)));
    verify(notifier, never()).notify(any(), any(), any(), any(), any(), anyInt());
  }

  @Test
  @DisplayName("JVM CPU가 warning 임계를 1회 위반해도 window(3) 미충족 시 notify 미호출")
  void JVM_CPU_warning_1회_위반시_window_미충족으로_notify_미호출() {
    when(store.shouldSend(any(), eq(Severity.WARNING), anyInt())).thenReturn(false);
    evaluator.evaluateJvm(jvmSnapshot(cpu(BigDecimal.valueOf(75), MetricStatus.SUCCESS)));
    verify(notifier, never()).notify(any(), any(), any(), any(), any(), anyInt());
  }

  @Test
  @DisplayName("JVM CPU가 warning 임계 위반 + window 충족 시 WARNING notify가 호출된다")
  void JVM_CPU_warning_위반_window_충족시_WARNING_notify_호출() {
    when(store.shouldSend(any(), eq(Severity.WARNING), anyInt())).thenReturn(true);
    evaluator.evaluateJvm(jvmSnapshot(cpu(BigDecimal.valueOf(75), MetricStatus.SUCCESS)));
    verify(notifier).notify(eq(MetricKind.JVM_CPU), eq(Severity.WARNING), any(), any(), any(), anyInt());
  }

  @Test
  @DisplayName("JVM CPU가 critical 임계 위반 + window=1 충족 시 CRITICAL notify가 호출된다")
  void JVM_CPU_critical_위반시_CRITICAL_notify_호출() {
    when(store.shouldSend(any(), eq(Severity.CRITICAL), anyInt())).thenReturn(true);
    evaluator.evaluateJvm(jvmSnapshot(cpu(BigDecimal.valueOf(90), MetricStatus.SUCCESS)));
    verify(notifier).notify(eq(MetricKind.JVM_CPU), eq(Severity.CRITICAL), any(), any(), any(), anyInt());
  }

  @Test
  @DisplayName("JVM CPU 값이 null이면 skip하고 notify가 호출되지 않는다")
  void JVM_CPU_값이_null이면_skip() {
    evaluator.evaluateJvm(jvmSnapshot(cpu(null, MetricStatus.SUCCESS)));
    verify(notifier, never()).notify(any(), any(), any(), any(), any(), anyInt());
  }

  @Test
  @DisplayName("JVM CPU status가 SUCCESS가 아니면 skip한다")
  void JVM_CPU_status가_SUCCESS_아니면_skip() {
    evaluator.evaluateJvm(jvmSnapshot(cpu(BigDecimal.valueOf(90), MetricStatus.QUERY_FAILED)));
    verify(notifier, never()).notify(any(), any(), any(), any(), any(), anyInt());
  }

  @Test
  @DisplayName("JVM CPU 정상 복귀 시 store.clear가 호출된다")
  void JVM_CPU_정상복귀시_store_clear_호출() {
    ViolationKey expectedKey = new ViolationKey(MetricKind.JVM_CPU, "argus", "localhost:8080", null);
    evaluator.evaluateJvm(jvmSnapshot(cpu(BigDecimal.valueOf(50), MetricStatus.SUCCESS)));
    verify(store).clear(expectedKey);
  }

  // ───────────────── JVM HEAP ─────────────────

  @Test
  @DisplayName("JVM HEAP 사용률이 warning 위반 + window 충족 시 WARNING notify가 호출된다")
  void JVM_HEAP_warning_위반_window_충족시_notify_호출() {
    when(store.shouldSend(any(), eq(Severity.WARNING), anyInt())).thenReturn(true);
    MemoryUsageDto memory = new MemoryUsageDto(
        BigDecimal.valueOf(80), BigDecimal.valueOf(70), OffsetDateTime.now(), MetricStatus.SUCCESS, null);
    evaluator.evaluateJvm(jvmSnapshot(null, memory, null, null));
    verify(notifier).notify(eq(MetricKind.JVM_HEAP), eq(Severity.WARNING), any(), any(), any(), anyInt());
  }

  @Test
  @DisplayName("JVM memory가 null이면 HEAP 메트릭을 skip한다")
  void JVM_memory_null이면_HEAP_skip() {
    evaluator.evaluateJvm(jvmSnapshot(null, null, null, null));
    verify(notifier, never()).notify(eq(MetricKind.JVM_HEAP), any(), any(), any(), any(), anyInt());
    verify(notifier, never()).notify(eq(MetricKind.JVM_HEAP_OLD_GEN), any(), any(), any(), any(), anyInt());
  }

  @Test
  @DisplayName("JVM HEAP 정상 복귀 시 store.clear가 호출된다")
  void JVM_HEAP_정상복귀시_store_clear_호출() {
    ViolationKey key = new ViolationKey(MetricKind.JVM_HEAP, "argus", "localhost:8080", null);
    MemoryUsageDto memory = new MemoryUsageDto(
        BigDecimal.valueOf(50), BigDecimal.valueOf(50), OffsetDateTime.now(), MetricStatus.SUCCESS, null);
    evaluator.evaluateJvm(jvmSnapshot(null, memory, null, null));
    verify(store).clear(key);
  }

  @Test
  @DisplayName("JVM Old Gen 사용률이 critical 임계 위반 시 CRITICAL notify가 호출된다")
  void JVM_OLD_GEN_critical_위반시_notify_호출() {
    when(store.shouldSend(any(), eq(Severity.CRITICAL), anyInt())).thenReturn(true);
    MemoryUsageDto memory = new MemoryUsageDto(
        BigDecimal.valueOf(60), BigDecimal.valueOf(92), OffsetDateTime.now(), MetricStatus.SUCCESS, null);
    evaluator.evaluateJvm(jvmSnapshot(null, memory, null, null));
    verify(notifier).notify(eq(MetricKind.JVM_HEAP_OLD_GEN), eq(Severity.CRITICAL), any(), any(), any(), anyInt());
  }

  // ───────────────── JVM GC ─────────────────

  @Test
  @DisplayName("JVM GC 평균 일시정지 시간이 warning 위반 + window 충족 시 notify가 호출된다")
  void JVM_GC_PAUSE_warning_위반_window_충족시_notify_호출() {
    when(store.shouldSend(any(), eq(Severity.WARNING), anyInt())).thenReturn(true);
    GcMetricDto gc = new GcMetricDto(
        BigDecimal.valueOf(0.3), BigDecimal.valueOf(5), OffsetDateTime.now(), MetricStatus.SUCCESS, null);
    evaluator.evaluateJvm(jvmSnapshot(null, null, gc, null));
    verify(notifier).notify(eq(MetricKind.JVM_GC_PAUSE), eq(Severity.WARNING), any(), any(), any(), anyInt());
  }

  @Test
  @DisplayName("JVM gc가 null이면 GC 메트릭을 skip한다")
  void JVM_gc_null이면_GC_메트릭_skip() {
    evaluator.evaluateJvm(jvmSnapshot(null, null, null, null));
    verify(notifier, never()).notify(eq(MetricKind.JVM_GC_PAUSE), any(), any(), any(), any(), anyInt());
    verify(notifier, never()).notify(eq(MetricKind.JVM_GC_COUNT), any(), any(), any(), any(), anyInt());
  }

  @Test
  @DisplayName("JVM GC 정상 복귀 시 store.clear가 호출된다")
  void JVM_GC_PAUSE_정상복귀시_store_clear_호출() {
    ViolationKey key = new ViolationKey(MetricKind.JVM_GC_PAUSE, "argus", "localhost:8080", null);
    GcMetricDto gc = new GcMetricDto(
        BigDecimal.valueOf(0.05), BigDecimal.valueOf(2), OffsetDateTime.now(), MetricStatus.SUCCESS, null);
    evaluator.evaluateJvm(jvmSnapshot(null, null, gc, null));
    verify(store).clear(key);
  }

  @Test
  @DisplayName("JVM GC 빈도가 warning 임계 위반 + window 충족 시 notify가 호출된다")
  void JVM_GC_COUNT_warning_위반_window_충족시_notify_호출() {
    when(store.shouldSend(any(), eq(Severity.WARNING), anyInt())).thenReturn(true);
    GcMetricDto gc = new GcMetricDto(
        BigDecimal.valueOf(0.05), BigDecimal.valueOf(15), OffsetDateTime.now(), MetricStatus.SUCCESS, null);
    evaluator.evaluateJvm(jvmSnapshot(null, null, gc, null));
    verify(notifier).notify(eq(MetricKind.JVM_GC_COUNT), eq(Severity.WARNING), any(), any(), any(), anyInt());
  }

  // ───────────────── JVM Thread ─────────────────

  @Test
  @DisplayName("JVM 활성 스레드 수가 warning 위반 + window 충족 시 notify가 호출된다")
  void JVM_THREAD_ACTIVE_warning_위반_window_충족시_notify_호출() {
    when(store.shouldSend(any(), eq(Severity.WARNING), anyInt())).thenReturn(true);
    ThreadMetricDto thread = new ThreadMetricDto(250, 100, 100, OffsetDateTime.now(), MetricStatus.SUCCESS, null);
    evaluator.evaluateJvm(jvmSnapshot(null, null, null, thread));
    verify(notifier).notify(eq(MetricKind.JVM_THREAD_ACTIVE), eq(Severity.WARNING), any(), any(), any(), anyInt());
  }

  @Test
  @DisplayName("JVM thread가 null이면 THREAD 메트릭을 skip한다")
  void JVM_thread_null이면_THREAD_메트릭_skip() {
    evaluator.evaluateJvm(jvmSnapshot(null, null, null, null));
    verify(notifier, never()).notify(eq(MetricKind.JVM_THREAD_ACTIVE), any(), any(), any(), any(), anyInt());
  }

  @Test
  @DisplayName("JVM 활성 스레드 수 정상 복귀 시 store.clear가 호출된다")
  void JVM_THREAD_ACTIVE_정상복귀시_store_clear_호출() {
    ViolationKey key = new ViolationKey(MetricKind.JVM_THREAD_ACTIVE, "argus", "localhost:8080", null);
    ThreadMetricDto thread = new ThreadMetricDto(100, 100, 100, OffsetDateTime.now(), MetricStatus.SUCCESS, null);
    evaluator.evaluateJvm(jvmSnapshot(null, null, null, thread));
    verify(store).clear(key);
  }

  @Test
  @DisplayName("JVM 최대 스레드 수가 warning 임계 위반 시 WARNING notify가 호출된다")
  void JVM_THREAD_PEAK_warning_위반시_notify_호출() {
    when(store.shouldSend(any(), eq(Severity.WARNING), anyInt())).thenReturn(true);
    ThreadMetricDto thread = new ThreadMetricDto(50, 350, 50, OffsetDateTime.now(), MetricStatus.SUCCESS, null);
    evaluator.evaluateJvm(jvmSnapshot(null, null, null, thread));
    verify(notifier).notify(eq(MetricKind.JVM_THREAD_PEAK), eq(Severity.WARNING), any(), any(), any(), anyInt());
  }

  @Test
  @DisplayName("JVM 최대 스레드 수 정상 복귀 시 store.clear가 호출된다")
  void JVM_THREAD_PEAK_정상복귀시_store_clear_호출() {
    ViolationKey key = new ViolationKey(MetricKind.JVM_THREAD_PEAK, "argus", "localhost:8080", null);
    ThreadMetricDto thread = new ThreadMetricDto(50, 100, 50, OffsetDateTime.now(), MetricStatus.SUCCESS, null);
    evaluator.evaluateJvm(jvmSnapshot(null, null, null, thread));
    verify(store).clear(key);
  }

  @Test
  @DisplayName("JVM 데몬 스레드 수가 warning 임계 위반 시 WARNING notify가 호출된다")
  void JVM_THREAD_DAEMON_warning_위반시_notify_호출() {
    when(store.shouldSend(any(), eq(Severity.WARNING), anyInt())).thenReturn(true);
    ThreadMetricDto thread = new ThreadMetricDto(50, 100, 250, OffsetDateTime.now(), MetricStatus.SUCCESS, null);
    evaluator.evaluateJvm(jvmSnapshot(null, null, null, thread));
    verify(notifier).notify(eq(MetricKind.JVM_THREAD_DAEMON), eq(Severity.WARNING), any(), any(), any(), anyInt());
  }

  @Test
  @DisplayName("JVM 데몬 스레드 수 정상 복귀 시 store.clear가 호출된다")
  void JVM_THREAD_DAEMON_정상복귀시_store_clear_호출() {
    ViolationKey key = new ViolationKey(MetricKind.JVM_THREAD_DAEMON, "argus", "localhost:8080", null);
    ThreadMetricDto thread = new ThreadMetricDto(50, 100, 100, OffsetDateTime.now(), MetricStatus.SUCCESS, null);
    evaluator.evaluateJvm(jvmSnapshot(null, null, null, thread));
    verify(store).clear(key);
  }

  // ───────────────── HTTP ─────────────────

  @Test
  @DisplayName("HTTP P99가 warning 임계 위반 + window 충족 시 WARNING notify가 호출된다")
  void HTTP_P99_warning_위반_window_충족시_notify_호출() {
    when(store.shouldSend(any(), eq(Severity.WARNING), anyInt())).thenReturn(true);
    evaluator.evaluateHttp(httpSnapshot(
        List.of(new EndpointMetricPointDto("/api/orders", BigDecimal.valueOf(1.5), OffsetDateTime.now())),
        List.of()));
    verify(notifier).notify(eq(MetricKind.HTTP_P99), eq(Severity.WARNING), any(), any(), any(), anyInt());
  }

  @Test
  @DisplayName("HTTP P99가 critical 임계 위반 + window 충족 시 CRITICAL notify가 호출된다")
  void HTTP_P99_critical_위반_window_충족시_notify_호출() {
    when(store.shouldSend(any(), eq(Severity.CRITICAL), anyInt())).thenReturn(true);
    evaluator.evaluateHttp(httpSnapshot(
        List.of(new EndpointMetricPointDto("/api/orders", BigDecimal.valueOf(3.5), OffsetDateTime.now())),
        List.of()));
    verify(notifier).notify(eq(MetricKind.HTTP_P99), eq(Severity.CRITICAL), any(), any(), any(), anyInt());
  }

  @Test
  @DisplayName("HTTP P99 정상 복귀 시 store.clear가 호출된다")
  void HTTP_P99_정상복귀시_store_clear_호출() {
    ViolationKey key = new ViolationKey(MetricKind.HTTP_P99, "argus", "localhost:8080", "/api/orders");
    evaluator.evaluateHttp(httpSnapshot(
        List.of(new EndpointMetricPointDto("/api/orders", BigDecimal.valueOf(0.5), OffsetDateTime.now())),
        List.of()));
    verify(store).clear(key);
  }

  @Test
  @DisplayName("HTTP 오류율이 critical 임계 위반 시 CRITICAL notify가 호출된다")
  void HTTP_ERROR_RATE_critical_위반시_notify_호출() {
    when(store.shouldSend(any(), eq(Severity.CRITICAL), anyInt())).thenReturn(true);
    evaluator.evaluateHttp(httpSnapshot(
        List.of(),
        List.of(new EndpointMetricPointDto("/api/pay", BigDecimal.valueOf(6.0), OffsetDateTime.now()))));
    verify(notifier).notify(eq(MetricKind.HTTP_ERROR_RATE), eq(Severity.CRITICAL), any(), any(), any(), anyInt());
  }

  @Test
  @DisplayName("HTTP 오류율이 warning 임계 위반 + window 충족 시 WARNING notify가 호출된다")
  void HTTP_ERROR_RATE_warning_위반_window_충족시_notify_호출() {
    when(store.shouldSend(any(), eq(Severity.WARNING), anyInt())).thenReturn(true);
    evaluator.evaluateHttp(httpSnapshot(
        List.of(),
        List.of(new EndpointMetricPointDto("/api/pay", BigDecimal.valueOf(2.0), OffsetDateTime.now()))));
    verify(notifier).notify(eq(MetricKind.HTTP_ERROR_RATE), eq(Severity.WARNING), any(), any(), any(), anyInt());
  }

  @Test
  @DisplayName("HTTP 오류율 정상 복귀 시 store.clear가 호출된다")
  void HTTP_ERROR_RATE_정상복귀시_store_clear_호출() {
    ViolationKey key = new ViolationKey(MetricKind.HTTP_ERROR_RATE, "argus", "localhost:8080", "/api/pay");
    evaluator.evaluateHttp(httpSnapshot(
        List.of(),
        List.of(new EndpointMetricPointDto("/api/pay", BigDecimal.valueOf(0.5), OffsetDateTime.now()))));
    verify(store).clear(key);
  }

  @Test
  @DisplayName("HTTP endpoint별로 독립적인 ViolationKey를 사용한다")
  void HTTP_endpoint별로_독립적인_키를_사용한다() {
    ViolationKey keyA = new ViolationKey(MetricKind.HTTP_P99, "argus", "localhost:8080", "/api/a");
    ViolationKey keyB = new ViolationKey(MetricKind.HTTP_P99, "argus", "localhost:8080", "/api/b");
    evaluator.evaluateHttp(httpSnapshot(
        List.of(
            new EndpointMetricPointDto("/api/a", BigDecimal.valueOf(0.5), OffsetDateTime.now()),
            new EndpointMetricPointDto("/api/b", BigDecimal.valueOf(0.5), OffsetDateTime.now())),
        List.of()));
    verify(store).clear(keyA);
    verify(store).clear(keyB);
  }

  // ───────────────── Hikari ─────────────────

  @Test
  @DisplayName("Hikari 활성 커넥션이 critical 임계 위반 시 CRITICAL notify가 호출된다")
  void HIKARI_ACTIVE_critical_위반시_notify_호출() {
    when(store.shouldSend(any(), eq(Severity.CRITICAL), anyInt())).thenReturn(true);
    evaluator.evaluateHikari(hikariSnapshot(
        List.of(new PoolMetricPointDto("HikariPool-1", BigDecimal.valueOf(10), OffsetDateTime.now())),
        List.of(), List.of()));
    verify(notifier).notify(eq(MetricKind.HIKARI_ACTIVE), eq(Severity.CRITICAL), any(), any(), any(), anyInt());
  }

  @Test
  @DisplayName("Hikari 활성 커넥션 정상 복귀 시 store.clear가 호출된다")
  void HIKARI_ACTIVE_정상복귀시_store_clear_호출() {
    ViolationKey key = new ViolationKey(MetricKind.HIKARI_ACTIVE, "argus", "localhost:8080", "HikariPool-1");
    evaluator.evaluateHikari(hikariSnapshot(
        List.of(new PoolMetricPointDto("HikariPool-1", BigDecimal.valueOf(3), OffsetDateTime.now())),
        List.of(), List.of()));
    verify(store).clear(key);
  }

  @Test
  @DisplayName("Hikari 대기 커넥션이 warning 임계 위반 시 WARNING notify가 호출된다")
  void HIKARI_PENDING_warning_위반시_notify_호출() {
    when(store.shouldSend(any(), eq(Severity.WARNING), anyInt())).thenReturn(true);
    evaluator.evaluateHikari(hikariSnapshot(
        List.of(), List.of(),
        List.of(new PoolMetricPointDto("HikariPool-1", BigDecimal.valueOf(2), OffsetDateTime.now()))));
    verify(notifier).notify(eq(MetricKind.HIKARI_PENDING), eq(Severity.WARNING), any(), any(), any(), anyInt());
  }

  @Test
  @DisplayName("Hikari 대기 커넥션 정상 복귀 시 store.clear가 호출된다")
  void HIKARI_PENDING_정상복귀시_store_clear_호출() {
    ViolationKey key = new ViolationKey(MetricKind.HIKARI_PENDING, "argus", "localhost:8080", "HikariPool-1");
    evaluator.evaluateHikari(hikariSnapshot(
        List.of(), List.of(),
        List.of(new PoolMetricPointDto("HikariPool-1", BigDecimal.valueOf(0), OffsetDateTime.now()))));
    verify(store).clear(key);
  }

  @Test
  @DisplayName("Hikari 사용률이 warning 임계 위반 시 WARNING notify가 호출된다")
  void HIKARI_USAGE_RATIO_warning_위반시_notify_호출() {
    when(store.shouldSend(any(), eq(Severity.WARNING), anyInt())).thenReturn(true);
    evaluator.evaluateHikari(hikariSnapshot(
        List.of(),
        List.of(new PoolMetricPointDto("HikariPool-1", BigDecimal.valueOf(85), OffsetDateTime.now())),
        List.of()));
    verify(notifier).notify(eq(MetricKind.HIKARI_USAGE_RATIO), eq(Severity.WARNING), any(), any(), any(), anyInt());
  }

  @Test
  @DisplayName("Hikari 사용률 정상 복귀 시 store.clear가 호출된다")
  void HIKARI_USAGE_RATIO_정상복귀시_store_clear_호출() {
    ViolationKey key = new ViolationKey(MetricKind.HIKARI_USAGE_RATIO, "argus", "localhost:8080", "HikariPool-1");
    evaluator.evaluateHikari(hikariSnapshot(
        List.of(),
        List.of(new PoolMetricPointDto("HikariPool-1", BigDecimal.valueOf(50), OffsetDateTime.now())),
        List.of()));
    verify(store).clear(key);
  }

  @Test
  @DisplayName("Hikari pool별로 독립적인 ViolationKey를 사용한다")
  void HIKARI_pool별로_독립적인_키를_사용한다() {
    ViolationKey poolA = new ViolationKey(MetricKind.HIKARI_ACTIVE, "argus", "localhost:8080", "pool-A");
    ViolationKey poolB = new ViolationKey(MetricKind.HIKARI_ACTIVE, "argus", "localhost:8080", "pool-B");
    evaluator.evaluateHikari(hikariSnapshot(
        List.of(
            new PoolMetricPointDto("pool-A", BigDecimal.valueOf(3), OffsetDateTime.now()),
            new PoolMetricPointDto("pool-B", BigDecimal.valueOf(3), OffsetDateTime.now())),
        List.of(), List.of()));
    verify(store).clear(poolA);
    verify(store).clear(poolB);
  }

  // ───────────────── ViolationStateStore 통합 ─────────────────

  @Nested
  @DisplayName("ViolationStateStore 통합")
  class IntegrationWithRealStore {

    @Test
    @DisplayName("(통합) window 횟수 연속 충족 시 notify가 정확히 1회 호출된다")
    void 통합_window_연속_충족시_notify_1회_호출() {
      ThresholdEvaluator realEval = new ThresholdEvaluator(properties, new ViolationStateStore(), notifier);
      // CPU warning=70, window=3
      realEval.evaluateJvm(jvmSnapshot(cpu(BigDecimal.valueOf(75), MetricStatus.SUCCESS)));
      realEval.evaluateJvm(jvmSnapshot(cpu(BigDecimal.valueOf(75), MetricStatus.SUCCESS)));
      verify(notifier, never()).notify(any(), any(), any(), any(), any(), anyInt());

      realEval.evaluateJvm(jvmSnapshot(cpu(BigDecimal.valueOf(75), MetricStatus.SUCCESS)));
      verify(notifier, times(1)).notify(
          eq(MetricKind.JVM_CPU), eq(Severity.WARNING), any(), any(), any(), anyInt());
    }

    @Test
    @DisplayName("(통합) severity 교차 시 연속 카운트가 리셋되어 window 미충족으로 notify 미호출")
    void 통합_severity_교차시_window_미충족으로_notify_미호출() {
      ThresholdEvaluator realEval = new ThresholdEvaluator(properties, new ViolationStateStore(), notifier);
      // CPU warning=70, critical=85, window=3
      realEval.evaluateJvm(jvmSnapshot(cpu(BigDecimal.valueOf(75), MetricStatus.SUCCESS))); // w=1
      realEval.evaluateJvm(jvmSnapshot(cpu(BigDecimal.valueOf(90), MetricStatus.SUCCESS))); // c=1, w=0
      realEval.evaluateJvm(jvmSnapshot(cpu(BigDecimal.valueOf(75), MetricStatus.SUCCESS))); // w=1, c=0
      realEval.evaluateJvm(jvmSnapshot(cpu(BigDecimal.valueOf(90), MetricStatus.SUCCESS))); // c=1, w=0
      realEval.evaluateJvm(jvmSnapshot(cpu(BigDecimal.valueOf(75), MetricStatus.SUCCESS))); // w=1, c=0
      verify(notifier, never()).notify(
          eq(MetricKind.JVM_CPU), eq(Severity.WARNING), any(), any(), any(), anyInt());
    }
  }

  // ───────────────── 헬퍼 메서드 ─────────────────

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

  private CpuUsageDto cpu(BigDecimal value, MetricStatus status) {
    return new CpuUsageDto(value, OffsetDateTime.now(), status, null);
  }

  private JvmMetricSnapshotDto jvmSnapshot(CpuUsageDto cpu) {
    return jvmSnapshot(cpu, null, null, null);
  }

  private JvmMetricSnapshotDto jvmSnapshot(CpuUsageDto cpu, MemoryUsageDto memory,
      GcMetricDto gc, ThreadMetricDto thread) {
    CpuUsageDto effectiveCpu = cpu != null ? cpu
        : new CpuUsageDto(BigDecimal.valueOf(50), OffsetDateTime.now(), MetricStatus.SUCCESS, null);
    return new JvmMetricSnapshotDto("argus", "localhost:8080", OffsetDateTime.now(),
        effectiveCpu, memory, gc, thread, SnapshotStatus.COMPLETE);
  }

  private HttpMetricSnapshotDto httpSnapshot(List<EndpointMetricPointDto> p99Points,
      List<EndpointMetricPointDto> errorPoints) {
    return new HttpMetricSnapshotDto("argus", "localhost:8080", OffsetDateTime.now(),
        new HttpResponseTimeDto(p99Points, OffsetDateTime.now(), MetricStatus.SUCCESS, null),
        new HttpThroughputDto(List.of(), OffsetDateTime.now(), MetricStatus.SUCCESS, null),
        new HttpErrorRateDto(errorPoints, OffsetDateTime.now(), MetricStatus.SUCCESS, null),
        SnapshotStatus.COMPLETE);
  }

  private HikariMetricSnapshotDto hikariSnapshot(List<PoolMetricPointDto> activePoints,
      List<PoolMetricPointDto> usageRatioPoints, List<PoolMetricPointDto> pendingPoints) {
    return new HikariMetricSnapshotDto("argus", "localhost:8080", OffsetDateTime.now(),
        new HikariActiveDto(activePoints, usageRatioPoints, OffsetDateTime.now(), MetricStatus.SUCCESS, null),
        new HikariPendingDto(pendingPoints, OffsetDateTime.now(), MetricStatus.SUCCESS, null),
        SnapshotStatus.COMPLETE);
  }
}
