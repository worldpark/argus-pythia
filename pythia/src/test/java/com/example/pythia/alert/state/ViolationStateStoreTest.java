package com.example.pythia.alert.state;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.example.pythia.alert.config.ViolationStateProperties;
import com.example.pythia.alert.domain.MetricKind;
import com.example.pythia.alert.domain.Severity;
import com.example.pythia.alert.domain.ViolationKey;
import com.example.pythia.alert.exception.ViolationStateErrorCode;
import com.example.pythia.alert.exception.ViolationStateException;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.StringRedisTemplate;

@SuppressWarnings("unchecked")
@ExtendWith(MockitoExtension.class)
class ViolationStateStoreTest {

  @Mock
  private RedissonClient redissonClient;

  @Mock
  private RLock rLock;

  @Mock
  private StringRedisTemplate redisTemplate;

  // StringRedisTemplate.opsForHash() 반환 타입은 HashOperations<String, Object, Object>
  private HashOperations<String, Object, Object> hashOperations;

  @Captor
  private ArgumentCaptor<String> keyCaptor;

  private ViolationStateProperties properties;
  private ViolationStateStore store;
  private ViolationKey key;

  @BeforeEach
  void setUp() {
    properties = new ViolationStateProperties();
    // RetryRegistry: 테스트에서 max-attempts=1로 설정해 즉시 실패하도록 구성 (기존 테스트 유지)
    RetryRegistry retryRegistry = RetryRegistry.of(
        RetryConfig.custom()
            .maxAttempts(1) // 재시도 없이 즉시 LockAcquisitionRetryException 전파
            .build());
    store = new ViolationStateStore(redisTemplate, redissonClient, properties, retryRegistry);
    key = new ViolationKey(MetricKind.JVM_CPU, "argus", "localhost:8080", null);

    hashOperations = mock(HashOperations.class);
    // lenient: lock 관련 테스트에서는 opsForHash 가 호출되지 않을 수 있으므로 lenient 처리
    lenient().when(redissonClient.getLock(anyString())).thenReturn(rLock);
    lenient().when(redisTemplate.opsForHash()).thenReturn(hashOperations);
  }

  /**
   * multiGet이 null 포함 3개 항목을 반환하는 헬퍼 (Redis 실제 동작과 동일)
   */
  private List<Object> emptyHashResult() {
    return Arrays.asList(null, null, null);
  }

  @Test
  @DisplayName("lock 성공 + Hash 빈 상태 + severity=WARNING + window=1 -> true 반환")
  void lock_성공_빈상태_WARNING_window1_true반환() throws InterruptedException {
    when(rLock.tryLock(anyLong(), anyLong(), eq(TimeUnit.MILLISECONDS))).thenReturn(true);
    when(rLock.isHeldByCurrentThread()).thenReturn(true);
    // 실제 Redis multiGet: 필드 없으면 null을 항목 수만큼 반환
    when(hashOperations.multiGet(anyString(), anyList()))
        .thenReturn(emptyHashResult());

    boolean result = store.shouldSend(key, Severity.WARNING, 1);

    assertThat(result).isTrue();
    ArgumentCaptor<Map> mapCaptor = ArgumentCaptor.forClass(Map.class);
    verify(hashOperations).putAll(anyString(), mapCaptor.capture());
    Map<String, String> updates = mapCaptor.getValue();
    assertThat(updates.get("warningCount")).isEqualTo("1");
    assertThat(updates.get("criticalCount")).isEqualTo("0");
    assertThat(updates.get("lastSent")).isEqualTo("WARNING");
    verify(redisTemplate).expire(anyString(), any(Duration.class));
    verify(rLock).unlock();
  }

  @Test
  @DisplayName("lastSent=WARNING 이미 존재 + 동일 severity 재호출 -> false 반환, warningCount +1")
  void lastSent_WARNING_동일severity_재호출_false반환() throws InterruptedException {
    when(rLock.tryLock(anyLong(), anyLong(), eq(TimeUnit.MILLISECONDS))).thenReturn(true);
    when(rLock.isHeldByCurrentThread()).thenReturn(true);
    when(hashOperations.multiGet(anyString(), anyList()))
        .thenReturn(Arrays.asList("1", "0", "WARNING"));

    boolean result = store.shouldSend(key, Severity.WARNING, 1);

    assertThat(result).isFalse();
    ArgumentCaptor<Map> mapCaptor = ArgumentCaptor.forClass(Map.class);
    verify(hashOperations).putAll(anyString(), mapCaptor.capture());
    Map<String, String> updates = mapCaptor.getValue();
    assertThat(updates.get("warningCount")).isEqualTo("2");
    assertThat(updates.get("lastSent")).isEqualTo("WARNING");
  }

  @Test
  @DisplayName("severity 교차 (WARNING 누적 후 CRITICAL 입력) -> warningCount=0 reset, criticalCount=1")
  void severity_교차_WARNING후_CRITICAL_warningCount_reset() throws InterruptedException {
    when(rLock.tryLock(anyLong(), anyLong(), eq(TimeUnit.MILLISECONDS))).thenReturn(true);
    when(rLock.isHeldByCurrentThread()).thenReturn(true);
    // 기존 warningCount=2, criticalCount=0, lastSent 없음("")
    when(hashOperations.multiGet(anyString(), anyList()))
        .thenReturn(Arrays.asList("2", "0", ""));

    // window=1 이므로 critCount=1 >= 1 AND lastSent("") != CRITICAL -> true
    boolean result = store.shouldSend(key, Severity.CRITICAL, 1);

    assertThat(result).isTrue();
    ArgumentCaptor<Map> mapCaptor = ArgumentCaptor.forClass(Map.class);
    verify(hashOperations).putAll(anyString(), mapCaptor.capture());
    Map<String, String> updates = mapCaptor.getValue();
    assertThat(updates.get("warningCount")).isEqualTo("0");
    assertThat(updates.get("criticalCount")).isEqualTo("1");
    assertThat(updates.get("lastSent")).isEqualTo("CRITICAL");
  }

  @Test
  @DisplayName("window 미달 (count < window) -> false 반환, putAll/expire 정상 호출, lastSent 변경 없음")
  void window_미달_false반환_putAll_expire_호출() throws InterruptedException {
    when(rLock.tryLock(anyLong(), anyLong(), eq(TimeUnit.MILLISECONDS))).thenReturn(true);
    when(rLock.isHeldByCurrentThread()).thenReturn(true);
    when(hashOperations.multiGet(anyString(), anyList()))
        .thenReturn(Arrays.asList("0", "0", ""));

    boolean result = store.shouldSend(key, Severity.WARNING, 3);

    assertThat(result).isFalse();
    verify(hashOperations).putAll(anyString(), any(Map.class));
    verify(redisTemplate).expire(anyString(), any(Duration.class));
    ArgumentCaptor<Map> mapCaptor = ArgumentCaptor.forClass(Map.class);
    verify(hashOperations).putAll(anyString(), mapCaptor.capture());
    // warningCount=1 이지만 window=3 미달 -> lastSent 빈 문자열 유지
    assertThat(mapCaptor.getValue().get("lastSent")).isEqualTo("");
  }

  @Test
  @DisplayName("lock 획득 실패 (tryLock false 반환) -> ViolationStateException(LOCK_ACQUISITION_FAILED), hash 접근 없음")
  void lock_획득실패_LOCK_ACQUISITION_FAILED_예외() throws InterruptedException {
    when(rLock.tryLock(anyLong(), anyLong(), eq(TimeUnit.MILLISECONDS))).thenReturn(false);

    assertThatThrownBy(() -> store.shouldSend(key, Severity.WARNING, 1))
        .isInstanceOf(ViolationStateException.class)
        .satisfies(ex -> {
          ViolationStateException vse = (ViolationStateException) ex;
          assertThat(vse.getErrorCode()).isEqualTo(ViolationStateErrorCode.LOCK_ACQUISITION_FAILED);
        });
    verify(hashOperations, never()).multiGet(anyString(), anyList());
  }

  @Test
  @DisplayName("tryLock이 InterruptedException 던짐 -> ViolationStateException(LOCK_INTERRUPTED), interrupt flag 복원")
  void tryLock_InterruptedException_LOCK_INTERRUPTED_interrupt_flag_복원() throws InterruptedException {
    when(rLock.tryLock(anyLong(), anyLong(), eq(TimeUnit.MILLISECONDS)))
        .thenThrow(new InterruptedException("interrupted"));

    assertThatThrownBy(() -> store.shouldSend(key, Severity.WARNING, 1))
        .isInstanceOf(ViolationStateException.class)
        .satisfies(ex -> {
          ViolationStateException vse = (ViolationStateException) ex;
          assertThat(vse.getErrorCode()).isEqualTo(ViolationStateErrorCode.LOCK_INTERRUPTED);
        });
    assertThat(Thread.currentThread().isInterrupted()).isTrue();
    // interrupt flag 정리 (다른 테스트에 영향 없도록)
    Thread.interrupted();
  }

  @Test
  @DisplayName("hashOperations.multiGet 호출 시 DataAccessException -> ViolationStateException(REDIS_ACCESS_FAILED), finally unlock 호출")
  void multiGet_DataAccessException_REDIS_ACCESS_FAILED_unlock_호출() throws InterruptedException {
    when(rLock.tryLock(anyLong(), anyLong(), eq(TimeUnit.MILLISECONDS))).thenReturn(true);
    when(rLock.isHeldByCurrentThread()).thenReturn(true);
    when(hashOperations.multiGet(anyString(), anyList()))
        .thenThrow(new QueryTimeoutException("timeout"));

    assertThatThrownBy(() -> store.shouldSend(key, Severity.WARNING, 1))
        .isInstanceOf(ViolationStateException.class)
        .satisfies(ex -> {
          ViolationStateException vse = (ViolationStateException) ex;
          assertThat(vse.getErrorCode()).isEqualTo(ViolationStateErrorCode.REDIS_ACCESS_FAILED);
        });
    verify(rLock).unlock();
  }

  @Test
  @DisplayName("finally unlock 동작: isHeldByCurrentThread=true -> unlock 1회, false -> unlock 호출 안 함")
  void finally_unlock_동작_isHeldByCurrentThread_분기() throws InterruptedException {
    // (a) isHeldByCurrentThread=true -> unlock 1회
    when(rLock.tryLock(anyLong(), anyLong(), eq(TimeUnit.MILLISECONDS))).thenReturn(true);
    when(rLock.isHeldByCurrentThread()).thenReturn(true);
    when(hashOperations.multiGet(anyString(), anyList()))
        .thenReturn(emptyHashResult());

    store.shouldSend(key, Severity.WARNING, 1);
    verify(rLock, times(1)).unlock();

    // (b) isHeldByCurrentThread=false -> unlock 호출 안 함 (새 lock mock 사용)
    RLock rLock2 = mock(RLock.class);
    when(redissonClient.getLock(anyString())).thenReturn(rLock2);
    when(rLock2.tryLock(anyLong(), anyLong(), eq(TimeUnit.MILLISECONDS))).thenReturn(true);
    when(rLock2.isHeldByCurrentThread()).thenReturn(false);
    when(hashOperations.multiGet(anyString(), anyList()))
        .thenReturn(emptyHashResult());

    store.shouldSend(key, Severity.WARNING, 1);
    verify(rLock2, never()).unlock();
  }

  @Test
  @DisplayName("clear -> redisTemplate.delete(올바른 key) 호출")
  void clear_올바른키로_delete_호출() {
    store.clear(key);
    verify(redisTemplate).delete(keyCaptor.capture());
    String deletedKey = keyCaptor.getValue();
    // JVM_CPU, argus, localhost:8080(->localhost_8080), null(->-)
    assertThat(deletedKey).isEqualTo("pythia:alert:violation:JVM_CPU:argus:localhost_8080:-");
  }

  @Test
  @DisplayName("clear - DataAccessException 발생 시 ViolationStateException(REDIS_ACCESS_FAILED) 변환")
  void clear_DataAccessException_REDIS_ACCESS_FAILED() {
    doThrow(new QueryTimeoutException("timeout")).when(redisTemplate).delete(anyString());

    assertThatThrownBy(() -> store.clear(key))
        .isInstanceOf(ViolationStateException.class)
        .satisfies(ex -> {
          ViolationStateException vse = (ViolationStateException) ex;
          assertThat(vse.getErrorCode()).isEqualTo(ViolationStateErrorCode.REDIS_ACCESS_FAILED);
        });
  }

  @Test
  @DisplayName("Key sanitization: sub=null -> -, sub='ns:foo' -> ns_foo")
  void key_sanitization_sub_null과_콜론포함() throws InterruptedException {
    // sub=null -> 락 키가 :- 로 끝나야 함
    ViolationKey keyNullSub = new ViolationKey(MetricKind.JVM_CPU, "argus", "inst", null);
    when(rLock.tryLock(anyLong(), anyLong(), eq(TimeUnit.MILLISECONDS))).thenReturn(true);
    when(rLock.isHeldByCurrentThread()).thenReturn(true);
    when(hashOperations.multiGet(anyString(), anyList()))
        .thenReturn(emptyHashResult());

    store.shouldSend(keyNullSub, Severity.WARNING, 1);

    ArgumentCaptor<String> lockKeyCaptor = ArgumentCaptor.forClass(String.class);
    verify(redissonClient).getLock(lockKeyCaptor.capture());
    assertThat(lockKeyCaptor.getValue()).endsWith(":-");

    // sub="ns:foo" -> clear 시 키에 ns_foo 포함 확인
    ViolationKey keyColonSub = new ViolationKey(MetricKind.JVM_CPU, "argus", "inst", "ns:foo");
    store.clear(keyColonSub);

    ArgumentCaptor<String> deleteKeyCaptor = ArgumentCaptor.forClass(String.class);
    verify(redisTemplate).delete(deleteKeyCaptor.capture());
    assertThat(deleteKeyCaptor.getValue()).contains("ns_foo");
  }
}
