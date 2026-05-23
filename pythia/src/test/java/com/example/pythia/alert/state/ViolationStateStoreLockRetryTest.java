package com.example.pythia.alert.state;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.example.pythia.alert.config.ViolationStateProperties;
import com.example.pythia.alert.domain.MetricKind;
import com.example.pythia.alert.domain.Severity;
import com.example.pythia.alert.domain.ViolationKey;
import com.example.pythia.alert.exception.LockAcquisitionRetryException;
import com.example.pythia.alert.exception.ViolationStateErrorCode;
import com.example.pythia.alert.exception.ViolationStateException;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * ViolationStateStore Lock 획득 Resilience4j Retry 동작 검증.
 */
@SuppressWarnings("unchecked")
@ExtendWith(MockitoExtension.class)
class ViolationStateStoreLockRetryTest {

  @Mock
  private RedissonClient redissonClient;

  @Mock
  private RLock rLock;

  @Mock
  private StringRedisTemplate redisTemplate;

  private HashOperations<String, Object, Object> hashOperations;
  private ViolationStateStore store;
  private ViolationKey key;

  /**
   * RetryRegistry 생성 헬퍼: violationLock 인스턴스 설정과 동일한 config.
   */
  private RetryRegistry buildRetryRegistry(int maxAttempts) {
    RetryConfig config = RetryConfig.custom()
        .maxAttempts(maxAttempts)
        .waitDuration(Duration.ofMillis(5)) // 테스트에서는 짧게
        .retryExceptions(LockAcquisitionRetryException.class)
        .ignoreExceptions(ViolationStateException.class, InterruptedException.class)
        .build();
    return RetryRegistry.of(config);
  }

  @BeforeEach
  void setUp() {
    ViolationStateProperties properties = new ViolationStateProperties();
    RetryRegistry retryRegistry = buildRetryRegistry(3);
    store = new ViolationStateStore(redisTemplate, redissonClient, properties, retryRegistry);
    key = new ViolationKey(MetricKind.JVM_CPU, "argus", "localhost:8080", null);

    hashOperations = mock(HashOperations.class);
    lenient().when(redissonClient.getLock(anyString())).thenReturn(rLock);
    lenient().when(redisTemplate.opsForHash()).thenReturn(hashOperations);
  }

  @Test
  @DisplayName("tryLock 2회 false 후 3회째 true -> 정상 처리, tryLock 총 3회 호출")
  void tryLock_2회_false_3회째_true_정상처리() throws InterruptedException {
    when(rLock.tryLock(anyLong(), anyLong(), eq(TimeUnit.MILLISECONDS)))
        .thenReturn(false)
        .thenReturn(false)
        .thenReturn(true);
    when(rLock.isHeldByCurrentThread()).thenReturn(true);
    when(hashOperations.multiGet(anyString(), anyList()))
        .thenReturn(Arrays.asList(null, null, null));

    boolean result = store.shouldSend(key, Severity.WARNING, 1);

    assertThat(result).isTrue();
    verify(rLock, times(3)).tryLock(anyLong(), anyLong(), eq(TimeUnit.MILLISECONDS));
  }

  @Test
  @DisplayName("tryLock 항상 false (max-attempts=3) -> ViolationStateException(LOCK_ACQUISITION_FAILED), tryLock 3회 호출")
  void tryLock_항상_false_LOCK_ACQUISITION_FAILED() throws InterruptedException {
    when(rLock.tryLock(anyLong(), anyLong(), eq(TimeUnit.MILLISECONDS))).thenReturn(false);

    assertThatThrownBy(() -> store.shouldSend(key, Severity.WARNING, 1))
        .isInstanceOf(ViolationStateException.class)
        .satisfies(ex -> {
          ViolationStateException vse = (ViolationStateException) ex;
          assertThat(vse.getErrorCode()).isEqualTo(ViolationStateErrorCode.LOCK_ACQUISITION_FAILED);
        });

    verify(rLock, times(3)).tryLock(anyLong(), anyLong(), eq(TimeUnit.MILLISECONDS));
  }

  @Test
  @DisplayName("첫 tryLock에서 InterruptedException -> LOCK_INTERRUPTED, interrupt flag 복원, 재시도 없음(1회)")
  void tryLock_InterruptedException_LOCK_INTERRUPTED_재시도_없음() throws InterruptedException {
    when(rLock.tryLock(anyLong(), anyLong(), eq(TimeUnit.MILLISECONDS)))
        .thenThrow(new InterruptedException("interrupted"));

    assertThatThrownBy(() -> store.shouldSend(key, Severity.WARNING, 1))
        .isInstanceOf(ViolationStateException.class)
        .satisfies(ex -> {
          ViolationStateException vse = (ViolationStateException) ex;
          assertThat(vse.getErrorCode()).isEqualTo(ViolationStateErrorCode.LOCK_INTERRUPTED);
        });

    // InterruptedException은 ignore-exceptions -> retry 미수행, 1회만 호출
    verify(rLock, times(1)).tryLock(anyLong(), anyLong(), eq(TimeUnit.MILLISECONDS));
    // interrupt flag 복원 확인
    assertThat(Thread.currentThread().isInterrupted()).isTrue();
    // 다른 테스트에 영향 없도록 flag 정리
    Thread.interrupted();
  }

  @Test
  @DisplayName("tryLock 1회 false 후 2회째 true -> 정상 처리, tryLock 총 2회 호출")
  void tryLock_1회_false_2회째_true() throws InterruptedException {
    when(rLock.tryLock(anyLong(), anyLong(), eq(TimeUnit.MILLISECONDS)))
        .thenReturn(false)
        .thenReturn(true);
    when(rLock.isHeldByCurrentThread()).thenReturn(true);
    when(hashOperations.multiGet(anyString(), anyList()))
        .thenReturn(Arrays.asList(null, null, null));

    store.shouldSend(key, Severity.WARNING, 1);

    verify(rLock, times(2)).tryLock(anyLong(), anyLong(), eq(TimeUnit.MILLISECONDS));
  }
}
