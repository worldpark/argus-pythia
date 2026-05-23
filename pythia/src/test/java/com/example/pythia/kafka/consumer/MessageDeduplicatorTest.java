package com.example.pythia.kafka.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import com.example.pythia.kafka.config.KafkaDedupProperties;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.OffsetDateTime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

@ExtendWith(MockitoExtension.class)
class MessageDeduplicatorTest {

  @Mock
  private StringRedisTemplate redisTemplate;

  @Mock
  private ValueOperations<String, String> valueOps;

  private KafkaDedupProperties properties;
  private MessageDeduplicator deduplicator;

  @BeforeEach
  void setUp() {
    properties = new KafkaDedupProperties();
    properties.setTtl(Duration.ofHours(24));
    deduplicator = new MessageDeduplicator(redisTemplate, properties);
    when(redisTemplate.opsForValue()).thenReturn(valueOps);
  }

  private OffsetDateTime fixedTime() {
    return OffsetDateTime.parse("2026-05-23T10:00:00+09:00");
  }

  @Test
  @DisplayName("setIfAbsent가 true 반환 시 처음 처리 -> markProcessed true 반환")
  void setIfAbsent_true_반환시_처음처리() {
    when(valueOps.setIfAbsent(anyString(), eq("1"), any(Duration.class))).thenReturn(true);

    boolean result = deduplicator.markProcessed("jvm.metrics.raw", "argus", "localhost:8080", fixedTime());

    assertThat(result).isTrue();
  }

  @Test
  @DisplayName("setIfAbsent가 false 반환 시 중복 -> markProcessed false 반환")
  void setIfAbsent_false_반환시_중복() {
    when(valueOps.setIfAbsent(anyString(), eq("1"), any(Duration.class))).thenReturn(false);

    boolean result = deduplicator.markProcessed("jvm.metrics.raw", "argus", "localhost:8080", fixedTime());

    assertThat(result).isFalse();
  }

  @Test
  @DisplayName("setIfAbsent가 null 반환 시 false (중복 처리)")
  void setIfAbsent_null_반환시_false() {
    when(valueOps.setIfAbsent(anyString(), eq("1"), any(Duration.class))).thenReturn(null);

    boolean result = deduplicator.markProcessed("jvm.metrics.raw", "argus", "localhost:8080", fixedTime());

    assertThat(result).isFalse();
  }

  @Test
  @DisplayName("Redis DataAccessException 발생 시 fail-open: true 반환")
  void DataAccessException_발생시_fail_open_true_반환() {
    when(valueOps.setIfAbsent(anyString(), eq("1"), any(Duration.class)))
        .thenThrow(new QueryTimeoutException("Redis timeout"));

    boolean result = deduplicator.markProcessed("jvm.metrics.raw", "argus", "localhost:8080", fixedTime());

    assertThat(result).isTrue();
  }

  @Test
  @DisplayName("Redis key에 topic, application, sanitize된 instance, epoch ms가 포함된다")
  void redis_key_구성_검증() {
    ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
    when(valueOps.setIfAbsent(keyCaptor.capture(), eq("1"), any(Duration.class))).thenReturn(true);

    OffsetDateTime time = fixedTime();
    deduplicator.markProcessed("jvm.metrics.raw", "argus", "localhost:8080", time);

    String capturedKey = keyCaptor.getValue();
    assertThat(capturedKey).startsWith("pythia:kafka:processed:");
    assertThat(capturedKey).contains("jvm.metrics.raw");
    assertThat(capturedKey).contains("argus");
    assertThat(capturedKey).contains("localhost_8080");
    assertThat(capturedKey).doesNotContain("localhost:8080");
    assertThat(capturedKey).contains(String.valueOf(time.toInstant().toEpochMilli()));
  }

  @Test
  @DisplayName("동일 파라미터 두 번째 호출 시 setIfAbsent false -> 중복 감지")
  void 두번째_호출시_중복_감지() {
    when(valueOps.setIfAbsent(anyString(), eq("1"), any(Duration.class)))
        .thenReturn(true)
        .thenReturn(false);

    OffsetDateTime time = fixedTime();
    boolean first = deduplicator.markProcessed("jvm.metrics.raw", "argus", "localhost:8080", time);
    boolean second = deduplicator.markProcessed("jvm.metrics.raw", "argus", "localhost:8080", time);

    assertThat(first).isTrue();
    assertThat(second).isFalse();
  }

  @Test
  @DisplayName("application에 콜론 포함 시 언더스코어로 치환된다")
  void application_콜론_sanitize_검증() {
    ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
    when(valueOps.setIfAbsent(keyCaptor.capture(), eq("1"), any(Duration.class))).thenReturn(true);

    deduplicator.markProcessed("jvm.metrics.raw", "my:app", "host", fixedTime());

    String capturedKey = keyCaptor.getValue();
    assertThat(capturedKey).contains("my_app");
    assertThat(capturedKey).doesNotContain("my:app");
  }

  @Test
  @DisplayName("application 또는 instance가 null이면 '-'로 치환된다")
  void null_값_sanitize_검증() {
    ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
    when(valueOps.setIfAbsent(keyCaptor.capture(), eq("1"), any(Duration.class))).thenReturn(true);

    deduplicator.markProcessed("jvm.metrics.raw", null, null, fixedTime());

    String capturedKey = keyCaptor.getValue();
    // null application과 null instance 모두 '-'로 치환
    long dashCount = capturedKey.chars().filter(c -> c == '-').count();
    assertThat(dashCount).isGreaterThanOrEqualTo(2);
  }
}
