package com.example.pythia.kafka.consumer;

import com.example.pythia.kafka.config.KafkaDedupProperties;
import java.time.OffsetDateTime;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

/**
 * Kafka 메시지 중복 처리 방지 컴포넌트.
 *
 * <p>Redis SETNX(setIfAbsent)를 사용하여 동일 메시지의 중복 처리를 방지한다.
 * Redis 장애 시 fail-open 정책을 적용하여 처리를 계속 진행한다 (무손실 우선).
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class MessageDeduplicator {

  private final StringRedisTemplate redisTemplate;
  private final KafkaDedupProperties properties;

  /**
   * 메시지를 처리 완료로 표시한다.
   *
   * @param topic       Kafka topic 이름
   * @param application 애플리케이션 이름
   * @param instance    인스턴스 이름
   * @param collectedAt 수집 시각
   * @return true if first time (처리 진행), false if duplicate (스킵 대상)
   */
  public boolean markProcessed(String topic, String application, String instance,
      OffsetDateTime collectedAt) {
    String key = "pythia:kafka:processed:%s:%s:%s:%d"
        .formatted(sanitize(topic), sanitize(application), sanitize(instance),
            collectedAt.toInstant().toEpochMilli());
    try {
      Boolean firstTime = redisTemplate.opsForValue()
          .setIfAbsent(key, "1", properties.getTtl());
      return Boolean.TRUE.equals(firstTime);
    } catch (DataAccessException ex) {
      // fail-open: Redis 장애 시 처리 계속 진행
      log.warn("Dedup Redis failure, fail-open: key={}, err={}", key, ex.getMessage());
      return true;
    }
  }

  private static String sanitize(String value) {
    if (value == null) {
      return "-";
    }
    return value.replace(":", "_");
  }
}
