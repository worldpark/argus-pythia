package com.example.pythia.alert.state;

import com.example.pythia.alert.config.ViolationStateProperties;
import com.example.pythia.alert.domain.Severity;
import com.example.pythia.alert.domain.ViolationKey;
import com.example.pythia.alert.exception.ViolationStateErrorCode;
import com.example.pythia.alert.exception.ViolationStateException;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.redisson.client.RedisException;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.serializer.SerializationException;
import org.springframework.stereotype.Component;

@Component
public class ViolationStateStore {

  private final StringRedisTemplate redisTemplate;
  private final RedissonClient redissonClient;
  private final ViolationStateProperties properties;

  public ViolationStateStore(
      StringRedisTemplate redisTemplate,
      RedissonClient redissonClient,
      ViolationStateProperties properties) {
    this.redisTemplate = redisTemplate;
    this.redissonClient = redissonClient;
    this.properties = properties;
  }

  public boolean shouldSend(ViolationKey key, Severity severity, int window) {
    String redisKey = toRedisKey(key);
    String lockKey = toLockKey(key);
    long waitMs = properties.getLockWait().toMillis();
    long leaseMs = properties.getLockLease().toMillis();
    Duration ttl = properties.getTtl();

    RLock lock = redissonClient.getLock(lockKey);
    boolean acquired;
    try {
      acquired = lock.tryLock(waitMs, leaseMs, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ViolationStateException(ViolationStateErrorCode.LOCK_INTERRUPTED, e);
    }
    if (!acquired) {
      throw new ViolationStateException(ViolationStateErrorCode.LOCK_ACQUISITION_FAILED);
    }

    try {
      HashOperations<String, String, String> hash = redisTemplate.opsForHash();

      // 1) 현재 상태 조회 (HMGET)
      // Redis multiGet은 요청 필드 순서대로 null 포함 리스트 반환. 방어적으로 size 확인.
      List<String> current = hash.multiGet(redisKey,
          List.of("warningCount", "criticalCount", "lastSent"));
      int warnCount = (current != null && current.size() > 0) ? parseIntOrZero(current.get(0)) : 0;
      int critCount = (current != null && current.size() > 1) ? parseIntOrZero(current.get(1)) : 0;
      String lastSent = (current != null && current.size() > 2) ? current.get(2) : null;

      // 2) severity +1, 반대 severity 0 reset
      if (severity == Severity.WARNING) {
        warnCount += 1;
        critCount = 0;
      } else {
        critCount += 1;
        warnCount = 0;
      }

      // 3) 발송 판정
      int activeCount = (severity == Severity.WARNING) ? warnCount : critCount;
      boolean shouldSend = false;
      if (activeCount >= window && !severity.name().equals(lastSent)) {
        lastSent = severity.name();
        shouldSend = true;
      }

      // 4) 저장 (HSET multi field)
      Map<String, String> updates = new LinkedHashMap<>();
      updates.put("warningCount", Integer.toString(warnCount));
      updates.put("criticalCount", Integer.toString(critCount));
      updates.put("lastSent", lastSent == null ? "" : lastSent);
      hash.putAll(redisKey, updates);

      // 5) slide TTL
      redisTemplate.expire(redisKey, ttl);

      return shouldSend;
    } catch (DataAccessException | SerializationException e) {
      throw new ViolationStateException(ViolationStateErrorCode.REDIS_ACCESS_FAILED, e);
    } catch (RedisException e) {
      throw new ViolationStateException(ViolationStateErrorCode.REDIS_ACCESS_FAILED, e);
    } finally {
      if (lock.isHeldByCurrentThread()) {
        try {
          lock.unlock();
        } catch (IllegalMonitorStateException ignored) {
          // 이미 만료된 락은 무시
        }
      }
    }
  }

  public void clear(ViolationKey key) {
    String redisKey = toRedisKey(key);
    try {
      redisTemplate.delete(redisKey);
    } catch (DataAccessException e) {
      throw new ViolationStateException(ViolationStateErrorCode.REDIS_ACCESS_FAILED, e);
    }
  }

  private String toRedisKey(ViolationKey key) {
    return properties.getKeyPrefix()
        + sanitize(key.kind().name()) + ":"
        + sanitize(key.application()) + ":"
        + sanitize(key.instance()) + ":"
        + sanitize(key.sub());
  }

  private String toLockKey(ViolationKey key) {
    return properties.getLockKeyPrefix()
        + sanitize(key.kind().name()) + ":"
        + sanitize(key.application()) + ":"
        + sanitize(key.instance()) + ":"
        + sanitize(key.sub());
  }

  private static String sanitize(String value) {
    if (value == null) {
      return "-";
    }
    return value.replace(":", "_");
  }

  private static int parseIntOrZero(String s) {
    if (s == null || s.isBlank()) {
      return 0;
    }
    try {
      return Integer.parseInt(s);
    } catch (NumberFormatException e) {
      return 0;
    }
  }
}
