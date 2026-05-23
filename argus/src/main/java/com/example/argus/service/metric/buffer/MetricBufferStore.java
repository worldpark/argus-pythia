package com.example.argus.service.metric.buffer;

import com.example.argus.config.MetricBufferProperties;
import com.example.argus.dto.metric.buffer.MetricBufferType;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class MetricBufferStore {

    private final StringRedisTemplate redisTemplate;
    private final MetricBufferProperties properties;

    public void enqueue(MetricBufferType type, String envelopeJson, double score) {
        String key = buildKey(type);
        ZSetOperations<String, String> zSetOps = redisTemplate.opsForZSet();
        zSetOps.add(key, envelopeJson, score);
        refreshExpire(key);
    }

    public void dropOldest(MetricBufferType type, long countToDrop) {
        String key = buildKey(type);
        redisTemplate.opsForZSet().removeRange(key, 0, countToDrop - 1);
    }

    public void dropNewest(MetricBufferType type, String envelopeJson) {
        String key = buildKey(type);
        redisTemplate.opsForZSet().remove(key, envelopeJson);
    }

    public long size(MetricBufferType type) {
        String key = buildKey(type);
        Long count = redisTemplate.opsForZSet().zCard(key);
        return count != null ? count : 0L;
    }

    public List<String> peekOldest(MetricBufferType type, int batchSize) {
        String key = buildKey(type);
        Set<String> members = redisTemplate.opsForZSet().range(key, 0, batchSize - 1);
        if (members == null || members.isEmpty()) {
            return Collections.emptyList();
        }
        return List.copyOf(members);
    }

    public void remove(MetricBufferType type, String envelopeJson) {
        String key = buildKey(type);
        redisTemplate.opsForZSet().remove(key, envelopeJson);
    }

    public long evictExpired(MetricBufferType type, double maxScoreExclusive) {
        String key = buildKey(type);
        Long removed = redisTemplate.opsForZSet()
                .removeRangeByScore(key, 0, maxScoreExclusive);
        long count = removed != null ? removed : 0L;
        if (count > 0) {
            log.info("metric-buffer: evicted={} expired entries for type={}", count, type);
        }
        return count;
    }

    public String buildKey(MetricBufferType type) {
        return properties.getKeyPrefix() + type.getKeySuffix();
    }

    private void refreshExpire(String key) {
        long ttlSeconds = properties.getTtl().toSeconds() * 2;
        redisTemplate.expire(key, ttlSeconds, TimeUnit.SECONDS);
    }
}
