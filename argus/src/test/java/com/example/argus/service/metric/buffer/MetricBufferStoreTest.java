package com.example.argus.service.metric.buffer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.example.argus.config.MetricBufferProperties;
import com.example.argus.dto.metric.buffer.MetricBufferType;
import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;

@ExtendWith(MockitoExtension.class)
class MetricBufferStoreTest {

    @Mock private StringRedisTemplate redisTemplate;
    @Mock private ZSetOperations<String, String> zSetOps;
    @Mock private MetricBufferProperties properties;

    @InjectMocks private MetricBufferStore store;

    @BeforeEach
    void setUp() {
        lenient().when(redisTemplate.opsForZSet()).thenReturn(zSetOps);
        lenient().when(properties.getKeyPrefix()).thenReturn("argus:buffer:");
        lenient().when(properties.getTtl()).thenReturn(Duration.ofMinutes(5));
    }

    @Test
    @DisplayName("enqueue 는 ZADD 와 EXPIRE 를 호출한다")
    void enqueue_호출시_ZADD와_EXPIRE를_호출한다() {
        double score = 1000.0;
        String envelopeJson = "{\"id\":\"abc\"}";

        store.enqueue(MetricBufferType.JVM, envelopeJson, score);

        verify(zSetOps).add("argus:buffer:jvm", envelopeJson, score);
        verify(redisTemplate).expire(eq("argus:buffer:jvm"), anyLong(), eq(TimeUnit.SECONDS));
    }

    @Test
    @DisplayName("peekOldest 는 ZRANGE 를 호출하고 결과를 반환한다")
    void peekOldest_ZRANGE를_호출하고_결과를_반환한다() {
        Set<String> members = new LinkedHashSet<>();
        members.add("member1");
        members.add("member2");
        when(zSetOps.range("argus:buffer:http", 0, 9)).thenReturn(members);

        List<String> result = store.peekOldest(MetricBufferType.HTTP, 10);

        assertThat(result).containsExactly("member1", "member2");
        verify(zSetOps).range("argus:buffer:http", 0, 9);
    }

    @Test
    @DisplayName("peekOldest 가 null 을 반환하면 빈 리스트를 반환한다")
    void peekOldest_null_반환시_빈리스트_반환() {
        when(zSetOps.range(anyString(), anyLong(), anyLong())).thenReturn(null);

        List<String> result = store.peekOldest(MetricBufferType.JVM, 5);

        assertThat(result).isEmpty();
    }

    @Test
    @DisplayName("remove 는 ZREM 을 호출한다")
    void remove_ZREM을_호출한다() {
        String member = "{\"id\":\"xyz\"}";

        store.remove(MetricBufferType.HIKARI, member);

        verify(zSetOps).remove("argus:buffer:hikari", member);
    }

    @Test
    @DisplayName("evictExpired 는 ZREMRANGEBYSCORE 를 호출하고 제거 건수를 반환한다")
    void evictExpired_ZREMRANGEBYSCORE를_호출하고_건수를_반환한다() {
        when(zSetOps.removeRangeByScore("argus:buffer:jvm", 0, 5000.0)).thenReturn(3L);

        long evicted = store.evictExpired(MetricBufferType.JVM, 5000.0);

        assertThat(evicted).isEqualTo(3L);
        verify(zSetOps).removeRangeByScore("argus:buffer:jvm", 0, 5000.0);
    }

    @Test
    @DisplayName("dropOldest 는 ZREMRANGEBYRANK 를 호출한다")
    void dropOldest_ZREMRANGEBYRANK를_호출한다() {
        store.dropOldest(MetricBufferType.HTTP, 2L);

        verify(zSetOps).removeRange("argus:buffer:http", 0, 1L);
    }

    @Test
    @DisplayName("dropNewest 는 해당 member 에 대해 ZREM 을 호출한다")
    void dropNewest_ZREM을_호출한다() {
        String newest = "{\"id\":\"new\"}";

        store.dropNewest(MetricBufferType.HIKARI, newest);

        verify(zSetOps).remove("argus:buffer:hikari", newest);
    }

    @Test
    @DisplayName("EXPIRE 는 TTL 의 2배 초 단위로 설정한다")
    void expire_TTL의_2배를_초단위로_설정한다() {
        store.enqueue(MetricBufferType.JVM, "data", 1000.0);

        // TTL 5분 = 300초, 2배 = 600초
        verify(redisTemplate).expire("argus:buffer:jvm", 600L, TimeUnit.SECONDS);
    }
}
