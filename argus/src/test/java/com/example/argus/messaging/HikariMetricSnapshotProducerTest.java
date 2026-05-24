package com.example.argus.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.example.argus.common.concurrency.ConcurrencyLimiter;
import com.example.argus.dto.metric.hikari.HikariMetricSnapshotDto;
import com.example.argus.exception.ConcurrencyLimitExceededException;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.util.ReflectionTestUtils;

@ExtendWith(MockitoExtension.class)
class HikariMetricSnapshotProducerTest {

    @Mock private KafkaTemplate<String, HikariMetricSnapshotDto> kafkaTemplate;
    @Mock private ConcurrencyLimiter limiter;

    private HikariMetricSnapshotProducer producer;

    private static final String TOPIC = "hikari.metrics.raw";

    @BeforeEach
    void setUp() {
        producer = new HikariMetricSnapshotProducer(kafkaTemplate, limiter);
        ReflectionTestUtils.setField(producer, "metricsTopic", TOPIC);
    }

    @Test
    @DisplayName("send 호출시 올바른 topic, key, value로 KafkaTemplate.send를 호출한다")
    void send_호출시_올바른_topic_key_value로_KafkaTemplate_send를_호출한다() {
        String serviceId = "svc-A";
        HikariMetricSnapshotDto snapshot = mock(HikariMetricSnapshotDto.class);
        when(kafkaTemplate.send(anyString(), anyString(), any(HikariMetricSnapshotDto.class)))
            .thenReturn(new CompletableFuture<>());

        ArgumentCaptor<String> topicCap = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> keyCap = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<HikariMetricSnapshotDto> snapshotCap =
            ArgumentCaptor.forClass(HikariMetricSnapshotDto.class);

        producer.send(serviceId, snapshot);

        verify(kafkaTemplate).send(topicCap.capture(), keyCap.capture(), snapshotCap.capture());
        assertThat(topicCap.getValue()).isEqualTo("hikari.metrics.raw");
        assertThat(keyCap.getValue()).isEqualTo(serviceId);
        assertThat(snapshotCap.getValue()).isSameAs(snapshot);
    }

    @Test
    @DisplayName("정상 send: future 완료 시 release() 가 1회 호출된다")
    void send_normalSend_releaseCalledOnceWhenFutureCompletes() {
        String serviceId = "svc-A";
        HikariMetricSnapshotDto snapshot = mock(HikariMetricSnapshotDto.class);
        CompletableFuture<SendResult<String, HikariMetricSnapshotDto>> future = new CompletableFuture<>();
        when(kafkaTemplate.send(anyString(), anyString(), any(HikariMetricSnapshotDto.class)))
            .thenReturn(future);

        producer.send(serviceId, snapshot);
        verify(limiter, never()).release();

        future.complete(mock(SendResult.class));
        verify(limiter, times(1)).release();
    }

    @Test
    @DisplayName("acquire 실패 시 ConcurrencyLimitExceededException 원인의 예외 완료 future 가 반환되고 kafkaTemplate.send 는 미호출된다")
    void send_acquireFails_returnsExceptionallyCompletedFutureAndSendNotCalled() {
        String serviceId = "svc-A";
        HikariMetricSnapshotDto snapshot = mock(HikariMetricSnapshotDto.class);
        doThrow(new ConcurrencyLimitExceededException("kafka-publish", 3, 300L))
            .when(limiter).acquire();

        CompletableFuture<SendResult<String, HikariMetricSnapshotDto>> result =
            producer.send(serviceId, snapshot);

        assertThat(result.isCompletedExceptionally()).isTrue();
        assertThatThrownBy(result::join)
            .hasCauseInstanceOf(ConcurrencyLimitExceededException.class);
        verify(kafkaTemplate, never()).send(anyString(), anyString(), any());
        verify(limiter, never()).release();
    }

    @Test
    @DisplayName("acquire 성공 후 kafkaTemplate.send 가 동기 예외 시 release() 1회 호출 + 예외 완료 future 반환")
    void send_syncSendThrows_releaseCalledOnceAndExceptionalFutureReturned() {
        String serviceId = "svc-A";
        HikariMetricSnapshotDto snapshot = mock(HikariMetricSnapshotDto.class);
        when(kafkaTemplate.send(anyString(), anyString(), any(HikariMetricSnapshotDto.class)))
            .thenThrow(new RuntimeException("serializer failure"));

        CompletableFuture<SendResult<String, HikariMetricSnapshotDto>> result =
            producer.send(serviceId, snapshot);

        assertThat(result.isCompletedExceptionally()).isTrue();
        verify(limiter, times(1)).release();
    }

    @Test
    @DisplayName("send 가 정상 future 반환 후 future 예외 완료 시 release() 1회만 호출된다")
    void send_futureCompletesExceptionally_releaseCalledExactlyOnce() {
        String serviceId = "svc-A";
        HikariMetricSnapshotDto snapshot = mock(HikariMetricSnapshotDto.class);
        CompletableFuture<SendResult<String, HikariMetricSnapshotDto>> future = new CompletableFuture<>();
        when(kafkaTemplate.send(anyString(), anyString(), any(HikariMetricSnapshotDto.class)))
            .thenReturn(future);

        producer.send(serviceId, snapshot);
        future.completeExceptionally(new RuntimeException("broker error"));

        verify(limiter, times(1)).release();
    }
}
