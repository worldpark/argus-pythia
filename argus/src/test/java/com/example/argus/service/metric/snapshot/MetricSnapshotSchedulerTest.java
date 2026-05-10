package com.example.argus.service.metric.snapshot;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.example.argus.dto.metric.jvm.JvmMetricSnapshotDto;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.SendResult;

@ExtendWith(MockitoExtension.class)
class MetricSnapshotSchedulerTest {

  @Mock private JvmMetricSnapshotPublisher publisher;
  @InjectMocks private MetricSnapshotScheduler scheduler;

  @Test
  @DisplayName("triggerSnapshot 호출 시 publisher.publish()를 정확히 1회 호출한다")
  void triggerSnapshot_invokesPublisherPublishOnce() {
    when(publisher.publish()).thenReturn(new CompletableFuture<>());

    scheduler.triggerSnapshot();

    verify(publisher, times(1)).publish();
  }

  @Test
  @DisplayName("publisher가 RuntimeException을 throw해도 triggerSnapshot은 예외를 전파하지 않는다")
  void triggerSnapshot_publisherThrows_doesNotPropagate() {
    when(publisher.publish()).thenThrow(new RuntimeException("boom"));

    assertThatNoException().isThrownBy(() -> scheduler.triggerSnapshot());
    verify(publisher, times(1)).publish();
  }

  @Test
  @DisplayName("Kafka future가 실패해도 whenComplete에서 처리되고 triggerSnapshot은 throw하지 않는다")
  void triggerSnapshot_kafkaFutureFails_logsButDoesNotThrow() {
    CompletableFuture<SendResult<String, JvmMetricSnapshotDto>> failedFuture = new CompletableFuture<>();
    failedFuture.completeExceptionally(new RuntimeException("kafka down"));
    when(publisher.publish()).thenReturn(failedFuture);

    assertThatNoException().isThrownBy(() -> scheduler.triggerSnapshot());
    verify(publisher, times(1)).publish();
  }

  @Test
  @DisplayName("Kafka future가 성공하면 whenComplete 성공 분기가 실행되고 throw하지 않는다")
  void triggerSnapshot_kafkaFutureSucceeds_logsSuccess() {
    RecordMetadata meta = new RecordMetadata(
            new TopicPartition("jvm.metrics.raw", 0), 0L, 0, 0L, 0, 0);
    SendResult<String, JvmMetricSnapshotDto> result =
            new SendResult<>(null, meta);
    when(publisher.publish())
            .thenReturn(CompletableFuture.completedFuture(result));

    assertThatNoException().isThrownBy(() -> scheduler.triggerSnapshot());
    verify(publisher, times(1)).publish();
  }
}
