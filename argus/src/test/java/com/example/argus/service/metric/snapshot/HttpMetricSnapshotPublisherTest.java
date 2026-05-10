package com.example.argus.service.metric.snapshot;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.example.argus.dto.metric.http.HttpMetricSnapshotDto;
import com.example.argus.messaging.HttpMetricSnapshotProducer;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.SendResult;

@ExtendWith(MockitoExtension.class)
class HttpMetricSnapshotPublisherTest {

  @Mock private HttpMetricSnapshotAssembler assembler;
  @Mock private HttpMetricSnapshotProducer producer;
  @InjectMocks private HttpMetricSnapshotPublisher publisher;

  @Test
  @DisplayName("application이 있으면 해당 값을 partition key로 producer.send를 호출한다")
  void publish_application이_있으면_해당값으로_send를_호출한다() {
    HttpMetricSnapshotDto snapshot = mock(HttpMetricSnapshotDto.class);
    when(snapshot.application()).thenReturn("svc-A");
    when(assembler.assemble()).thenReturn(snapshot);
    CompletableFuture<SendResult<String, HttpMetricSnapshotDto>> future = new CompletableFuture<>();
    when(producer.send("svc-A", snapshot)).thenReturn(future);

    CompletableFuture<SendResult<String, HttpMetricSnapshotDto>> result = publisher.publish();

    verify(producer).send("svc-A", snapshot);
    assertThat(result).isSameAs(future);
  }

  @Test
  @DisplayName("application이 null이면 'unknown'을 partition key로 사용한다")
  void publish_application이_null이면_unknown키로_send를_호출한다() {
    HttpMetricSnapshotDto snapshot = mock(HttpMetricSnapshotDto.class);
    when(snapshot.application()).thenReturn(null);
    when(assembler.assemble()).thenReturn(snapshot);
    when(producer.send("unknown", snapshot)).thenReturn(new CompletableFuture<>());

    publisher.publish();

    verify(producer).send("unknown", snapshot);
  }

  @Test
  @DisplayName("assembler가 예외를 던지면 publisher도 그대로 전파한다")
  void publish_assembler_예외_전파() {
    RuntimeException cause = new RuntimeException("prometheus error");
    when(assembler.assemble()).thenThrow(cause);

    assertThatThrownBy(() -> publisher.publish()).isSameAs(cause);

    verifyNoInteractions(producer);
  }
}
