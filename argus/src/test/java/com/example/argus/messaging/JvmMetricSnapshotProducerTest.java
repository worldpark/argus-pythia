package com.example.argus.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.example.argus.dto.metric.jvm.JvmMetricSnapshotDto;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.util.ReflectionTestUtils;

@ExtendWith(MockitoExtension.class)
class JvmMetricSnapshotProducerTest {

  @Mock private KafkaTemplate<String, JvmMetricSnapshotDto> kafkaTemplate;

  private JvmMetricSnapshotProducer producer;

  private static final String TOPIC = "jvm.metrics.raw";

  @BeforeEach
  void setUp() {
    producer = new JvmMetricSnapshotProducer(kafkaTemplate);
    ReflectionTestUtils.setField(producer, "metricsTopic", TOPIC);
  }

  @Test
  @DisplayName("send 호출시 올바른 topic, key, value로 KafkaTemplate.send를 호출한다")
  void send_호출시_올바른_topic_key_value로_KafkaTemplate_send를_호출한다() {
    String serviceId = "svc-A";
    JvmMetricSnapshotDto snapshot = mock(JvmMetricSnapshotDto.class);
    // 콜백 미실행을 위해 완료되지 않은 future 반환
    when(kafkaTemplate.send(anyString(), anyString(), any(JvmMetricSnapshotDto.class)))
        .thenReturn(new CompletableFuture<>());

    ArgumentCaptor<String> topicCap = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> keyCap = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<JvmMetricSnapshotDto> snapshotCap =
        ArgumentCaptor.forClass(JvmMetricSnapshotDto.class);

    producer.send(serviceId, snapshot);

    verify(kafkaTemplate).send(topicCap.capture(), keyCap.capture(), snapshotCap.capture());
    assertThat(topicCap.getValue()).isEqualTo("jvm.metrics.raw");
    assertThat(keyCap.getValue()).isEqualTo(serviceId);
    assertThat(snapshotCap.getValue()).isSameAs(snapshot);
  }
}
