package com.example.pythia.kafka.consumer;

import com.example.pythia.kafka.dto.jvm.JvmMetricSnapshotDto;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class JvmMetricSnapshotConsumer {

  private final JvmMetricSnapshotHandler handler;

  public JvmMetricSnapshotConsumer(JvmMetricSnapshotHandler handler) {
    this.handler = handler;
  }

  @KafkaListener(
      topics = "${pythia.kafka.topic.jvm-metrics-raw}",
      containerFactory = "jvmKafkaListenerContainerFactory")
  public void consume(
      @Payload JvmMetricSnapshotDto snapshot,
      @Header(KafkaHeaders.RECEIVED_KEY) String key,
      @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
    log.debug("Received JVM metric snapshot: topic={} key={} app={}", topic, key, snapshot.application());
    handler.handle(snapshot);
  }
}
