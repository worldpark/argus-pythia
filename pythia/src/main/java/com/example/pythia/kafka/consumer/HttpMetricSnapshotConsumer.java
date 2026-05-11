package com.example.pythia.kafka.consumer;

import com.example.pythia.kafka.dto.http.HttpMetricSnapshotDto;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class HttpMetricSnapshotConsumer {

  private final HttpMetricSnapshotHandler handler;

  public HttpMetricSnapshotConsumer(HttpMetricSnapshotHandler handler) {
    this.handler = handler;
  }

  @KafkaListener(
      topics = "${pythia.kafka.topic.http-metrics-raw}",
      containerFactory = "httpKafkaListenerContainerFactory")
  public void consume(
      @Payload HttpMetricSnapshotDto snapshot,
      @Header(KafkaHeaders.RECEIVED_KEY) String key,
      @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
    log.debug("Received HTTP metric snapshot: topic={} key={} app={}", topic, key, snapshot.application());
    handler.handle(snapshot);
  }
}
