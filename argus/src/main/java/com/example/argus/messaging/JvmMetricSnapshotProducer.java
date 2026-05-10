package com.example.argus.messaging;

import com.example.argus.dto.metric.jvm.JvmMetricSnapshotDto;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class JvmMetricSnapshotProducer {

  private final KafkaTemplate<String, JvmMetricSnapshotDto> kafkaTemplate;

  @Value("${argus.kafka.topic.metrics-raw}")
  private String metricsTopic;

  public CompletableFuture<SendResult<String, JvmMetricSnapshotDto>> send(
      String serviceId, JvmMetricSnapshotDto snapshot) {
    CompletableFuture<SendResult<String, JvmMetricSnapshotDto>> future =
        kafkaTemplate.send(metricsTopic, serviceId, snapshot);
    future.whenComplete((result, ex) -> {
      if (ex != null) {
        log.error(
            "Failed to send JvmMetricSnapshot for serviceId={}: {}", serviceId, ex.getMessage(), ex);
      } else {
        log.debug(
            "Sent JvmMetricSnapshot for serviceId={}, offset={}",
            serviceId,
            result.getRecordMetadata().offset());
      }
    });
    return future;
  }
}
