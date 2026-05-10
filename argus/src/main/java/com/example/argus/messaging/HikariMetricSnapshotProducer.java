package com.example.argus.messaging;

import com.example.argus.dto.metric.hikari.HikariMetricSnapshotDto;
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
public class HikariMetricSnapshotProducer {

  private final KafkaTemplate<String, HikariMetricSnapshotDto> kafkaTemplate;

  @Value("${argus.kafka.topic.hikari-metrics-raw}")
  private String metricsTopic;

  public CompletableFuture<SendResult<String, HikariMetricSnapshotDto>> send(
      String serviceId, HikariMetricSnapshotDto snapshot) {
    CompletableFuture<SendResult<String, HikariMetricSnapshotDto>> future =
        kafkaTemplate.send(metricsTopic, serviceId, snapshot);
    future.whenComplete((result, ex) -> {
      if (ex != null) {
        log.error(
            "Failed to send HikariMetricSnapshot for serviceId={}: {}", serviceId, ex.getMessage(), ex);
      } else {
        log.debug(
            "Sent HikariMetricSnapshot for serviceId={}, offset={}",
            serviceId,
            result.getRecordMetadata().offset());
      }
    });
    return future;
  }
}
