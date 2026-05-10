package com.example.argus.messaging;

import com.example.argus.dto.metric.http.HttpMetricSnapshotDto;
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
public class HttpMetricSnapshotProducer {

  private final KafkaTemplate<String, HttpMetricSnapshotDto> kafkaTemplate;

  @Value("${argus.kafka.topic.http-metrics-raw}")
  private String metricsTopic;

  public CompletableFuture<SendResult<String, HttpMetricSnapshotDto>> send(
      String serviceId, HttpMetricSnapshotDto snapshot) {
    CompletableFuture<SendResult<String, HttpMetricSnapshotDto>> future =
        kafkaTemplate.send(metricsTopic, serviceId, snapshot);
    future.whenComplete((result, ex) -> {
      if (ex != null) {
        log.error(
            "Failed to send HttpMetricSnapshot for serviceId={}: {}", serviceId, ex.getMessage(), ex);
      } else {
        log.debug(
            "Sent HttpMetricSnapshot for serviceId={}, offset={}",
            serviceId,
            result.getRecordMetadata().offset());
      }
    });
    return future;
  }
}
