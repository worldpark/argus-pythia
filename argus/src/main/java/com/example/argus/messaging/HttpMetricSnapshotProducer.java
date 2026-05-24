package com.example.argus.messaging;

import com.example.argus.common.concurrency.ConcurrencyLimiter;
import com.example.argus.dto.metric.http.HttpMetricSnapshotDto;
import com.example.argus.exception.ConcurrencyLimitExceededException;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class HttpMetricSnapshotProducer {

    private final KafkaTemplate<String, HttpMetricSnapshotDto> kafkaTemplate;
    private final ConcurrencyLimiter limiter;

    @Value("${argus.kafka.topic.http-metrics-raw}")
    private String metricsTopic;

    public HttpMetricSnapshotProducer(
        KafkaTemplate<String, HttpMetricSnapshotDto> kafkaTemplate,
        @Qualifier("kafkaPublishConcurrencyLimiter") ConcurrencyLimiter limiter) {
        this.kafkaTemplate = kafkaTemplate;
        this.limiter = limiter;
    }

    public CompletableFuture<SendResult<String, HttpMetricSnapshotDto>> send(
        String serviceId, HttpMetricSnapshotDto snapshot) {
        try {
            limiter.acquire();
        } catch (ConcurrencyLimitExceededException e) {
            log.warn("kafka-publish throttled: serviceId={}, {}", serviceId, e.getMessage());
            CompletableFuture<SendResult<String, HttpMetricSnapshotDto>> failed = new CompletableFuture<>();
            failed.completeExceptionally(e);
            return failed;
        }

        try {
            CompletableFuture<SendResult<String, HttpMetricSnapshotDto>> future =
                kafkaTemplate.send(metricsTopic, serviceId, snapshot);
            future.whenComplete((result, ex) -> {
                limiter.release();
                if (ex != null) {
                    log.error(
                        "Failed to send HttpMetricSnapshot for serviceId={}: {}",
                        serviceId, ex.getMessage(), ex);
                } else {
                    log.debug(
                        "Sent HttpMetricSnapshot for serviceId={}, offset={}",
                        serviceId, result.getRecordMetadata().offset());
                }
            });
            return future;
        } catch (Exception e) {
            limiter.release();
            log.error(
                "Failed to send HttpMetricSnapshot (sync error) for serviceId={}: {}",
                serviceId, e.getMessage(), e);
            CompletableFuture<SendResult<String, HttpMetricSnapshotDto>> failed = new CompletableFuture<>();
            failed.completeExceptionally(e);
            return failed;
        }
    }
}
