package com.example.argus.messaging;

import com.example.argus.common.concurrency.ConcurrencyLimiter;
import com.example.argus.dto.metric.hikari.HikariMetricSnapshotDto;
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
public class HikariMetricSnapshotProducer {

    private final KafkaTemplate<String, HikariMetricSnapshotDto> kafkaTemplate;
    private final ConcurrencyLimiter limiter;

    @Value("${argus.kafka.topic.hikari-metrics-raw}")
    private String metricsTopic;

    public HikariMetricSnapshotProducer(
        KafkaTemplate<String, HikariMetricSnapshotDto> kafkaTemplate,
        @Qualifier("kafkaPublishConcurrencyLimiter") ConcurrencyLimiter limiter) {
        this.kafkaTemplate = kafkaTemplate;
        this.limiter = limiter;
    }

    public CompletableFuture<SendResult<String, HikariMetricSnapshotDto>> send(
        String serviceId, HikariMetricSnapshotDto snapshot) {
        try {
            limiter.acquire();
        } catch (ConcurrencyLimitExceededException e) {
            log.warn("kafka-publish throttled: serviceId={}, {}", serviceId, e.getMessage());
            CompletableFuture<SendResult<String, HikariMetricSnapshotDto>> failed = new CompletableFuture<>();
            failed.completeExceptionally(e);
            return failed;
        }

        try {
            CompletableFuture<SendResult<String, HikariMetricSnapshotDto>> future =
                kafkaTemplate.send(metricsTopic, serviceId, snapshot);
            future.whenComplete((result, ex) -> {
                limiter.release();
                if (ex != null) {
                    log.error(
                        "Failed to send HikariMetricSnapshot for serviceId={}: {}",
                        serviceId, ex.getMessage(), ex);
                } else {
                    log.debug(
                        "Sent HikariMetricSnapshot for serviceId={}, offset={}",
                        serviceId, result.getRecordMetadata().offset());
                }
            });
            return future;
        } catch (Exception e) {
            limiter.release();
            log.error(
                "Failed to send HikariMetricSnapshot (sync error) for serviceId={}: {}",
                serviceId, e.getMessage(), e);
            CompletableFuture<SendResult<String, HikariMetricSnapshotDto>> failed = new CompletableFuture<>();
            failed.completeExceptionally(e);
            return failed;
        }
    }
}
