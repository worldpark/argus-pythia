package com.example.argus.service.metric.snapshot;

import com.example.argus.dto.metric.buffer.MetricBufferType;
import com.example.argus.dto.metric.hikari.HikariMetricSnapshotDto;
import com.example.argus.messaging.HikariMetricSnapshotProducer;
import com.example.argus.service.metric.buffer.MetricBufferService;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class HikariMetricSnapshotPublisher {

    private final HikariMetricSnapshotAssembler assembler;
    private final HikariMetricSnapshotProducer producer;
    private final MetricBufferService bufferService;

    public CompletableFuture<SendResult<String, HikariMetricSnapshotDto>> publish() {
        HikariMetricSnapshotDto snapshot = assembler.assemble();
        String serviceId = snapshot.application() != null ? snapshot.application() : "unknown";
        CompletableFuture<SendResult<String, HikariMetricSnapshotDto>> future = producer.send(serviceId, snapshot);
        future.whenComplete((result, ex) -> {
            if (ex != null) {
                try {
                    bufferService.enqueueOnFailure(MetricBufferType.HIKARI, snapshot);
                } catch (Exception bufferEx) {
                    log.error("metric-buffer: fallback enqueue failed for HIKARI snapshot, snapshot will be lost", bufferEx);
                }
            }
        });
        return future;
    }
}
