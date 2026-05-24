package com.example.argus.service.metric.snapshot;

import com.example.argus.dto.metric.buffer.MetricBufferType;
import com.example.argus.dto.metric.jvm.JvmMetricSnapshotDto;
import com.example.argus.messaging.JvmMetricSnapshotProducer;
import com.example.argus.service.metric.buffer.MetricBufferService;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class JvmMetricSnapshotPublisher {

    private final JvmMetricSnapshotAssembler assembler;
    private final JvmMetricSnapshotProducer producer;
    private final MetricBufferService bufferService;

    public CompletableFuture<SendResult<String, JvmMetricSnapshotDto>> publish() {
        long start = System.nanoTime();
        JvmMetricSnapshotDto snapshot = assembler.assemble();
        String serviceId = snapshot.application() != null ? snapshot.application() : "unknown";
        CompletableFuture<SendResult<String, JvmMetricSnapshotDto>> future = producer.send(serviceId, snapshot);
        future.whenComplete((result, ex) -> {
            long elapsedMs = (System.nanoTime() - start) / 1_000_000;
            if (ex != null) {
                log.warn("metric-publish failed: metricType=JVM serviceId={} elapsedMs={}", serviceId, elapsedMs, ex);
                try {
                    bufferService.enqueueOnFailure(MetricBufferType.JVM, snapshot);
                } catch (Exception bufferEx) {
                    log.error("metric-buffer: fallback enqueue failed for JVM snapshot, snapshot will be lost", bufferEx);
                }
            } else {
                log.info("metric-publish completed: metricType=JVM serviceId={} elapsedMs={}", serviceId, elapsedMs);
            }
        });
        return future;
    }
}
