package com.example.argus.service.metric.snapshot;

import com.example.argus.dto.metric.buffer.MetricBufferType;
import com.example.argus.dto.metric.http.HttpMetricSnapshotDto;
import com.example.argus.messaging.HttpMetricSnapshotProducer;
import com.example.argus.service.metric.buffer.MetricBufferService;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class HttpMetricSnapshotPublisher {

    private final HttpMetricSnapshotAssembler assembler;
    private final HttpMetricSnapshotProducer producer;
    private final MetricBufferService bufferService;

    public CompletableFuture<SendResult<String, HttpMetricSnapshotDto>> publish() {
        HttpMetricSnapshotDto snapshot = assembler.assemble();
        String serviceId = snapshot.application() != null ? snapshot.application() : "unknown";
        CompletableFuture<SendResult<String, HttpMetricSnapshotDto>> future = producer.send(serviceId, snapshot);
        future.whenComplete((result, ex) -> {
            if (ex != null) {
                try {
                    bufferService.enqueueOnFailure(MetricBufferType.HTTP, snapshot);
                } catch (Exception bufferEx) {
                    log.error("metric-buffer: fallback enqueue failed for HTTP snapshot, snapshot will be lost", bufferEx);
                }
            }
        });
        return future;
    }
}
