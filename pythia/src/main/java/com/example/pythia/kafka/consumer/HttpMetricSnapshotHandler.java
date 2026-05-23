package com.example.pythia.kafka.consumer;

import com.example.pythia.alert.service.ThresholdEvaluator;
import com.example.pythia.kafka.dto.SnapshotStatus;
import com.example.pythia.kafka.dto.http.HttpMetricSnapshotDto;
import com.example.pythia.metric.exception.MetricStoreException;
import com.example.pythia.metric.service.MetricStoreService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class HttpMetricSnapshotHandler {

    static final String TOPIC = "http.metrics.raw";

    private final ThresholdEvaluator evaluator;
    private final MetricStoreService metricStoreService;
    private final MessageDeduplicator deduplicator;

    public void handle(HttpMetricSnapshotDto snapshot) {
        if (snapshot.status() == SnapshotStatus.FAILED || snapshot.p99() == null) {
            log.warn(
                "Empty or failed HTTP snapshot received: status={} app={}", snapshot.status(), snapshot.application());
            return;
        }
        if (!deduplicator.markProcessed(TOPIC, snapshot.application(), snapshot.instance(), snapshot.collectedAt())) {
            log.debug("Skip duplicate HTTP snapshot: app={}, inst={}, ts={}",
                snapshot.application(), snapshot.instance(), snapshot.collectedAt());
            return;
        }
        evaluator.evaluateHttp(snapshot);
        try {
            metricStoreService.save(snapshot);
        } catch (MetricStoreException e) {
            log.error("metric persist failed: app={} instance={}", snapshot.application(), snapshot.instance(), e);
        }
    }
}
