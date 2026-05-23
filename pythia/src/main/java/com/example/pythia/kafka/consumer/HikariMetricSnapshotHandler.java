package com.example.pythia.kafka.consumer;

import com.example.pythia.alert.service.ThresholdEvaluator;
import com.example.pythia.kafka.dto.SnapshotStatus;
import com.example.pythia.kafka.dto.hikari.HikariMetricSnapshotDto;
import com.example.pythia.metric.exception.MetricStoreException;
import com.example.pythia.metric.service.MetricStoreService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class HikariMetricSnapshotHandler {

    static final String TOPIC = "hikari.metrics.raw";

    private final ThresholdEvaluator evaluator;
    private final MetricStoreService metricStoreService;
    private final MessageDeduplicator deduplicator;

    public void handle(HikariMetricSnapshotDto snapshot) {
        if (snapshot.status() == SnapshotStatus.FAILED || snapshot.active() == null) {
            log.warn(
                "Empty or failed Hikari snapshot received: status={} app={}", snapshot.status(), snapshot.application());
            return;
        }
        if (!deduplicator.markProcessed(TOPIC, snapshot.application(), snapshot.instance(), snapshot.collectedAt())) {
            log.debug("Skip duplicate Hikari snapshot: app={}, inst={}, ts={}",
                snapshot.application(), snapshot.instance(), snapshot.collectedAt());
            return;
        }
        evaluator.evaluateHikari(snapshot);
        try {
            metricStoreService.save(snapshot);
        } catch (MetricStoreException e) {
            log.error("metric persist failed: app={} instance={}", snapshot.application(), snapshot.instance(), e);
        }
    }
}
