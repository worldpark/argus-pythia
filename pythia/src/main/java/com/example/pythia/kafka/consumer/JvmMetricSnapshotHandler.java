package com.example.pythia.kafka.consumer;

import com.example.pythia.alert.service.ThresholdEvaluator;
import com.example.pythia.kafka.dto.SnapshotStatus;
import com.example.pythia.kafka.dto.jvm.JvmMetricSnapshotDto;
import com.example.pythia.metric.exception.MetricStoreException;
import com.example.pythia.metric.service.MetricStoreService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class JvmMetricSnapshotHandler {

    private final ThresholdEvaluator evaluator;
    private final MetricStoreService metricStoreService;

    public JvmMetricSnapshotHandler(ThresholdEvaluator evaluator, MetricStoreService metricStoreService) {
        this.evaluator = evaluator;
        this.metricStoreService = metricStoreService;
    }

    public void handle(JvmMetricSnapshotDto snapshot) {
        if (snapshot.status() == SnapshotStatus.FAILED || snapshot.cpu() == null) {
            log.warn(
                "Empty or failed JVM snapshot received: status={} app={}", snapshot.status(), snapshot.application());
            return;
        }
        evaluator.evaluateJvm(snapshot);
        try {
            metricStoreService.save(snapshot);
        } catch (MetricStoreException e) {
            log.error("metric persist failed: app={} instance={}", snapshot.application(), snapshot.instance(), e);
        }
    }
}
