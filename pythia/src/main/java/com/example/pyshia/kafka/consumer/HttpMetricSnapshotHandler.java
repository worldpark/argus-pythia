package com.example.pyshia.kafka.consumer;

import com.example.pyshia.alert.service.ThresholdEvaluator;
import com.example.pyshia.kafka.dto.SnapshotStatus;
import com.example.pyshia.kafka.dto.http.HttpMetricSnapshotDto;
import com.example.pyshia.metric.exception.MetricStoreException;
import com.example.pyshia.metric.service.MetricStoreService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class HttpMetricSnapshotHandler {

    private final ThresholdEvaluator evaluator;
    private final MetricStoreService metricStoreService;

    public HttpMetricSnapshotHandler(ThresholdEvaluator evaluator, MetricStoreService metricStoreService) {
        this.evaluator = evaluator;
        this.metricStoreService = metricStoreService;
    }

    public void handle(HttpMetricSnapshotDto snapshot) {
        if (snapshot.status() == SnapshotStatus.FAILED || snapshot.p99() == null) {
            log.warn(
                "Empty or failed HTTP snapshot received: status={} app={}", snapshot.status(), snapshot.application());
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
