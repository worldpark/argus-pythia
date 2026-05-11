package com.example.pythia.metric.service;

import com.example.pythia.kafka.dto.hikari.HikariMetricSnapshotDto;
import com.example.pythia.kafka.dto.http.HttpMetricSnapshotDto;
import com.example.pythia.kafka.dto.jvm.JvmMetricSnapshotDto;

public interface MetricStoreService {

    void save(JvmMetricSnapshotDto snapshot);

    void save(HttpMetricSnapshotDto snapshot);

    void save(HikariMetricSnapshotDto snapshot);
}
