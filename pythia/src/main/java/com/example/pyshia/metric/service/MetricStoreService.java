package com.example.pyshia.metric.service;

import com.example.pyshia.kafka.dto.hikari.HikariMetricSnapshotDto;
import com.example.pyshia.kafka.dto.http.HttpMetricSnapshotDto;
import com.example.pyshia.kafka.dto.jvm.JvmMetricSnapshotDto;

public interface MetricStoreService {

    void save(JvmMetricSnapshotDto snapshot);

    void save(HttpMetricSnapshotDto snapshot);

    void save(HikariMetricSnapshotDto snapshot);
}
