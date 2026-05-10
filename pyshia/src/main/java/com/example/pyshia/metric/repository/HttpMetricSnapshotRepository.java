package com.example.pyshia.metric.repository;

import com.example.pyshia.metric.domain.HttpMetricSnapshotEntity;
import org.springframework.data.jpa.repository.JpaRepository;

public interface HttpMetricSnapshotRepository extends JpaRepository<HttpMetricSnapshotEntity, Long> {
}
