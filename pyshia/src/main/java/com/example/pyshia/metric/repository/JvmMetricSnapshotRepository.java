package com.example.pyshia.metric.repository;

import com.example.pyshia.metric.domain.JvmMetricSnapshotEntity;
import org.springframework.data.jpa.repository.JpaRepository;

public interface JvmMetricSnapshotRepository extends JpaRepository<JvmMetricSnapshotEntity, Long> {
}
