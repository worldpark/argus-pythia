package com.example.pythia.metric.repository;

import com.example.pythia.metric.domain.JvmMetricSnapshotEntity;
import org.springframework.data.jpa.repository.JpaRepository;

public interface JvmMetricSnapshotRepository extends JpaRepository<JvmMetricSnapshotEntity, Long> {
}
