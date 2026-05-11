package com.example.pythia.metric.repository;

import com.example.pythia.metric.domain.HttpMetricSnapshotEntity;
import org.springframework.data.jpa.repository.JpaRepository;

public interface HttpMetricSnapshotRepository extends JpaRepository<HttpMetricSnapshotEntity, Long> {
}
