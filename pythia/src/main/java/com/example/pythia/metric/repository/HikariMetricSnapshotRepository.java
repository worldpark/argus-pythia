package com.example.pythia.metric.repository;

import com.example.pythia.metric.domain.HikariMetricSnapshotEntity;
import org.springframework.data.jpa.repository.JpaRepository;

public interface HikariMetricSnapshotRepository extends JpaRepository<HikariMetricSnapshotEntity, Long> {
}
