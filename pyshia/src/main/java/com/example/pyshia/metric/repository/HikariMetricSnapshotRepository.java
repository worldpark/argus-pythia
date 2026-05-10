package com.example.pyshia.metric.repository;

import com.example.pyshia.metric.domain.HikariMetricSnapshotEntity;
import org.springframework.data.jpa.repository.JpaRepository;

public interface HikariMetricSnapshotRepository extends JpaRepository<HikariMetricSnapshotEntity, Long> {
}
