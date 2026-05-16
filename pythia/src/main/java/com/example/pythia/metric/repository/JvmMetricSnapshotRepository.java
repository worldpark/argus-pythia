package com.example.pythia.metric.repository;

import com.example.pythia.metric.domain.JvmMetricSnapshotEntity;
import java.time.OffsetDateTime;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface JvmMetricSnapshotRepository extends JpaRepository<JvmMetricSnapshotEntity, Long> {

  List<JvmMetricSnapshotEntity> findByApplicationAndInstanceAndCollectedAtBetweenOrderByCollectedAtAsc(
      String application, String instance, OffsetDateTime from, OffsetDateTime to);
}
