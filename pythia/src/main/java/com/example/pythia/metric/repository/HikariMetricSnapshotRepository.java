package com.example.pythia.metric.repository;

import com.example.pythia.metric.domain.HikariMetricSnapshotEntity;
import java.time.OffsetDateTime;
import java.util.List;
import org.springframework.data.jpa.repository.EntityGraph;
import org.springframework.data.jpa.repository.JpaRepository;

public interface HikariMetricSnapshotRepository extends JpaRepository<HikariMetricSnapshotEntity, Long> {

  @EntityGraph(attributePaths = "points")
  List<HikariMetricSnapshotEntity> findByApplicationAndInstanceAndCollectedAtBetweenOrderByCollectedAtAsc(
      String application, String instance, OffsetDateTime from, OffsetDateTime to);
}
