package com.example.pythia.metric.repository;

import com.example.pythia.metric.domain.HttpMetricSnapshotEntity;
import java.time.OffsetDateTime;
import java.util.List;
import org.springframework.data.jpa.repository.EntityGraph;
import org.springframework.data.jpa.repository.JpaRepository;

public interface HttpMetricSnapshotRepository extends JpaRepository<HttpMetricSnapshotEntity, Long> {

  @EntityGraph(attributePaths = "points")
  List<HttpMetricSnapshotEntity> findByApplicationAndInstanceAndCollectedAtBetweenOrderByCollectedAtAsc(
      String application, String instance, OffsetDateTime from, OffsetDateTime to);
}
