package com.example.argus.service.metric.snapshot;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class MetricSnapshotScheduler {

  private final JvmMetricSnapshotPublisher jvmMetricSnapshotPublisher;
  private final HttpMetricSnapshotPublisher httpMetricSnapshotPublisher;
  private final HikariMetricSnapshotPublisher hikariMetricSnapshotPublisher;

  @Scheduled(fixedDelay = 60_000L, initialDelay = 0L)
  public void triggerSnapshot() {
    try {
      jvmMetricSnapshotPublisher.publish().whenComplete((result, ex) -> {

        if (ex != null) {
          log.error("snapshot publish failed", ex);
        } else {
          log.debug("snapshot published: offset={}, partition={}",
              result.getRecordMetadata().offset(),
              result.getRecordMetadata().partition());
        }
      });

      httpMetricSnapshotPublisher.publish().whenComplete((result, ex) -> {

        if (ex != null) {
          log.error("snapshot publish failed", ex);
        } else {
          log.debug("snapshot published: offset={}, partition={}",
                  result.getRecordMetadata().offset(),
                  result.getRecordMetadata().partition());
        }
      });

      hikariMetricSnapshotPublisher.publish().whenComplete((result, ex) -> {

        if (ex != null) {
          log.error("snapshot publish failed", ex);
        } else {
          log.debug("snapshot published: offset={}, partition={}",
                  result.getRecordMetadata().offset(),
                  result.getRecordMetadata().partition());
        }
      });

    } catch (Exception e) {
      log.error("snapshot trigger failed", e);
    }
  }
}
