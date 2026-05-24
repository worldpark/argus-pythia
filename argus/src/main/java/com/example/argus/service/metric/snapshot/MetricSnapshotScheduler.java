package com.example.argus.service.metric.snapshot;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class MetricSnapshotScheduler {

  private static final AtomicLong CYCLE_SEQUENCE = new AtomicLong(0L);

  private final JvmMetricSnapshotPublisher jvmMetricSnapshotPublisher;
  private final HttpMetricSnapshotPublisher httpMetricSnapshotPublisher;
  private final HikariMetricSnapshotPublisher hikariMetricSnapshotPublisher;

  @Scheduled(fixedDelay = 60_000L, initialDelay = 0L)
  public void triggerSnapshot() {
    long cycleId = CYCLE_SEQUENCE.incrementAndGet();
    long cycleStart = System.nanoTime();
    log.info("metric-cycle start: cycleId={}", cycleId);
    try {
      CompletableFuture<?> jvmFuture = jvmMetricSnapshotPublisher.publish().whenComplete((result, ex) -> {

        if (ex != null) {
          log.error("snapshot publish failed: cycleId={} metricType=JVM", cycleId, ex);
        } else {
          log.debug("snapshot published: cycleId={} metricType=JVM offset={}, partition={}",
              cycleId,
              result.getRecordMetadata().offset(),
              result.getRecordMetadata().partition());
        }
      });

      CompletableFuture<?> httpFuture = httpMetricSnapshotPublisher.publish().whenComplete((result, ex) -> {

        if (ex != null) {
          log.error("snapshot publish failed: cycleId={} metricType=HTTP", cycleId, ex);
        } else {
          log.debug("snapshot published: cycleId={} metricType=HTTP offset={}, partition={}",
                  cycleId,
                  result.getRecordMetadata().offset(),
                  result.getRecordMetadata().partition());
        }
      });

      CompletableFuture<?> hikariFuture = hikariMetricSnapshotPublisher.publish().whenComplete((result, ex) -> {

        if (ex != null) {
          log.error("snapshot publish failed: cycleId={} metricType=HIKARI", cycleId, ex);
        } else {
          log.debug("snapshot published: cycleId={} metricType=HIKARI offset={}, partition={}",
                  cycleId,
                  result.getRecordMetadata().offset(),
                  result.getRecordMetadata().partition());
        }
      });

      CompletableFuture.allOf(jvmFuture, httpFuture, hikariFuture)
          .whenComplete((ignored, ex) -> {
            long totalMs = (System.nanoTime() - cycleStart) / 1_000_000;
            if (ex != null) {
              log.warn("metric-cycle completed with failure: cycleId={} totalMs={}", cycleId, totalMs);
            } else {
              log.info("metric-cycle completed: cycleId={} totalMs={}", cycleId, totalMs);
            }
          });

    } catch (Exception e) {
      log.error("snapshot trigger failed: cycleId={}", cycleId, e);
    }
  }
}
