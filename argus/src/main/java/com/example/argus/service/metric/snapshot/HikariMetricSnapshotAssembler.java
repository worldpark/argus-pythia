package com.example.argus.service.metric.snapshot;

import com.example.argus.dto.metric.hikari.HikariActiveDto;
import com.example.argus.dto.metric.hikari.HikariMetricSnapshotDto;
import com.example.argus.dto.metric.hikari.HikariPendingDto;
import com.example.argus.dto.metric.SnapshotStatus;
import com.example.argus.exception.PrometheusQueryException;
import com.example.argus.service.PrometheusQueryService;
import com.example.argus.service.metric.MetricType;
import com.example.argus.service.metric.mapper.MetricPointMapper;
import com.example.argus.service.metric.mapper.MetricPointMapper.LabeledPoint;
import com.example.argus.service.metric.mapper.MetricPointMapper.MultiMappingResult;
import java.time.Clock;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Service
public class HikariMetricSnapshotAssembler {

    private static final String LABEL_POOL = "pool";

    private static final Logger log = LoggerFactory.getLogger(HikariMetricSnapshotAssembler.class);

    private final PrometheusQueryService queryService;
    private final Clock clock;
    private final Executor fanoutExecutor;

    public HikariMetricSnapshotAssembler(
            PrometheusQueryService queryService,
            Clock clock,
            @Qualifier("metricFanoutExecutor") Executor fanoutExecutor) {
        this.queryService = queryService;
        this.clock = clock;
        this.fanoutExecutor = fanoutExecutor;
    }

    public HikariMetricSnapshotDto assemble() {
        long start = System.nanoTime();

        OffsetDateTime collectedAt = OffsetDateTime.now(clock);

        CompletableFuture<MultiMappingResult> fActive =
                CompletableFuture.supplyAsync(
                        () -> safeQuery(MetricType.HIKARI_ACTIVE_CONNECTIONS), fanoutExecutor);
        CompletableFuture<MultiMappingResult> fUsageRatio =
                CompletableFuture.supplyAsync(
                        () -> safeQuery(MetricType.HIKARI_USAGE_RATIO), fanoutExecutor);
        CompletableFuture<MultiMappingResult> fPending =
                CompletableFuture.supplyAsync(
                        () -> safeQuery(MetricType.HIKARI_PENDING_CONNECTIONS), fanoutExecutor);

        CompletableFuture.allOf(fActive, fUsageRatio, fPending).join();

        LabelAccumulator labels = new LabelAccumulator();

        MultiMappingResult activeResult = fActive.join();
        if (activeResult instanceof MultiMappingResult.Success s) {
            s.points().forEach(labels::accept);
        }
        MultiMappingResult usageRatioResult = fUsageRatio.join();
        if (usageRatioResult instanceof MultiMappingResult.Success s) {
            s.points().forEach(labels::accept);
        }
        HikariActiveDto active = HikariActiveDto.from(activeResult, usageRatioResult);

        MultiMappingResult pendingResult = fPending.join();
        if (pendingResult instanceof MultiMappingResult.Success s) {
            s.points().forEach(labels::accept);
        }
        HikariPendingDto pending = HikariPendingDto.from(pendingResult);

        SnapshotStatus snapshotStatus = SnapshotStatus.from(List.of(active.status(), pending.status()));

        log.info("metric-assemble: type=HIKARI elapsedMs={}", (System.nanoTime() - start) / 1_000_000);

        return new HikariMetricSnapshotDto(
                labels.application(), labels.instance(), collectedAt, active, pending, snapshotStatus);
    }

    private MultiMappingResult safeQuery(MetricType type) {
        try {
            return MetricPointMapper.toPoints(queryService.queryByMetric(type), type, LABEL_POOL);
        } catch (PrometheusQueryException e) {
            return new MultiMappingResult.QueryFailed(e.getMessage());
        } catch (Exception e) {
            log.error("Unexpected error while querying {}", type, e);
            return new MultiMappingResult.ParseFailed(e.getMessage());
        }
    }

    private static class LabelAccumulator {
        private String application;
        private String instance;

        void accept(LabeledPoint point) {
            if (application == null && point.application() != null) {
                application = point.application();
            }
            if (instance == null && point.instance() != null) {
                instance = point.instance();
            }
        }

        String application() {
            return application;
        }

        String instance() {
            return instance;
        }
    }
}
