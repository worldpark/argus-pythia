package com.example.pythia.metric.service;

import com.example.pythia.kafka.dto.hikari.HikariActiveDto;
import com.example.pythia.kafka.dto.hikari.HikariMetricSnapshotDto;
import com.example.pythia.kafka.dto.hikari.HikariPendingDto;
import com.example.pythia.kafka.dto.hikari.PoolMetricPointDto;
import com.example.pythia.kafka.dto.http.EndpointMetricPointDto;
import com.example.pythia.kafka.dto.http.HttpErrorRateDto;
import com.example.pythia.kafka.dto.http.HttpMetricSnapshotDto;
import com.example.pythia.kafka.dto.http.HttpResponseTimeDto;
import com.example.pythia.kafka.dto.http.HttpThroughputDto;
import com.example.pythia.kafka.dto.jvm.CpuUsageDto;
import com.example.pythia.kafka.dto.jvm.GcMetricDto;
import com.example.pythia.kafka.dto.jvm.JvmMetricSnapshotDto;
import com.example.pythia.kafka.dto.jvm.MemoryUsageDto;
import com.example.pythia.kafka.dto.jvm.ThreadMetricDto;
import com.example.pythia.metric.domain.HikariMetricKind;
import com.example.pythia.metric.domain.HikariMetricSnapshotEntity;
import com.example.pythia.metric.domain.HikariPoolMetricPointEntity;
import com.example.pythia.metric.domain.HttpEndpointMetricPointEntity;
import com.example.pythia.metric.domain.HttpMetricKind;
import com.example.pythia.metric.domain.HttpMetricSnapshotEntity;
import com.example.pythia.metric.domain.JvmMetricSnapshotEntity;
import com.example.pythia.metric.exception.MetricStoreErrorCode;
import com.example.pythia.metric.exception.MetricStoreException;
import com.example.pythia.metric.repository.HikariMetricSnapshotRepository;
import com.example.pythia.metric.repository.HttpMetricSnapshotRepository;
import com.example.pythia.metric.repository.JvmMetricSnapshotRepository;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Service
@RequiredArgsConstructor
public class MetricStoreServiceImpl implements MetricStoreService {

    private final JvmMetricSnapshotRepository jvmMetricSnapshotRepository;
    private final HttpMetricSnapshotRepository httpMetricSnapshotRepository;
    private final HikariMetricSnapshotRepository hikariMetricSnapshotRepository;

    @Override
    @Transactional
    public void save(JvmMetricSnapshotDto snapshot) {
        validateRequiredFields(snapshot.application(), snapshot.instance(), snapshot.collectedAt());
        JvmMetricSnapshotEntity entity = toJvmEntity(snapshot);
        try {
            jvmMetricSnapshotRepository.save(entity);
            log.debug("JVM snapshot saved: app={} instance={}", snapshot.application(), snapshot.instance());
        } catch (DataAccessException e) {
            throw new MetricStoreException(MetricStoreErrorCode.METRIC_PERSIST_FAILED, e);
        }
    }

    @Override
    @Transactional
    public void save(HttpMetricSnapshotDto snapshot) {
        validateRequiredFields(snapshot.application(), snapshot.instance(), snapshot.collectedAt());
        HttpMetricSnapshotEntity entity = toHttpEntity(snapshot);
        try {
            httpMetricSnapshotRepository.save(entity);
            log.debug("HTTP snapshot saved: app={} instance={}", snapshot.application(), snapshot.instance());
        } catch (DataAccessException e) {
            throw new MetricStoreException(MetricStoreErrorCode.METRIC_PERSIST_FAILED, e);
        }
    }

    @Override
    @Transactional
    public void save(HikariMetricSnapshotDto snapshot) {
        validateRequiredFields(snapshot.application(), snapshot.instance(), snapshot.collectedAt());
        HikariMetricSnapshotEntity entity = toHikariEntity(snapshot);
        try {
            hikariMetricSnapshotRepository.save(entity);
            log.debug("Hikari snapshot saved: app={} instance={}", snapshot.application(), snapshot.instance());
        } catch (DataAccessException e) {
            throw new MetricStoreException(MetricStoreErrorCode.METRIC_PERSIST_FAILED, e);
        }
    }

    private static void validateRequiredFields(
        String application, String instance, java.time.OffsetDateTime collectedAt) {
        if (application == null || instance == null || collectedAt == null) {
            throw new MetricStoreException(MetricStoreErrorCode.INVALID_SNAPSHOT_PAYLOAD);
        }
    }

    private static JvmMetricSnapshotEntity toJvmEntity(JvmMetricSnapshotDto snapshot) {
        CpuUsageDto cpu = snapshot.cpu();
        MemoryUsageDto memory = snapshot.memory();
        GcMetricDto gc = snapshot.gc();
        ThreadMetricDto thread = snapshot.thread();

        return JvmMetricSnapshotEntity.builder()
            .application(snapshot.application())
            .instance(snapshot.instance())
            .collectedAt(snapshot.collectedAt())
            .status(snapshot.status())
            .cpuUsagePercent(cpu != null ? cpu.usagePercent() : null)
            .cpuMeasuredAt(cpu != null ? cpu.measuredAt() : null)
            .cpuStatus(cpu != null ? cpu.status() : null)
            .cpuMissingReason(cpu != null ? cpu.missingReason() : null)
            .heapUsagePercent(memory != null ? memory.heapUsagePercent() : null)
            .oldGenUsagePercent(memory != null ? memory.oldGenUsagePercent() : null)
            .memoryMeasuredAt(memory != null ? memory.measuredAt() : null)
            .memoryStatus(memory != null ? memory.status() : null)
            .memoryMissingReason(memory != null ? memory.missingReason() : null)
            .gcAvgDurationSeconds(gc != null ? gc.avgDurationSeconds() : null)
            .gcCount(gc != null ? gc.count() : null)
            .gcMeasuredAt(gc != null ? gc.measuredAt() : null)
            .gcStatus(gc != null ? gc.status() : null)
            .gcMissingReason(gc != null ? gc.missingReason() : null)
            .threadActiveCount(thread != null ? thread.activeCount() : null)
            .threadPeakCount(thread != null ? thread.peakCount() : null)
            .threadDaemonCount(thread != null ? thread.daemonCount() : null)
            .threadMeasuredAt(thread != null ? thread.measuredAt() : null)
            .threadStatus(thread != null ? thread.status() : null)
            .threadMissingReason(thread != null ? thread.missingReason() : null)
            .build();
    }

    private static HttpMetricSnapshotEntity toHttpEntity(HttpMetricSnapshotDto snapshot) {
        HttpResponseTimeDto p99 = snapshot.p99();
        HttpThroughputDto rps = snapshot.rps();
        HttpErrorRateDto errorRate = snapshot.errorRate();

        HttpMetricSnapshotEntity entity = HttpMetricSnapshotEntity.builder()
            .application(snapshot.application())
            .instance(snapshot.instance())
            .collectedAt(snapshot.collectedAt())
            .status(snapshot.status())
            .p99MeasuredAt(p99 != null ? p99.measuredAt() : null)
            .p99Status(p99 != null ? p99.status() : null)
            .p99MissingReason(p99 != null ? p99.missingReason() : null)
            .rpsMeasuredAt(rps != null ? rps.measuredAt() : null)
            .rpsStatus(rps != null ? rps.status() : null)
            .rpsMissingReason(rps != null ? rps.missingReason() : null)
            .errorRateMeasuredAt(errorRate != null ? errorRate.measuredAt() : null)
            .errorRateStatus(errorRate != null ? errorRate.status() : null)
            .errorRateMissingReason(errorRate != null ? errorRate.missingReason() : null)
            .build();

        if (p99 != null && p99.points() != null) {
            for (EndpointMetricPointDto point : p99.points()) {
                if (point == null) continue;
                entity.addPoint(HttpEndpointMetricPointEntity.of(
                    HttpMetricKind.P99, point.endpoint(), point.value(), point.measuredAt()));
            }
        }

        if (rps != null && rps.points() != null) {
            for (EndpointMetricPointDto point : rps.points()) {
                if (point == null) continue;
                entity.addPoint(HttpEndpointMetricPointEntity.of(
                    HttpMetricKind.RPS, point.endpoint(), point.value(), point.measuredAt()));
            }
        }

        if (errorRate != null && errorRate.points() != null) {
            for (EndpointMetricPointDto point : errorRate.points()) {
                if (point == null) continue;
                entity.addPoint(HttpEndpointMetricPointEntity.of(
                    HttpMetricKind.ERROR_RATE, point.endpoint(), point.value(), point.measuredAt()));
            }
        }

        return entity;
    }

    private static HikariMetricSnapshotEntity toHikariEntity(HikariMetricSnapshotDto snapshot) {
        HikariActiveDto active = snapshot.active();
        HikariPendingDto pending = snapshot.pending();

        HikariMetricSnapshotEntity entity = HikariMetricSnapshotEntity.builder()
            .application(snapshot.application())
            .instance(snapshot.instance())
            .collectedAt(snapshot.collectedAt())
            .status(snapshot.status())
            .activeMeasuredAt(active != null ? active.measuredAt() : null)
            .activeStatus(active != null ? active.status() : null)
            .activeMissingReason(active != null ? active.missingReason() : null)
            .pendingMeasuredAt(pending != null ? pending.measuredAt() : null)
            .pendingStatus(pending != null ? pending.status() : null)
            .pendingMissingReason(pending != null ? pending.missingReason() : null)
            .build();

        if (active != null && active.points() != null) {
            for (PoolMetricPointDto point : active.points()) {
                if (point == null) continue;
                entity.addPoint(HikariPoolMetricPointEntity.of(
                    HikariMetricKind.ACTIVE, point.pool(), point.value(), point.measuredAt()));
            }
        }

        if (pending != null && pending.points() != null) {
            for (PoolMetricPointDto point : pending.points()) {
                if (point == null) continue;
                entity.addPoint(HikariPoolMetricPointEntity.of(
                    HikariMetricKind.PENDING, point.pool(), point.value(), point.measuredAt()));
            }
        }

        if (active != null && active.usageRatio() != null) {
            for (PoolMetricPointDto point : active.usageRatio()) {
                if (point == null) continue;
                entity.addPoint(HikariPoolMetricPointEntity.of(
                    HikariMetricKind.USAGE_RATIO, point.pool(), point.value(), point.measuredAt()));
            }
        }

        return entity;
    }
}
