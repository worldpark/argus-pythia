package com.example.pythia.metric.domain;

import com.example.pythia.kafka.dto.MetricStatus;
import com.example.pythia.kafka.dto.SnapshotStatus;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.math.BigDecimal;
import java.time.OffsetDateTime;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "jvm_metric_snapshot")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Builder
public class JvmMetricSnapshotEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, length = 128)
    private String application;

    @Column(nullable = false, length = 128)
    private String instance;

    @Column(nullable = false, columnDefinition = "TIMESTAMP")
    private OffsetDateTime collectedAt;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, columnDefinition = "varchar(16)")
    private SnapshotStatus status;

    // CPU 평탄화
    @Column(precision = 19, scale = 6)
    private BigDecimal cpuUsagePercent;

    @Column(columnDefinition = "TIMESTAMP")
    private OffsetDateTime cpuMeasuredAt;

    @Enumerated(EnumType.STRING)
    @Column(columnDefinition = "varchar(16)")
    private MetricStatus cpuStatus;

    @Column(length = 512)
    private String cpuMissingReason;

    // Memory 평탄화
    @Column(precision = 19, scale = 6)
    private BigDecimal heapUsagePercent;

    @Column(precision = 19, scale = 6)
    private BigDecimal oldGenUsagePercent;

    @Column(columnDefinition = "TIMESTAMP")
    private OffsetDateTime memoryMeasuredAt;

    @Enumerated(EnumType.STRING)
    @Column(columnDefinition = "varchar(16)")
    private MetricStatus memoryStatus;

    @Column(length = 512)
    private String memoryMissingReason;

    // GC 평탄화
    @Column(precision = 19, scale = 6)
    private BigDecimal gcAvgDurationSeconds;

    @Column(precision = 19, scale = 6)
    private BigDecimal gcCount;

    @Column(columnDefinition = "TIMESTAMP")
    private OffsetDateTime gcMeasuredAt;

    @Enumerated(EnumType.STRING)
    @Column(columnDefinition = "varchar(16)")
    private MetricStatus gcStatus;

    @Column(length = 512)
    private String gcMissingReason;

    // Thread 평탄화
    private Integer threadActiveCount;

    private Integer threadPeakCount;

    private Integer threadDaemonCount;

    @Column(columnDefinition = "TIMESTAMP")
    private OffsetDateTime threadMeasuredAt;

    @Enumerated(EnumType.STRING)
    @Column(columnDefinition = "varchar(16)")
    private MetricStatus threadStatus;

    @Column(length = 512)
    private String threadMissingReason;
}
