package com.example.pythia.metric.domain;

import com.example.pythia.kafka.dto.MetricStatus;
import com.example.pythia.kafka.dto.SnapshotStatus;
import jakarta.persistence.CascadeType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.OneToMany;
import jakarta.persistence.Table;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "http_metric_snapshot", indexes = {
    @Index(name = "idx_http_metric_app_inst_collected_at",
        columnList = "application, instance, collected_at")
})
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Builder
public class HttpMetricSnapshotEntity {

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

    // p99 메타
    @Column(columnDefinition = "TIMESTAMP")
    private OffsetDateTime p99MeasuredAt;

    @Enumerated(EnumType.STRING)
    @Column(columnDefinition = "varchar(16)")
    private MetricStatus p99Status;

    @Column(length = 512)
    private String p99MissingReason;

    // rps 메타
    @Column(columnDefinition = "TIMESTAMP")
    private OffsetDateTime rpsMeasuredAt;

    @Enumerated(EnumType.STRING)
    @Column(columnDefinition = "varchar(16)")
    private MetricStatus rpsStatus;

    @Column(length = 512)
    private String rpsMissingReason;

    // errorRate 메타
    @Column(columnDefinition = "TIMESTAMP")
    private OffsetDateTime errorRateMeasuredAt;

    @Enumerated(EnumType.STRING)
    @Column(columnDefinition = "varchar(16)")
    private MetricStatus errorRateStatus;

    @Column(length = 512)
    private String errorRateMissingReason;

    // 양방향 @OneToMany — FK(snapshot_id)는 자식 @ManyToOne 측에서 관리 (NOT NULL)
    @OneToMany(mappedBy = "snapshot", cascade = CascadeType.PERSIST, fetch = FetchType.LAZY, orphanRemoval = false)
    @Builder.Default
    private List<HttpEndpointMetricPointEntity> points = new ArrayList<>();

    /** 자식 포인트를 추가하면서 양방향 연관관계를 동기화합니다. */
    public void addPoint(HttpEndpointMetricPointEntity point) {
        point.assignSnapshot(this);
        this.points.add(point);
    }
}
