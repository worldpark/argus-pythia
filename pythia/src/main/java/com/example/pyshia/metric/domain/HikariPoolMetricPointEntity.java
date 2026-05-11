package com.example.pyshia.metric.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import java.math.BigDecimal;
import java.time.OffsetDateTime;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "hikari_pool_metric_point")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Builder(access = AccessLevel.PRIVATE)
public class HikariPoolMetricPointEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "snapshot_id", nullable = false, updatable = false)
    private HikariMetricSnapshotEntity snapshot;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, columnDefinition = "varchar(16)")
    private HikariMetricKind kind;

    @Column(length = 256)
    private String pool;

    @Column(precision = 19, scale = 6)
    private BigDecimal value;

    @Column(columnDefinition = "TIMESTAMP")
    private OffsetDateTime measuredAt;

    /**
     * 정적 팩토리 메서드 — addPoint()를 통해 snapshot 연관관계를 반드시 거치도록 강제.
     * builder를 private으로 제한하여 외부에서 snapshot 없이 직접 영속화하는 경로를 차단.
     */
    public static HikariPoolMetricPointEntity of(
        HikariMetricKind kind, String pool, BigDecimal value, OffsetDateTime measuredAt) {
        return HikariPoolMetricPointEntity.builder()
            .kind(kind)
            .pool(pool)
            .value(value)
            .measuredAt(measuredAt)
            .build();
    }

    /** 연관관계 편의 메서드 — 부모 Entity 내부에서만 호출 */
    void assignSnapshot(HikariMetricSnapshotEntity snapshotEntity) {
        this.snapshot = snapshotEntity;
    }
}
