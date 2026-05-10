package com.example.argus.dto.metric.hikari;

import com.example.argus.dto.metric.SnapshotStatus;

import java.time.OffsetDateTime;

public record HikariMetricSnapshotDto(
    String application,
    String instance,
    OffsetDateTime collectedAt,
    HikariActiveDto active,
    HikariPendingDto pending,
    SnapshotStatus status) {}
