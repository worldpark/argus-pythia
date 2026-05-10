package com.example.pyshia.kafka.dto.hikari;


import com.example.pyshia.kafka.dto.SnapshotStatus;

import java.time.OffsetDateTime;

public record HikariMetricSnapshotDto(
    String application,
    String instance,
    OffsetDateTime collectedAt,
    HikariActiveDto active,
    HikariPendingDto pending,
    SnapshotStatus status) {}
