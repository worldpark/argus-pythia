package com.example.argus.dto.metric.jvm;

import com.example.argus.dto.metric.SnapshotStatus;

import java.time.OffsetDateTime;

public record JvmMetricSnapshotDto(
    String application,
    String instance,
    OffsetDateTime collectedAt,
    CpuUsageDto cpu,
    MemoryUsageDto memory,
    GcMetricDto gc,
    ThreadMetricDto thread,
    SnapshotStatus status) {}
