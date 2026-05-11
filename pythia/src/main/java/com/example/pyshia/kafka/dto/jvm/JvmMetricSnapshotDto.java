package com.example.pyshia.kafka.dto.jvm;


import com.example.pyshia.kafka.dto.SnapshotStatus;

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
