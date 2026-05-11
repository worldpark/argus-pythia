package com.example.pyshia.kafka.dto.jvm;

import com.example.pyshia.kafka.dto.MetricStatus;

import java.time.OffsetDateTime;

public record ThreadMetricDto(
    Integer activeCount,
    Integer peakCount,
    Integer daemonCount,
    OffsetDateTime measuredAt,
    MetricStatus status,
    String missingReason){
}
