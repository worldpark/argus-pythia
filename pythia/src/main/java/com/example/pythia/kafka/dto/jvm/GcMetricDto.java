package com.example.pythia.kafka.dto.jvm;

import com.example.pythia.kafka.dto.MetricStatus;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

public record GcMetricDto(
    BigDecimal avgDurationSeconds,
    BigDecimal count,
    OffsetDateTime measuredAt,
    MetricStatus status,
    String missingReason){
}
