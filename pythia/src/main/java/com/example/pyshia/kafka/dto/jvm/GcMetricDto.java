package com.example.pyshia.kafka.dto.jvm;

import com.example.pyshia.kafka.dto.MetricStatus;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

public record GcMetricDto(
    BigDecimal avgDurationSeconds,
    BigDecimal count,
    OffsetDateTime measuredAt,
    MetricStatus status,
    String missingReason){
}
