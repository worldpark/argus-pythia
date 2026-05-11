package com.example.pythia.kafka.dto.jvm;

import com.example.pythia.kafka.dto.MetricStatus;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

public record CpuUsageDto(
    BigDecimal usagePercent,
    OffsetDateTime measuredAt,
    MetricStatus status,
    String missingReason){
}
