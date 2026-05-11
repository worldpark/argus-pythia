package com.example.pythia.kafka.dto.jvm;

import com.example.pythia.kafka.dto.MetricStatus;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

public record MemoryUsageDto(
    BigDecimal heapUsagePercent,
    BigDecimal oldGenUsagePercent,
    OffsetDateTime measuredAt,
    MetricStatus status,
    String missingReason){
}
