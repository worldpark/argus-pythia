package com.example.pythia.kafka.dto.hikari;

import com.example.pythia.kafka.dto.MetricStatus;

import java.time.OffsetDateTime;
import java.util.List;

public record HikariActiveDto(
    List<PoolMetricPointDto> points,
    List<PoolMetricPointDto> usageRatio,
    OffsetDateTime measuredAt,
    MetricStatus status,
    String missingReason){
}
