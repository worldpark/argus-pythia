package com.example.pyshia.kafka.dto.hikari;

import com.example.pyshia.kafka.dto.MetricStatus;

import java.time.OffsetDateTime;
import java.util.List;

public record HikariActiveDto(
    List<PoolMetricPointDto> points,
    List<PoolMetricPointDto> usageRatio,
    OffsetDateTime measuredAt,
    MetricStatus status,
    String missingReason){
}
