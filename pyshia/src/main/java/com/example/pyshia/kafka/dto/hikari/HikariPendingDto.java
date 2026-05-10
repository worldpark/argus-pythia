package com.example.pyshia.kafka.dto.hikari;

import com.example.pyshia.kafka.dto.MetricStatus;

import java.time.OffsetDateTime;
import java.util.List;

public record HikariPendingDto(
    List<PoolMetricPointDto> points,
    OffsetDateTime measuredAt,
    MetricStatus status,
    String missingReason){
}
