package com.example.pythia.kafka.dto.hikari;

import com.example.pythia.kafka.dto.MetricStatus;

import java.time.OffsetDateTime;
import java.util.List;

public record HikariPendingDto(
    List<PoolMetricPointDto> points,
    OffsetDateTime measuredAt,
    MetricStatus status,
    String missingReason){
}
