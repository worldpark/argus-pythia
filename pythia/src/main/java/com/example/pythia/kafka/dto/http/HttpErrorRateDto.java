package com.example.pythia.kafka.dto.http;

import com.example.pythia.kafka.dto.MetricStatus;

import java.time.OffsetDateTime;
import java.util.List;

public record HttpErrorRateDto(
    List<EndpointMetricPointDto> points,
    OffsetDateTime measuredAt,
    MetricStatus status,
    String missingReason){
}
