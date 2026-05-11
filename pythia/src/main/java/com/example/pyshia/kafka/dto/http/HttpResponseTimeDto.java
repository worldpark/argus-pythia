package com.example.pyshia.kafka.dto.http;

import com.example.pyshia.kafka.dto.MetricStatus;

import java.time.OffsetDateTime;
import java.util.List;

public record HttpResponseTimeDto(
    List<EndpointMetricPointDto> points,
    OffsetDateTime measuredAt,
    MetricStatus status,
    String missingReason){
}
