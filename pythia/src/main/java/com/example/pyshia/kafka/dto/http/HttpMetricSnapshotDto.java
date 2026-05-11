package com.example.pyshia.kafka.dto.http;

import com.example.pyshia.kafka.dto.SnapshotStatus;

import java.time.OffsetDateTime;

public record HttpMetricSnapshotDto(
    String application,
    String instance,
    OffsetDateTime collectedAt,
    HttpResponseTimeDto p99,
    HttpThroughputDto rps,
    HttpErrorRateDto errorRate,
    SnapshotStatus status) {}
