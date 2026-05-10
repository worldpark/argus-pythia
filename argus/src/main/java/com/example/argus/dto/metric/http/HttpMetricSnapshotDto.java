package com.example.argus.dto.metric.http;

import com.example.argus.dto.metric.SnapshotStatus;

import java.time.OffsetDateTime;

public record HttpMetricSnapshotDto(
    String application,
    String instance,
    OffsetDateTime collectedAt,
    HttpResponseTimeDto p99,
    HttpThroughputDto rps,
    HttpErrorRateDto errorRate,
    SnapshotStatus status) {}
