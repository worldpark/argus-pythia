package com.example.pythia.kafka.dto.http;

import com.example.pythia.kafka.dto.SnapshotStatus;

import java.time.OffsetDateTime;

public record HttpMetricSnapshotDto(
    String application,
    String instance,
    OffsetDateTime collectedAt,
    HttpResponseTimeDto p99,
    HttpThroughputDto rps,
    HttpErrorRateDto errorRate,
    SnapshotStatus status) {}
