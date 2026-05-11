package com.example.pyshia.ai.dto;

import java.time.Duration;

public record AnalysisTarget(
    String application,
    String instance,
    Duration range) {
}
