package com.example.pythia.ai.dto;

import java.time.Duration;

public record AnalysisTarget(
    String application,
    String instance,
    Duration range) {
}
