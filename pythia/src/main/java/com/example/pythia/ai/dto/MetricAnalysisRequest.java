package com.example.pythia.ai.dto;

import java.util.List;

public record MetricAnalysisRequest(
    AnalysisTarget target,
    List<MetricSummary> summaries,
    List<TimeSeriesPoint> timeSeries) {
}
