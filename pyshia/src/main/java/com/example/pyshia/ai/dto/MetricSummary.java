package com.example.pyshia.ai.dto;

import java.math.BigDecimal;

public record MetricSummary(
    String metricName,
    SummaryAggregation aggregation,
    BigDecimal value,
    String unit) {
}
