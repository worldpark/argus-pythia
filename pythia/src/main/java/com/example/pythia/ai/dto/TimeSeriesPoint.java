package com.example.pythia.ai.dto;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

public record TimeSeriesPoint(
    OffsetDateTime timestamp,
    String metricName,
    BigDecimal value) {
}
