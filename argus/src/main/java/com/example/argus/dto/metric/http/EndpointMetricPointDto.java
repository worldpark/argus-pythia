package com.example.argus.dto.metric.http;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

public record EndpointMetricPointDto(
    String endpoint,
    BigDecimal value,
    OffsetDateTime measuredAt) {}
