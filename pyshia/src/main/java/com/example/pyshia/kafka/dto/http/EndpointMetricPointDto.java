package com.example.pyshia.kafka.dto.http;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

public record EndpointMetricPointDto(
    String endpoint,
    BigDecimal value,
    OffsetDateTime measuredAt) {}
