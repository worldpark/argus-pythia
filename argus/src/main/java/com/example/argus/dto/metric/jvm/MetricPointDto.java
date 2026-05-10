package com.example.argus.dto.metric.jvm;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

public record MetricPointDto(
    String application,
    String instance,
    OffsetDateTime timestamp,
    BigDecimal value) {}
