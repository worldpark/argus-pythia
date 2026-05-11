package com.example.pythia.kafka.dto.hikari;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

public record PoolMetricPointDto(
    String pool,
    BigDecimal value,
    OffsetDateTime measuredAt) {}
