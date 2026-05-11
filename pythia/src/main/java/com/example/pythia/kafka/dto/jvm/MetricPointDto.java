package com.example.pythia.kafka.dto.jvm;

import java.math.BigDecimal;
import java.time.Instant;

public record MetricPointDto(
    String application,
    String instance,
    Instant timestamp,
    BigDecimal value) {}
