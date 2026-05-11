package com.example.pythia.alert.domain;

public record ViolationKey(
    MetricKind kind,
    String application,
    String instance,
    String sub) {
}
