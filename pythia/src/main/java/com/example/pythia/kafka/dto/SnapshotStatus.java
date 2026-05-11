package com.example.pythia.kafka.dto;

import java.util.List;

public enum SnapshotStatus {
  COMPLETE,
  PARTIAL,
  FAILED;

  public static SnapshotStatus from(List<MetricStatus> statuses) {
    long hardFailCount = statuses.stream()
        .filter(s -> s == MetricStatus.QUERY_FAILED
                  || s == MetricStatus.PARSE_FAILED
                  || s == MetricStatus.EMPTY_RESULT)
        .count();
    long partialCount = statuses.stream()
        .filter(s -> s == MetricStatus.PARTIAL)
        .count();
    if (hardFailCount == statuses.size()) return FAILED;
    if (hardFailCount == 0 && partialCount == 0) return COMPLETE;
    return PARTIAL;
  }
}
