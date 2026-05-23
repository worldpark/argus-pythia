package com.example.argus.dto.metric.buffer;

public record BufferedSnapshotEnvelope(
        String id,
        long enqueuedAtEpochMs,
        MetricBufferType type,
        String payloadJson) {
}
