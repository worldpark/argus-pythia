package com.example.argus.service.metric.buffer;

import com.example.argus.config.MetricBufferProperties;
import com.example.argus.config.MetricBufferProperties.OverflowPolicy;
import com.example.argus.dto.metric.buffer.BufferedSnapshotEnvelope;
import com.example.argus.dto.metric.buffer.MetricBufferType;
import com.example.argus.exception.MetricBufferException;
import java.time.Instant;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Service;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

@Service
@RequiredArgsConstructor
@Slf4j
public class MetricBufferService {

    private final MetricBufferStore store;
    private final MetricBufferProperties properties;
    private final ObjectMapper objectMapper;

    public void enqueueOnFailure(MetricBufferType type, Object snapshot) {
        long nowMs = Instant.now().toEpochMilli();
        try {
            String payloadJson = objectMapper.writeValueAsString(snapshot);
            BufferedSnapshotEnvelope envelope = new BufferedSnapshotEnvelope(
                    UUID.randomUUID().toString(),
                    nowMs,
                    type,
                    payloadJson
            );
            String envelopeJson = objectMapper.writeValueAsString(envelope);

            try {
                store.enqueue(type, envelopeJson, nowMs);
                applyOverflowPolicy(type, envelopeJson);
            } catch (DataAccessException e) {
                throw new MetricBufferException("Redis ZADD failed for type=" + type, e);
            }

        } catch (JacksonException e) {
            throw new MetricBufferException("serialization failed for type=" + type, e);
        }
    }

    private void applyOverflowPolicy(MetricBufferType type, String envelopeJson) {
        long currentSize = store.size(type);
        int maxSize = properties.getMaxSize();
        if (currentSize <= maxSize) {
            return;
        }

        OverflowPolicy policy = properties.getOverflowPolicy();
        switch (policy) {
            case DROP_OLDEST -> {
                long toDrop = currentSize - maxSize;
                store.dropOldest(type, toDrop);
                log.warn("metric-buffer: overflow DROP_OLDEST dropped={} for type={}", toDrop, type);
            }
            case DROP_NEWEST -> {
                store.dropNewest(type, envelopeJson);
                log.warn("metric-buffer: overflow DROP_NEWEST dropped newest entry for type={}", type);
            }
            case REJECT -> throw new MetricBufferException(
                    "overflow REJECT: buffer full for type=" + type + " size=" + currentSize);
        }
    }

}
