package com.example.argus.service.metric.buffer;

import com.example.argus.config.MetricBufferProperties;
import com.example.argus.dto.metric.buffer.BufferedSnapshotEnvelope;
import com.example.argus.dto.metric.buffer.MetricBufferType;
import com.example.argus.dto.metric.hikari.HikariMetricSnapshotDto;
import com.example.argus.dto.metric.http.HttpMetricSnapshotDto;
import com.example.argus.dto.metric.jvm.JvmMetricSnapshotDto;
import com.example.argus.exception.MetricBufferException;
import com.example.argus.messaging.HikariMetricSnapshotProducer;
import com.example.argus.messaging.HttpMetricSnapshotProducer;
import com.example.argus.messaging.JvmMetricSnapshotProducer;
import java.time.Instant;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Service;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

@Service
@RequiredArgsConstructor
@Slf4j
public class MetricBufferDrainService {

    private final MetricBufferStore store;
    private final MetricBufferProperties properties;
    private final ObjectMapper objectMapper;
    private final JvmMetricSnapshotProducer jvmProducer;
    private final HttpMetricSnapshotProducer httpProducer;
    private final HikariMetricSnapshotProducer hikariProducer;

    public void drainAll() {
        for (MetricBufferType type : MetricBufferType.values()) {
            try {
                drainType(type);
            } catch (DataAccessException | MetricBufferException e) {
                log.error("metric-buffer: drain cycle failed for type={}", type, e);
            }
        }
    }

    private void drainType(MetricBufferType type) {
        long nowMs = Instant.now().toEpochMilli();
        long ttlMs = properties.getTtl().toMillis();
        store.evictExpired(type, nowMs - ttlMs);

        List<String> rawMembers = store.peekOldest(type, properties.getDrainBatchSize());
        for (String rawMember : rawMembers) {
            processMember(type, rawMember);
        }
    }

    private void processMember(MetricBufferType type, String rawMember) {
        BufferedSnapshotEnvelope envelope;
        try {
            envelope = objectMapper.readValue(rawMember, BufferedSnapshotEnvelope.class);
        } catch (JacksonException e) {
            log.warn("metric-buffer: poison-pill detected, removing immediately. type={}", type, e);
            store.remove(type, rawMember);
            return;
        }

        try {
            sendByType(type, envelope, rawMember);
        } catch (JacksonException e) {
            log.warn("metric-buffer: payload deserialization failed, removing poison-pill. type={}", type, e);
            store.remove(type, rawMember);
        }
    }

    private void sendByType(MetricBufferType type, BufferedSnapshotEnvelope envelope, String rawMember)
            throws JacksonException {
        switch (type) {
            case JVM -> {
                JvmMetricSnapshotDto dto =
                        objectMapper.readValue(envelope.payloadJson(), JvmMetricSnapshotDto.class);
                String serviceId = dto.application() != null ? dto.application() : "unknown";
                jvmProducer.send(serviceId, dto).whenComplete((result, ex) -> {
                    if (ex == null) {
                        store.remove(type, rawMember);
                        log.debug("metric-buffer: drain ack success, removed from buffer. type={}", type);
                    } else {
                        log.warn("metric-buffer: drain send failed, will retry next cycle. type={}", type, ex);
                    }
                });
            }
            case HTTP -> {
                HttpMetricSnapshotDto dto =
                        objectMapper.readValue(envelope.payloadJson(), HttpMetricSnapshotDto.class);
                String serviceId = dto.application() != null ? dto.application() : "unknown";
                httpProducer.send(serviceId, dto).whenComplete((result, ex) -> {
                    if (ex == null) {
                        store.remove(type, rawMember);
                        log.debug("metric-buffer: drain ack success, removed from buffer. type={}", type);
                    } else {
                        log.warn("metric-buffer: drain send failed, will retry next cycle. type={}", type, ex);
                    }
                });
            }
            case HIKARI -> {
                HikariMetricSnapshotDto dto =
                        objectMapper.readValue(envelope.payloadJson(), HikariMetricSnapshotDto.class);
                String serviceId = dto.application() != null ? dto.application() : "unknown";
                hikariProducer.send(serviceId, dto).whenComplete((result, ex) -> {
                    if (ex == null) {
                        store.remove(type, rawMember);
                        log.debug("metric-buffer: drain ack success, removed from buffer. type={}", type);
                    } else {
                        log.warn("metric-buffer: drain send failed, will retry next cycle. type={}", type, ex);
                    }
                });
            }
        }
    }
}
