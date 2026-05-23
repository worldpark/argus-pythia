package com.example.argus.service.metric.buffer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.example.argus.config.MetricBufferProperties;
import com.example.argus.dto.metric.buffer.BufferedSnapshotEnvelope;
import com.example.argus.dto.metric.buffer.MetricBufferType;
import com.example.argus.dto.metric.hikari.HikariMetricSnapshotDto;
import com.example.argus.dto.metric.http.HttpMetricSnapshotDto;
import com.example.argus.dto.metric.jvm.JvmMetricSnapshotDto;
import com.example.argus.messaging.HikariMetricSnapshotProducer;
import com.example.argus.messaging.HttpMetricSnapshotProducer;
import com.example.argus.messaging.JvmMetricSnapshotProducer;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.SendResult;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

@ExtendWith(MockitoExtension.class)
class MetricBufferDrainServiceTest {

    @Mock private MetricBufferStore store;
    @Mock private MetricBufferProperties properties;
    @Mock private ObjectMapper objectMapper;
    @Mock private JvmMetricSnapshotProducer jvmProducer;
    @Mock private HttpMetricSnapshotProducer httpProducer;
    @Mock private HikariMetricSnapshotProducer hikariProducer;

    @InjectMocks private MetricBufferDrainService drainService;

    @BeforeEach
    void setUp() {
        lenient().when(properties.getTtl()).thenReturn(Duration.ofMinutes(5));
        lenient().when(properties.getDrainBatchSize()).thenReturn(50);
    }

    @Test
    @DisplayName("drainAll 에서 evictExpired 가 peekOldest 보다 먼저 호출된다")
    void drainAll_evictExpired가_peekOldest보다_먼저_호출된다() throws Exception {
        when(store.peekOldest(any(), eq(50))).thenReturn(Collections.emptyList());

        drainService.drainAll();

        InOrder inOrder = inOrder(store);
        inOrder.verify(store).evictExpired(eq(MetricBufferType.JVM), anyDouble());
        inOrder.verify(store).peekOldest(eq(MetricBufferType.JVM), eq(50));
    }

    @Test
    @DisplayName("Kafka ack 성공 시 store.remove 가 호출된다")
    void drainAll_kafka_ack_성공시_remove_호출() throws Exception {
        String rawMember = "{\"id\":\"123\",\"enqueuedAtEpochMs\":1000,\"type\":\"JVM\",\"payloadJson\":\"{}\"}";
        JvmMetricSnapshotDto dto = new JvmMetricSnapshotDto("svc-A", null, null, null, null, null, null, null);
        BufferedSnapshotEnvelope envelope = new BufferedSnapshotEnvelope("123", 1000L, MetricBufferType.JVM, "{}");

        when(store.peekOldest(eq(MetricBufferType.JVM), eq(50))).thenReturn(List.of(rawMember));
        when(store.peekOldest(eq(MetricBufferType.HTTP), eq(50))).thenReturn(Collections.emptyList());
        when(store.peekOldest(eq(MetricBufferType.HIKARI), eq(50))).thenReturn(Collections.emptyList());
        when(objectMapper.readValue(rawMember, BufferedSnapshotEnvelope.class)).thenReturn(envelope);
        when(objectMapper.readValue("{}", JvmMetricSnapshotDto.class)).thenReturn(dto);

        CompletableFuture<SendResult<String, JvmMetricSnapshotDto>> successFuture = new CompletableFuture<>();
        when(jvmProducer.send("svc-A", dto)).thenReturn(successFuture);

        drainService.drainAll();
        successFuture.complete(null);

        verify(store).remove(MetricBufferType.JVM, rawMember);
    }

    @Test
    @DisplayName("Kafka ack 실패 시 store.remove 가 호출되지 않는다")
    void drainAll_kafka_ack_실패시_remove_미호출() throws Exception {
        String rawMember = "{\"id\":\"456\",\"enqueuedAtEpochMs\":2000,\"type\":\"JVM\",\"payloadJson\":\"{}\"}";
        JvmMetricSnapshotDto dto = new JvmMetricSnapshotDto("svc-B", null, null, null, null, null, null, null);
        BufferedSnapshotEnvelope envelope = new BufferedSnapshotEnvelope("456", 2000L, MetricBufferType.JVM, "{}");

        when(store.peekOldest(eq(MetricBufferType.JVM), eq(50))).thenReturn(List.of(rawMember));
        when(store.peekOldest(eq(MetricBufferType.HTTP), eq(50))).thenReturn(Collections.emptyList());
        when(store.peekOldest(eq(MetricBufferType.HIKARI), eq(50))).thenReturn(Collections.emptyList());
        when(objectMapper.readValue(rawMember, BufferedSnapshotEnvelope.class)).thenReturn(envelope);
        when(objectMapper.readValue("{}", JvmMetricSnapshotDto.class)).thenReturn(dto);

        CompletableFuture<SendResult<String, JvmMetricSnapshotDto>> failFuture = new CompletableFuture<>();
        when(jvmProducer.send("svc-B", dto)).thenReturn(failFuture);

        drainService.drainAll();
        failFuture.completeExceptionally(new RuntimeException("Kafka broker unavailable"));

        verify(store, never()).remove(eq(MetricBufferType.JVM), anyString());
    }

    @Test
    @DisplayName("envelope 역직렬화 실패 시 즉시 remove 하고 처리를 계속한다 (poison-pill)")
    void drainAll_envelope_역직렬화_실패시_즉시_remove() throws Exception {
        String rawMember = "invalid-json";

        when(store.peekOldest(eq(MetricBufferType.JVM), eq(50))).thenReturn(List.of(rawMember));
        when(store.peekOldest(eq(MetricBufferType.HTTP), eq(50))).thenReturn(Collections.emptyList());
        when(store.peekOldest(eq(MetricBufferType.HIKARI), eq(50))).thenReturn(Collections.emptyList());
        when(objectMapper.readValue(rawMember, BufferedSnapshotEnvelope.class))
                .thenThrow(new JacksonException("invalid") {});

        drainService.drainAll();

        verify(store).remove(MetricBufferType.JVM, rawMember);
        verify(jvmProducer, never()).send(anyString(), any());
    }

    @Test
    @DisplayName("payload 역직렬화 실패 시 즉시 remove 하고 처리를 계속한다 (poison-pill)")
    void drainAll_payload_역직렬화_실패시_즉시_remove() throws Exception {
        String rawMember = "{\"id\":\"789\",\"enqueuedAtEpochMs\":3000,\"type\":\"JVM\",\"payloadJson\":\"bad\"}";
        BufferedSnapshotEnvelope envelope = new BufferedSnapshotEnvelope("789", 3000L, MetricBufferType.JVM, "bad");

        when(store.peekOldest(eq(MetricBufferType.JVM), eq(50))).thenReturn(List.of(rawMember));
        when(store.peekOldest(eq(MetricBufferType.HTTP), eq(50))).thenReturn(Collections.emptyList());
        when(store.peekOldest(eq(MetricBufferType.HIKARI), eq(50))).thenReturn(Collections.emptyList());
        when(objectMapper.readValue(rawMember, BufferedSnapshotEnvelope.class)).thenReturn(envelope);
        when(objectMapper.readValue("bad", JvmMetricSnapshotDto.class))
                .thenThrow(new JacksonException("bad payload") {});

        drainService.drainAll();

        verify(store).remove(MetricBufferType.JVM, rawMember);
        verify(jvmProducer, never()).send(anyString(), any());
    }

    @Test
    @DisplayName("HTTP 타입 drain 에서 Kafka ack 성공 시 store.remove 가 호출된다")
    void drainAll_HTTP_ack_성공시_remove_호출() throws Exception {
        String rawMember = "{\"id\":\"http-1\",\"enqueuedAtEpochMs\":1000,\"type\":\"HTTP\",\"payloadJson\":\"{}\"}";
        HttpMetricSnapshotDto dto = new HttpMetricSnapshotDto("svc-C", null, null, null, null, null, null);
        BufferedSnapshotEnvelope envelope = new BufferedSnapshotEnvelope("http-1", 1000L, MetricBufferType.HTTP, "{}");

        when(store.peekOldest(eq(MetricBufferType.JVM), eq(50))).thenReturn(Collections.emptyList());
        when(store.peekOldest(eq(MetricBufferType.HTTP), eq(50))).thenReturn(List.of(rawMember));
        when(store.peekOldest(eq(MetricBufferType.HIKARI), eq(50))).thenReturn(Collections.emptyList());
        when(objectMapper.readValue(rawMember, BufferedSnapshotEnvelope.class)).thenReturn(envelope);
        when(objectMapper.readValue("{}", HttpMetricSnapshotDto.class)).thenReturn(dto);

        CompletableFuture<SendResult<String, HttpMetricSnapshotDto>> successFuture = new CompletableFuture<>();
        when(httpProducer.send("svc-C", dto)).thenReturn(successFuture);

        drainService.drainAll();
        successFuture.complete(null);

        verify(store).remove(MetricBufferType.HTTP, rawMember);
    }

    @Test
    @DisplayName("HIKARI 타입 drain 에서 Kafka ack 성공 시 store.remove 가 호출된다")
    void drainAll_HIKARI_ack_성공시_remove_호출() throws Exception {
        String rawMember = "{\"id\":\"hikari-1\",\"enqueuedAtEpochMs\":1000,\"type\":\"HIKARI\",\"payloadJson\":\"{}\"}";
        HikariMetricSnapshotDto dto = new HikariMetricSnapshotDto("svc-D", null, null, null, null, null);
        BufferedSnapshotEnvelope envelope =
                new BufferedSnapshotEnvelope("hikari-1", 1000L, MetricBufferType.HIKARI, "{}");

        when(store.peekOldest(eq(MetricBufferType.JVM), eq(50))).thenReturn(Collections.emptyList());
        when(store.peekOldest(eq(MetricBufferType.HTTP), eq(50))).thenReturn(Collections.emptyList());
        when(store.peekOldest(eq(MetricBufferType.HIKARI), eq(50))).thenReturn(List.of(rawMember));
        when(objectMapper.readValue(rawMember, BufferedSnapshotEnvelope.class)).thenReturn(envelope);
        when(objectMapper.readValue("{}", HikariMetricSnapshotDto.class)).thenReturn(dto);

        CompletableFuture<SendResult<String, HikariMetricSnapshotDto>> successFuture = new CompletableFuture<>();
        when(hikariProducer.send("svc-D", dto)).thenReturn(successFuture);

        drainService.drainAll();
        successFuture.complete(null);

        verify(store).remove(MetricBufferType.HIKARI, rawMember);
    }
}
