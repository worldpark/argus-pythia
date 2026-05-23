package com.example.argus.service.metric.buffer;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.example.argus.config.MetricBufferProperties;
import com.example.argus.config.MetricBufferProperties.OverflowPolicy;
import com.example.argus.dto.metric.buffer.MetricBufferType;
import com.example.argus.dto.metric.jvm.JvmMetricSnapshotDto;
import com.example.argus.exception.MetricBufferException;
import java.time.Duration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.QueryTimeoutException;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class MetricBufferServiceTest {

    @Mock private MetricBufferStore store;
    @Mock private MetricBufferProperties properties;
    @Mock private ObjectMapper objectMapper;

    @InjectMocks private MetricBufferService service;

    @BeforeEach
    void setUp() {
        lenient().when(properties.getMaxSize()).thenReturn(5000);
        lenient().when(properties.getOverflowPolicy()).thenReturn(OverflowPolicy.DROP_OLDEST);
        lenient().when(properties.getTtl()).thenReturn(Duration.ofMinutes(5));
    }

    @Test
    @DisplayName("정상 enqueueOnFailure 는 store.enqueue 를 호출한다")
    void enqueueOnFailure_정상_흐름에서_store_enqueue_호출() throws Exception {
        JvmMetricSnapshotDto snapshot = createJvmSnapshot("svc-A");
        when(objectMapper.writeValueAsString(snapshot)).thenReturn("{\"application\":\"svc-A\"}");
        when(objectMapper.writeValueAsString(any())).thenReturn("{\"envelope\":\"data\"}");
        when(store.size(MetricBufferType.JVM)).thenReturn(100L);

        service.enqueueOnFailure(MetricBufferType.JVM, snapshot);

        verify(store).enqueue(eq(MetricBufferType.JVM), anyString(), anyDouble());
    }

    @Test
    @DisplayName("Jackson 직렬화 실패 시 MetricBufferException 을 던진다")
    void enqueueOnFailure_직렬화_실패시_MetricBufferException_발생() throws Exception {
        JvmMetricSnapshotDto snapshot = createJvmSnapshot("svc-A");
        when(objectMapper.writeValueAsString(snapshot)).thenThrow(new JacksonException("error") {});

        assertThatThrownBy(() -> service.enqueueOnFailure(MetricBufferType.JVM, snapshot))
                .isInstanceOf(MetricBufferException.class)
                .hasMessageContaining("metric-buffer:");

        verify(store, never()).enqueue(any(), anyString(), anyDouble());
    }

    @Test
    @DisplayName("Redis DataAccessException 은 MetricBufferException 으로 래핑한다")
    void enqueueOnFailure_Redis_DataAccessException을_MetricBufferException으로_래핑() throws Exception {
        JvmMetricSnapshotDto snapshot = createJvmSnapshot("svc-A");
        when(objectMapper.writeValueAsString(snapshot)).thenReturn("{\"application\":\"svc-A\"}");
        when(objectMapper.writeValueAsString(any())).thenReturn("{\"envelope\":\"data\"}");
        DataAccessException redisEx = new QueryTimeoutException("Redis timeout");
        doThrow(redisEx).when(store).enqueue(any(), anyString(), anyDouble());

        assertThatThrownBy(() -> service.enqueueOnFailure(MetricBufferType.JVM, snapshot))
                .isInstanceOf(MetricBufferException.class)
                .hasMessageContaining("metric-buffer:")
                .hasCause(redisEx);
    }

    @Test
    @DisplayName("overflow REJECT 정책 시 MetricBufferException 을 던진다")
    void enqueueOnFailure_REJECT_정책에서_예외_발생() throws Exception {
        when(properties.getOverflowPolicy()).thenReturn(OverflowPolicy.REJECT);
        JvmMetricSnapshotDto snapshot = createJvmSnapshot("svc-A");
        when(objectMapper.writeValueAsString(snapshot)).thenReturn("{\"application\":\"svc-A\"}");
        when(objectMapper.writeValueAsString(any())).thenReturn("{\"envelope\":\"data\"}");
        when(store.size(MetricBufferType.JVM)).thenReturn(6000L);

        assertThatThrownBy(() -> service.enqueueOnFailure(MetricBufferType.JVM, snapshot))
                .isInstanceOf(MetricBufferException.class)
                .hasMessageContaining("overflow REJECT");
    }

    @Test
    @DisplayName("overflow DROP_OLDEST 정책 시 store.dropOldest 를 호출한다")
    void enqueueOnFailure_DROP_OLDEST_정책에서_dropOldest_호출() throws Exception {
        JvmMetricSnapshotDto snapshot = createJvmSnapshot("svc-A");
        when(objectMapper.writeValueAsString(snapshot)).thenReturn("{\"application\":\"svc-A\"}");
        when(objectMapper.writeValueAsString(any())).thenReturn("{\"envelope\":\"data\"}");
        when(store.size(MetricBufferType.JVM)).thenReturn(5005L);

        service.enqueueOnFailure(MetricBufferType.JVM, snapshot);

        verify(store).dropOldest(eq(MetricBufferType.JVM), eq(5L));
    }

    @Test
    @DisplayName("overflow DROP_NEWEST 정책 시 store.dropNewest 를 호출한다")
    void enqueueOnFailure_DROP_NEWEST_정책에서_dropNewest_호출() throws Exception {
        when(properties.getOverflowPolicy()).thenReturn(OverflowPolicy.DROP_NEWEST);
        JvmMetricSnapshotDto snapshot = createJvmSnapshot("svc-A");
        String envelopeJson = "{\"envelope\":\"data\"}";
        when(objectMapper.writeValueAsString(snapshot)).thenReturn("{\"application\":\"svc-A\"}");
        when(objectMapper.writeValueAsString(any())).thenReturn(envelopeJson);
        when(store.size(MetricBufferType.JVM)).thenReturn(5001L);

        service.enqueueOnFailure(MetricBufferType.JVM, snapshot);

        verify(store).dropNewest(eq(MetricBufferType.JVM), eq(envelopeJson));
    }

    private JvmMetricSnapshotDto createJvmSnapshot(String application) {
        return new JvmMetricSnapshotDto(application, null, null, null, null, null, null, null);
    }
}
