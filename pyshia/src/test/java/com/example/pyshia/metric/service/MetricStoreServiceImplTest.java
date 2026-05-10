package com.example.pyshia.metric.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentCaptor.forClass;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.example.pyshia.kafka.dto.MetricStatus;
import com.example.pyshia.kafka.dto.SnapshotStatus;
import com.example.pyshia.kafka.dto.hikari.HikariActiveDto;
import com.example.pyshia.kafka.dto.hikari.HikariMetricSnapshotDto;
import com.example.pyshia.kafka.dto.hikari.HikariPendingDto;
import com.example.pyshia.kafka.dto.hikari.PoolMetricPointDto;
import com.example.pyshia.kafka.dto.http.EndpointMetricPointDto;
import com.example.pyshia.kafka.dto.http.HttpErrorRateDto;
import com.example.pyshia.kafka.dto.http.HttpMetricSnapshotDto;
import com.example.pyshia.kafka.dto.http.HttpResponseTimeDto;
import com.example.pyshia.kafka.dto.http.HttpThroughputDto;
import com.example.pyshia.kafka.dto.jvm.CpuUsageDto;
import com.example.pyshia.kafka.dto.jvm.GcMetricDto;
import com.example.pyshia.kafka.dto.jvm.JvmMetricSnapshotDto;
import com.example.pyshia.kafka.dto.jvm.MemoryUsageDto;
import com.example.pyshia.kafka.dto.jvm.ThreadMetricDto;
import com.example.pyshia.metric.domain.HikariMetricKind;
import com.example.pyshia.metric.domain.HikariMetricSnapshotEntity;
import com.example.pyshia.metric.domain.HttpMetricKind;
import com.example.pyshia.metric.domain.HttpMetricSnapshotEntity;
import com.example.pyshia.metric.domain.JvmMetricSnapshotEntity;
import com.example.pyshia.metric.exception.MetricStoreErrorCode;
import com.example.pyshia.metric.exception.MetricStoreException;
import com.example.pyshia.metric.repository.HikariMetricSnapshotRepository;
import com.example.pyshia.metric.repository.HttpMetricSnapshotRepository;
import com.example.pyshia.metric.repository.JvmMetricSnapshotRepository;
import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataIntegrityViolationException;

@ExtendWith(MockitoExtension.class)
class MetricStoreServiceImplTest {

    @Mock
    private JvmMetricSnapshotRepository jvmMetricSnapshotRepository;

    @Mock
    private HttpMetricSnapshotRepository httpMetricSnapshotRepository;

    @Mock
    private HikariMetricSnapshotRepository hikariMetricSnapshotRepository;

    @InjectMocks
    private MetricStoreServiceImpl metricStoreService;

    // ===== JVM =====

    @Test
    @DisplayName("JVM 정상 스냅샷 저장 시 Repository.save 1회 호출 및 entity 필드 매핑 검증")
    void jvm_정상_스냅샷_저장시_repository_save_1회_호출() {
        JvmMetricSnapshotDto snapshot = normalJvmSnapshot();
        when(jvmMetricSnapshotRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        metricStoreService.save(snapshot);

        ArgumentCaptor<JvmMetricSnapshotEntity> captor = forClass(JvmMetricSnapshotEntity.class);
        verify(jvmMetricSnapshotRepository, times(1)).save(captor.capture());

        JvmMetricSnapshotEntity entity = captor.getValue();
        assertThat(entity.getApplication()).isEqualTo("argus");
        assertThat(entity.getInstance()).isEqualTo("localhost:8080");
        assertThat(entity.getStatus()).isEqualTo(SnapshotStatus.COMPLETE);
        assertThat(entity.getCpuUsagePercent()).isEqualByComparingTo(BigDecimal.valueOf(50.0));
        assertThat(entity.getHeapUsagePercent()).isEqualByComparingTo(BigDecimal.valueOf(60.0));
        assertThat(entity.getOldGenUsagePercent()).isEqualByComparingTo(BigDecimal.valueOf(40.0));
        assertThat(entity.getGcAvgDurationSeconds()).isEqualByComparingTo(BigDecimal.valueOf(0.01));
        assertThat(entity.getGcCount()).isEqualByComparingTo(BigDecimal.valueOf(5));
        assertThat(entity.getThreadActiveCount()).isEqualTo(20);
        assertThat(entity.getThreadPeakCount()).isEqualTo(25);
        assertThat(entity.getThreadDaemonCount()).isEqualTo(15);
    }

    @Test
    @DisplayName("JVM application null 시 MetricStoreException(INVALID_SNAPSHOT_PAYLOAD) throw")
    void jvm_application_null_시_예외_throw() {
        JvmMetricSnapshotDto snapshot = new JvmMetricSnapshotDto(
            null, "localhost:8080", OffsetDateTime.now(),
            new CpuUsageDto(BigDecimal.valueOf(50), OffsetDateTime.now(), MetricStatus.SUCCESS, null),
            null, null, null, SnapshotStatus.COMPLETE);

        assertThatThrownBy(() -> metricStoreService.save(snapshot))
            .isInstanceOf(MetricStoreException.class)
            .satisfies(e -> assertThat(((MetricStoreException) e).getErrorCode())
                .isEqualTo(MetricStoreErrorCode.INVALID_SNAPSHOT_PAYLOAD));
    }

    @Test
    @DisplayName("JVM 저장 중 DataAccessException 발생 시 MetricStoreException(METRIC_PERSIST_FAILED) 로 wrapping")
    void jvm_DataAccessException_시_wrapping() {
        JvmMetricSnapshotDto snapshot = normalJvmSnapshot();
        doThrow(new DataIntegrityViolationException("constraint violation"))
            .when(jvmMetricSnapshotRepository).save(any());

        assertThatThrownBy(() -> metricStoreService.save(snapshot))
            .isInstanceOf(MetricStoreException.class)
            .satisfies(e -> assertThat(((MetricStoreException) e).getErrorCode())
                .isEqualTo(MetricStoreErrorCode.METRIC_PERSIST_FAILED));
    }

    // ===== HTTP =====

    @Test
    @DisplayName("HTTP 정상 스냅샷 (p99/rps/errorRate 각 2개) 저장 시 자식 entity 6개 cascade 저장")
    void http_정상_스냅샷_저장시_자식_6개_cascade_저장() {
        HttpMetricSnapshotDto snapshot = normalHttpSnapshot();
        when(httpMetricSnapshotRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        metricStoreService.save(snapshot);

        ArgumentCaptor<HttpMetricSnapshotEntity> captor = forClass(HttpMetricSnapshotEntity.class);
        verify(httpMetricSnapshotRepository, times(1)).save(captor.capture());

        HttpMetricSnapshotEntity entity = captor.getValue();
        assertThat(entity.getPoints()).hasSize(6);
        assertThat(entity.getPoints()).filteredOn(p -> p.getKind() == HttpMetricKind.P99).hasSize(2);
        assertThat(entity.getPoints()).filteredOn(p -> p.getKind() == HttpMetricKind.RPS).hasSize(2);
        assertThat(entity.getPoints()).filteredOn(p -> p.getKind() == HttpMetricKind.ERROR_RATE).hasSize(2);
    }

    @Test
    @DisplayName("HTTP 일부 메타 null인 경우 snapshot 행은 저장되고 해당 자식 list는 비어있음")
    void http_일부_메타_null_시_자식_list_skip() {
        HttpMetricSnapshotDto snapshot = new HttpMetricSnapshotDto(
            "argus", "localhost:8080", OffsetDateTime.now(),
            new HttpResponseTimeDto(List.of(new EndpointMetricPointDto("/a", BigDecimal.ONE, OffsetDateTime.now())),
                OffsetDateTime.now(), MetricStatus.SUCCESS, null),
            null,   // rps null
            null,   // errorRate null
            SnapshotStatus.PARTIAL);
        when(httpMetricSnapshotRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        metricStoreService.save(snapshot);

        ArgumentCaptor<HttpMetricSnapshotEntity> captor = forClass(HttpMetricSnapshotEntity.class);
        verify(httpMetricSnapshotRepository, times(1)).save(captor.capture());

        HttpMetricSnapshotEntity entity = captor.getValue();
        assertThat(entity.getPoints()).hasSize(1);
        assertThat(entity.getPoints().get(0).getKind()).isEqualTo(HttpMetricKind.P99);
    }

    @Test
    @DisplayName("HTTP collectedAt null 시 MetricStoreException(INVALID_SNAPSHOT_PAYLOAD) throw")
    void http_collectedAt_null_시_예외_throw() {
        HttpMetricSnapshotDto snapshot = new HttpMetricSnapshotDto(
            "argus", "localhost:8080", null, null, null, null, SnapshotStatus.COMPLETE);

        assertThatThrownBy(() -> metricStoreService.save(snapshot))
            .isInstanceOf(MetricStoreException.class)
            .satisfies(e -> assertThat(((MetricStoreException) e).getErrorCode())
                .isEqualTo(MetricStoreErrorCode.INVALID_SNAPSHOT_PAYLOAD));
    }

    @Test
    @DisplayName("HTTP 저장 중 DataAccessException 발생 시 MetricStoreException(METRIC_PERSIST_FAILED) 로 wrapping")
    void http_DataAccessException_시_wrapping() {
        HttpMetricSnapshotDto snapshot = normalHttpSnapshot();
        doThrow(new DataIntegrityViolationException("constraint violation"))
            .when(httpMetricSnapshotRepository).save(any());

        assertThatThrownBy(() -> metricStoreService.save(snapshot))
            .isInstanceOf(MetricStoreException.class)
            .satisfies(e -> assertThat(((MetricStoreException) e).getErrorCode())
                .isEqualTo(MetricStoreErrorCode.METRIC_PERSIST_FAILED));
    }

    // ===== Hikari =====

    @Test
    @DisplayName("Hikari 정상 스냅샷 (active/pending/usageRatio 각 2개) 저장 시 자식 entity 6개 cascade 저장")
    void hikari_정상_스냅샷_저장시_자식_6개_cascade_저장() {
        HikariMetricSnapshotDto snapshot = normalHikariSnapshot();
        when(hikariMetricSnapshotRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        metricStoreService.save(snapshot);

        ArgumentCaptor<HikariMetricSnapshotEntity> captor = forClass(HikariMetricSnapshotEntity.class);
        verify(hikariMetricSnapshotRepository, times(1)).save(captor.capture());

        HikariMetricSnapshotEntity entity = captor.getValue();
        assertThat(entity.getPoints()).hasSize(6);
        assertThat(entity.getPoints()).filteredOn(p -> p.getKind() == HikariMetricKind.ACTIVE).hasSize(2);
        assertThat(entity.getPoints()).filteredOn(p -> p.getKind() == HikariMetricKind.PENDING).hasSize(2);
        assertThat(entity.getPoints()).filteredOn(p -> p.getKind() == HikariMetricKind.USAGE_RATIO).hasSize(2);
    }

    @Test
    @DisplayName("Hikari pending null 시 snapshot은 저장되고 pending 관련 points는 skip")
    void hikari_pending_null_시_pending_관련_points_skip() {
        List<PoolMetricPointDto> twoPoints = List.of(
            new PoolMetricPointDto("pool1", BigDecimal.valueOf(3), OffsetDateTime.now()),
            new PoolMetricPointDto("pool2", BigDecimal.valueOf(5), OffsetDateTime.now())
        );
        HikariMetricSnapshotDto snapshot = new HikariMetricSnapshotDto(
            "argus", "localhost:8080", OffsetDateTime.now(),
            new HikariActiveDto(twoPoints, twoPoints, OffsetDateTime.now(), MetricStatus.SUCCESS, null),
            null,  // pending null
            SnapshotStatus.PARTIAL);
        when(hikariMetricSnapshotRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        metricStoreService.save(snapshot);

        ArgumentCaptor<HikariMetricSnapshotEntity> captor = forClass(HikariMetricSnapshotEntity.class);
        verify(hikariMetricSnapshotRepository, times(1)).save(captor.capture());

        HikariMetricSnapshotEntity entity = captor.getValue();
        // pending points 제외 — ACTIVE 2개 + USAGE_RATIO 2개 = 4개
        assertThat(entity.getPoints()).hasSize(4);
        assertThat(entity.getPoints()).filteredOn(p -> p.getKind() == HikariMetricKind.ACTIVE).hasSize(2);
        assertThat(entity.getPoints()).filteredOn(p -> p.getKind() == HikariMetricKind.USAGE_RATIO).hasSize(2);
        assertThat(entity.getPoints()).filteredOn(p -> p.getKind() == HikariMetricKind.PENDING).isEmpty();
    }

    @Test
    @DisplayName("Hikari instance null 시 MetricStoreException(INVALID_SNAPSHOT_PAYLOAD) throw")
    void hikari_instance_null_시_예외_throw() {
        HikariMetricSnapshotDto snapshot = new HikariMetricSnapshotDto(
            "argus", null, OffsetDateTime.now(), null, null, SnapshotStatus.COMPLETE);

        assertThatThrownBy(() -> metricStoreService.save(snapshot))
            .isInstanceOf(MetricStoreException.class)
            .satisfies(e -> assertThat(((MetricStoreException) e).getErrorCode())
                .isEqualTo(MetricStoreErrorCode.INVALID_SNAPSHOT_PAYLOAD));
    }

    @Test
    @DisplayName("Hikari 저장 중 DataAccessException 발생 시 MetricStoreException(METRIC_PERSIST_FAILED) 로 wrapping")
    void hikari_DataAccessException_시_wrapping() {
        HikariMetricSnapshotDto snapshot = normalHikariSnapshot();
        doThrow(new DataIntegrityViolationException("constraint violation"))
            .when(hikariMetricSnapshotRepository).save(any());

        assertThatThrownBy(() -> metricStoreService.save(snapshot))
            .isInstanceOf(MetricStoreException.class)
            .satisfies(e -> assertThat(((MetricStoreException) e).getErrorCode())
                .isEqualTo(MetricStoreErrorCode.METRIC_PERSIST_FAILED));
    }

    // ===== Fixture =====

    private JvmMetricSnapshotDto normalJvmSnapshot() {
        return new JvmMetricSnapshotDto(
            "argus", "localhost:8080", OffsetDateTime.now(),
            new CpuUsageDto(BigDecimal.valueOf(50.0), OffsetDateTime.now(), MetricStatus.SUCCESS, null),
            new MemoryUsageDto(BigDecimal.valueOf(60.0), BigDecimal.valueOf(40.0), OffsetDateTime.now(), MetricStatus.SUCCESS, null),
            new GcMetricDto(BigDecimal.valueOf(0.01), BigDecimal.valueOf(5), OffsetDateTime.now(), MetricStatus.SUCCESS, null),
            new ThreadMetricDto(20, 25, 15, OffsetDateTime.now(), MetricStatus.SUCCESS, null),
            SnapshotStatus.COMPLETE);
    }

    private HttpMetricSnapshotDto normalHttpSnapshot() {
        List<EndpointMetricPointDto> twoPoints = List.of(
            new EndpointMetricPointDto("/api/a", BigDecimal.valueOf(0.5), OffsetDateTime.now()),
            new EndpointMetricPointDto("/api/b", BigDecimal.valueOf(0.8), OffsetDateTime.now())
        );
        return new HttpMetricSnapshotDto(
            "argus", "localhost:8080", OffsetDateTime.now(),
            new HttpResponseTimeDto(twoPoints, OffsetDateTime.now(), MetricStatus.SUCCESS, null),
            new HttpThroughputDto(twoPoints, OffsetDateTime.now(), MetricStatus.SUCCESS, null),
            new HttpErrorRateDto(twoPoints, OffsetDateTime.now(), MetricStatus.SUCCESS, null),
            SnapshotStatus.COMPLETE);
    }

    private HikariMetricSnapshotDto normalHikariSnapshot() {
        List<PoolMetricPointDto> twoPoints = List.of(
            new PoolMetricPointDto("pool1", BigDecimal.valueOf(3), OffsetDateTime.now()),
            new PoolMetricPointDto("pool2", BigDecimal.valueOf(5), OffsetDateTime.now())
        );
        return new HikariMetricSnapshotDto(
            "argus", "localhost:8080", OffsetDateTime.now(),
            new HikariActiveDto(twoPoints, twoPoints, OffsetDateTime.now(), MetricStatus.SUCCESS, null),
            new HikariPendingDto(twoPoints, OffsetDateTime.now(), MetricStatus.SUCCESS, null),
            SnapshotStatus.COMPLETE);
    }
}
