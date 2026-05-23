package com.example.pythia.metric.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.pythia.kafka.dto.MetricStatus;
import com.example.pythia.kafka.dto.SnapshotStatus;
import com.example.pythia.kafka.dto.hikari.HikariActiveDto;
import com.example.pythia.kafka.dto.hikari.HikariMetricSnapshotDto;
import com.example.pythia.kafka.dto.hikari.HikariPendingDto;
import com.example.pythia.kafka.dto.hikari.PoolMetricPointDto;
import com.example.pythia.kafka.dto.http.EndpointMetricPointDto;
import com.example.pythia.kafka.dto.http.HttpErrorRateDto;
import com.example.pythia.kafka.dto.http.HttpMetricSnapshotDto;
import com.example.pythia.kafka.dto.http.HttpResponseTimeDto;
import com.example.pythia.kafka.dto.http.HttpThroughputDto;
import com.example.pythia.kafka.dto.jvm.CpuUsageDto;
import com.example.pythia.kafka.dto.jvm.GcMetricDto;
import com.example.pythia.kafka.dto.jvm.JvmMetricSnapshotDto;
import com.example.pythia.kafka.dto.jvm.MemoryUsageDto;
import com.example.pythia.kafka.dto.jvm.ThreadMetricDto;
import com.example.pythia.metric.domain.HikariMetricSnapshotEntity;
import com.example.pythia.metric.domain.HttpMetricSnapshotEntity;
import com.example.pythia.metric.domain.JvmMetricSnapshotEntity;
import com.example.pythia.metric.repository.HikariMetricSnapshotRepository;
import com.example.pythia.metric.repository.HttpMetricSnapshotRepository;
import com.example.pythia.metric.repository.JvmMetricSnapshotRepository;
import jakarta.persistence.EntityManager;
import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.transaction.annotation.Transactional;

/**
 * Spring Boot 4.0.6 환경에서는 spring-boot-test-autoconfigure 모듈에 @DataJpaTest 클래스가
 * 포함되어 있지 않습니다 (jar 내 orm/jpa 패키지 자체가 없음).
 * 확인: spring-boot-test-autoconfigure-4.0.6.jar 내 autoconfigure 하위에 jdbc, json 패키지만 존재.
 * 따라서 @SpringBootTest + @ActiveProfiles("test") + @Transactional 조합으로 실제 INSERT 검증을 수행합니다.
 * Kafka, Mail 자동 구성은 application-test.yml의 autoconfigure.exclude로 격리합니다.
 */
@SpringBootTest
@ActiveProfiles("test")
@Transactional
class MetricStoreServiceImplPersistenceTest {

    @MockitoBean
    private RedissonClient redissonClient;

    @MockitoBean
    private StringRedisTemplate stringRedisTemplate;

    @Autowired
    private MetricStoreServiceImpl metricStoreService;

    @Autowired
    private JvmMetricSnapshotRepository jvmMetricSnapshotRepository;

    @Autowired
    private HttpMetricSnapshotRepository httpMetricSnapshotRepository;

    @Autowired
    private HikariMetricSnapshotRepository hikariMetricSnapshotRepository;

    @Autowired
    private EntityManager entityManager;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    @DisplayName("JVM 스냅샷 저장 후 repository.findAll()로 1건 조회 및 필드 검증")
    void jvm_스냅샷_실제_저장_검증() {
        OffsetDateTime kstTime = kstTime();
        JvmMetricSnapshotDto snapshot = new JvmMetricSnapshotDto(
            "argus", "localhost:8080", kstTime,
            new CpuUsageDto(BigDecimal.valueOf(55.5), kstTime, MetricStatus.SUCCESS, null),
            new MemoryUsageDto(BigDecimal.valueOf(70.0), BigDecimal.valueOf(50.0), kstTime, MetricStatus.SUCCESS, null),
            new GcMetricDto(BigDecimal.valueOf(0.02), BigDecimal.valueOf(3), kstTime, MetricStatus.SUCCESS, null),
            new ThreadMetricDto(10, 12, 8, kstTime, MetricStatus.SUCCESS, null),
            SnapshotStatus.COMPLETE);

        metricStoreService.save(snapshot);
        flushAndClear();

        List<JvmMetricSnapshotEntity> all = jvmMetricSnapshotRepository.findAll();
        assertThat(all).hasSize(1);
        JvmMetricSnapshotEntity entity = all.get(0);
        assertThat(entity.getApplication()).isEqualTo("argus");
        assertThat(entity.getInstance()).isEqualTo("localhost:8080");
        assertThat(entity.getStatus()).isEqualTo(SnapshotStatus.COMPLETE);
        assertThat(entity.getCollectedAt()).isEqualTo(kstTime);
        assertThat(entity.getCollectedAt().getOffset()).isEqualTo(ZoneOffset.ofHours(9));
        assertThat(entity.getCpuMeasuredAt()).isEqualTo(kstTime);
        assertThat(entity.getCpuMeasuredAt().getOffset()).isEqualTo(ZoneOffset.ofHours(9));
        assertRawTimestampStoredAsKst("jvm_metric_snapshot", "collected_at");
        assertRawTimestampStoredAsKst("jvm_metric_snapshot", "cpu_measured_at");
        assertThat(entity.getCpuUsagePercent()).isEqualByComparingTo(BigDecimal.valueOf(55.5));
        assertThat(entity.getThreadActiveCount()).isEqualTo(10);
    }

    @Test
    @DisplayName("HTTP 스냅샷 저장 후 자식 포인트 수 검증 (p99 2개 + rps 2개 + errorRate 2개 = 6개)")
    void http_스냅샷_실제_저장_및_자식_row_검증() {
        OffsetDateTime kstTime = kstTime();
        List<EndpointMetricPointDto> twoPoints = List.of(
            new EndpointMetricPointDto("/api/a", BigDecimal.valueOf(0.3), kstTime),
            new EndpointMetricPointDto("/api/b", BigDecimal.valueOf(0.6), kstTime)
        );
        HttpMetricSnapshotDto snapshot = new HttpMetricSnapshotDto(
            "argus", "localhost:8080", kstTime,
            new HttpResponseTimeDto(twoPoints, kstTime, MetricStatus.SUCCESS, null),
            new HttpThroughputDto(twoPoints, kstTime, MetricStatus.SUCCESS, null),
            new HttpErrorRateDto(twoPoints, kstTime, MetricStatus.SUCCESS, null),
            SnapshotStatus.COMPLETE);

        metricStoreService.save(snapshot);
        flushAndClear();

        List<HttpMetricSnapshotEntity> all = httpMetricSnapshotRepository.findAll();
        assertThat(all).hasSize(1);
        HttpMetricSnapshotEntity entity = all.get(0);
        assertThat(entity.getCollectedAt()).isEqualTo(kstTime);
        assertThat(entity.getCollectedAt().getOffset()).isEqualTo(ZoneOffset.ofHours(9));
        assertThat(entity.getP99MeasuredAt()).isEqualTo(kstTime);
        assertThat(entity.getP99MeasuredAt().getOffset()).isEqualTo(ZoneOffset.ofHours(9));
        assertRawTimestampStoredAsKst("http_metric_snapshot", "collected_at");
        assertRawTimestampStoredAsKst("http_metric_snapshot", "p99_measured_at");
        assertRawTimestampStoredAsKst("http_endpoint_metric_point", "measured_at");
        assertThat(entity.getPoints()).hasSize(6);
        assertThat(entity.getPoints())
            .allSatisfy(point -> assertThat(point.getMeasuredAt().getOffset()).isEqualTo(ZoneOffset.ofHours(9)));
    }

    @Test
    @DisplayName("Hikari 스냅샷 저장 후 자식 포인트 수 검증 (active 2개 + pending 2개 + usageRatio 2개 = 6개)")
    void hikari_스냅샷_실제_저장_및_자식_row_검증() {
        OffsetDateTime kstTime = kstTime();
        List<PoolMetricPointDto> twoPoints = List.of(
            new PoolMetricPointDto("HikariPool-1", BigDecimal.valueOf(2), kstTime),
            new PoolMetricPointDto("HikariPool-2", BigDecimal.valueOf(4), kstTime)
        );
        HikariMetricSnapshotDto snapshot = new HikariMetricSnapshotDto(
            "argus", "localhost:8080", kstTime,
            new HikariActiveDto(twoPoints, twoPoints, kstTime, MetricStatus.SUCCESS, null),
            new HikariPendingDto(twoPoints, kstTime, MetricStatus.SUCCESS, null),
            SnapshotStatus.COMPLETE);

        metricStoreService.save(snapshot);
        flushAndClear();

        List<HikariMetricSnapshotEntity> all = hikariMetricSnapshotRepository.findAll();
        assertThat(all).hasSize(1);
        HikariMetricSnapshotEntity entity = all.get(0);
        assertThat(entity.getCollectedAt()).isEqualTo(kstTime);
        assertThat(entity.getCollectedAt().getOffset()).isEqualTo(ZoneOffset.ofHours(9));
        assertThat(entity.getActiveMeasuredAt()).isEqualTo(kstTime);
        assertThat(entity.getActiveMeasuredAt().getOffset()).isEqualTo(ZoneOffset.ofHours(9));
        assertRawTimestampStoredAsKst("hikari_metric_snapshot", "collected_at");
        assertRawTimestampStoredAsKst("hikari_metric_snapshot", "active_measured_at");
        assertRawTimestampStoredAsKst("hikari_pool_metric_point", "measured_at");
        assertThat(entity.getPoints()).hasSize(6);
        assertThat(entity.getPoints())
            .allSatisfy(point -> assertThat(point.getMeasuredAt().getOffset()).isEqualTo(ZoneOffset.ofHours(9)));
    }

    private static OffsetDateTime kstTime() {
        return OffsetDateTime.of(2026, 5, 9, 22, 34, 34, 0, ZoneOffset.ofHours(9));
    }

    private void flushAndClear() {
        entityManager.flush();
        entityManager.clear();
    }

    private void assertRawTimestampStoredAsKst(String tableName, String columnName) {
        String rawTimestamp = jdbcTemplate.queryForObject(
            "select cast(" + columnName + " as varchar) from " + tableName + " limit 1",
            String.class);

        assertThat(rawTimestamp).contains("2026-05-09 22:34:34");
        assertThat(rawTimestamp).doesNotContain("2026-05-09 13:34:34");
    }
}
