# Argus 메트릭 PromQL 및 DTO 매핑

본 문서는 Argus의 `MetricType`에 정의된 PromQL과 각 Snapshot Assembler가 값을 조립해 넣는 DTO 필드를 정리한다.

- 기준 코드
  - `argus/src/main/java/com/example/argus/service/metric/MetricType.java`
  - `argus/src/main/java/com/example/argus/service/metric/snapshot/JvmMetricSnapshotAssembler.java`
  - `argus/src/main/java/com/example/argus/service/metric/snapshot/HttpMetricSnapshotAssembler.java`
  - `argus/src/main/java/com/example/argus/service/metric/snapshot/HikariMetricSnapshotAssembler.java`

---

## 1. 공통 흐름

Argus는 `PrometheusQueryService.queryByMetric(MetricType)`로 Prometheus를 조회한 뒤, Assembler에서 Prometheus 응답을 DTO로 변환한다.

| 도메인 | Assembler | 최종 Snapshot DTO | Mapper 방식 |
|--------|-----------|-------------------|-------------|
| JVM | `JvmMetricSnapshotAssembler` | `JvmMetricSnapshotDto` | 단일 vector 결과를 `MetricPointDto`로 변환 |
| HTTP | `HttpMetricSnapshotAssembler` | `HttpMetricSnapshotDto` | `uri` 라벨 기준 다중 point를 `EndpointMetricPointDto`로 변환 |
| HIKARI | `HikariMetricSnapshotAssembler` | `HikariMetricSnapshotDto` | `pool` 라벨 기준 다중 point를 `PoolMetricPointDto`로 변환 |

`application`, `instance`는 성공한 응답의 라벨에서 누적되어 Snapshot DTO 최상위 필드에 들어간다. 단, 현재 `HTTP_RPS`는 `sum by (uri)`라 `application`, `instance` 라벨이 없어 라벨 누적 대상이 아니다.

---

## 2. JVM PromQL

최종 DTO: `JvmMetricSnapshotDto`

| MetricType | PromQL | 입력 DTO 필드 | 비고 |
|------------|--------|---------------|------|
| `CPU_USAGE` | `avg by (application, instance) (avg_over_time(process_cpu_usage[1m]))` | `cpu.usagePercent` | `CpuUsageDto` |
| `HEAP_USAGE` | `sum by (application, instance) (jvm_memory_used_bytes{area="heap"}) / sum by (application, instance) (jvm_memory_max_bytes{area="heap"}) * 100` | `memory.heapUsagePercent` | `MemoryUsageDto` |
| `HEAP_OLD_GEN_USAGE` | `sum by (application, instance) (jvm_memory_used_bytes{area="heap", id=~".*Old Gen|.*Tenured Gen"}) / sum by (application, instance) (jvm_memory_max_bytes{area="heap", id=~".*Old Gen|.*Tenured Gen"}) * 100` | `memory.oldGenUsagePercent` | `MemoryUsageDto` |
| `GC_AVG_DURATION` | `sum by (application, instance) (increase(jvm_gc_pause_seconds_sum[1m])) / clamp_min(sum by (application, instance) (increase(jvm_gc_pause_seconds_count[1m])), 1)` | `gc.avgDurationSeconds` | `GcMetricDto` |
| `GC_COUNT` | `sum by (application, instance) (increase(jvm_gc_pause_seconds_count[1m]))` | `gc.count` | `GcMetricDto` |
| `ACTIVE_THREADS` | `avg by (application, instance) (jvm_threads_live_threads)` | `thread.activeCount` | `ThreadMetricDto` |
| `PEAK_THREADS` | `avg by (application, instance) (jvm_threads_peak_threads)` | `thread.peakCount` | `ThreadMetricDto` |
| `DAEMON_THREADS` | `avg by (application, instance) (jvm_threads_daemon_threads)` | `thread.daemonCount` | `ThreadMetricDto` |

### 2.1 JVM 조립 규칙

- `CPU_USAGE`는 `CpuUsageDto.from(...)`으로 변환되어 `JvmMetricSnapshotDto.cpu`에 들어간다.
- `HEAP_USAGE`, `HEAP_OLD_GEN_USAGE`는 함께 조회되어 `MemoryUsageDto.from(heapResult, oldGenResult)`로 조립된다.
- `GC_AVG_DURATION`, `GC_COUNT`는 함께 조회되어 `GcMetricDto.from(durationResult, countResult)`로 조립된다.
- `ACTIVE_THREADS`, `PEAK_THREADS`, `DAEMON_THREADS`는 함께 조회되어 `ThreadMetricDto.from(activeResult, peakResult, daemonResult)`로 조립된다.
- `cpu`, `memory`, `gc`, `thread`의 `MetricStatus`를 모아 `SnapshotStatus.from(...)`으로 최종 `JvmMetricSnapshotDto.status`를 계산한다.

---

## 3. HTTP PromQL

최종 DTO: `HttpMetricSnapshotDto`

| MetricType | PromQL | 입력 DTO 필드 | 비고 |
|------------|--------|---------------|------|
| `HTTP_P99_RESPONSE_TIME` | `histogram_quantile(0.99, sum by (application, instance, uri, le) (rate(http_server_requests_seconds_bucket[1m])))` | `p99.points[].value` | `EndpointMetricPointDto.endpoint`에는 `uri` 라벨이 들어간다 |
| `HTTP_RPS` | `sum by (uri) (rate(http_server_requests_seconds_count[1m]))` | `rps.points[].value` | `application`, `instance` 라벨 없음 |
| `HTTP_ERROR_RATE` | `sum by (application, instance, uri) (rate(http_server_requests_seconds_count{status=~"5.."}[1m])) / clamp_min(sum by (application, instance, uri) (rate(http_server_requests_seconds_count[1m])), 1)` | `errorRate.points[].value` | 5xx 비율 |

### 3.1 HTTP 조립 규칙

- `HTTP_P99_RESPONSE_TIME`은 `HttpResponseTimeDto.from(...)`으로 변환되어 `HttpMetricSnapshotDto.p99`에 들어간다.
- `HTTP_RPS`는 `HttpThroughputDto.from(...)`으로 변환되어 `HttpMetricSnapshotDto.rps`에 들어간다.
- `HTTP_ERROR_RATE`는 `HttpErrorRateDto.from(...)`으로 변환되어 `HttpMetricSnapshotDto.errorRate`에 들어간다.
- HTTP 계열은 `MetricPointMapper.toPoints(..., "uri")`를 사용하므로 Prometheus 응답의 `uri` 라벨이 `EndpointMetricPointDto.endpoint`로 매핑된다.
- `p99`, `rps`, `errorRate`의 `MetricStatus`를 모아 `SnapshotStatus.from(...)`으로 최종 `HttpMetricSnapshotDto.status`를 계산한다.

---

## 4. HIKARI PromQL

최종 DTO: `HikariMetricSnapshotDto`

| MetricType | PromQL | 입력 DTO 필드 | 비고 |
|------------|--------|---------------|------|
| `HIKARI_ACTIVE_CONNECTIONS` | `avg_over_time(hikaricp_connections_active[1m])` | `active.points[].value` | `PoolMetricPointDto.pool`에는 `pool` 라벨이 들어간다 |
| `HIKARI_USAGE_RATIO` | `hikaricp_connections_active / clamp_min(hikaricp_connections_max, 1)` | `active.usageRatio[].value` | active/max 비율 |
| `HIKARI_PENDING_CONNECTIONS` | `avg_over_time(hikaricp_connections_pending[1m])` | `pending.points[].value` | `PoolMetricPointDto.pool`에는 `pool` 라벨이 들어간다 |

### 4.1 HIKARI 조립 규칙

- `HIKARI_ACTIVE_CONNECTIONS`, `HIKARI_USAGE_RATIO`는 함께 조회되어 `HikariActiveDto.from(activeResult, usageRatioResult)`로 조립된다.
- `HIKARI_PENDING_CONNECTIONS`는 `HikariPendingDto.from(...)`으로 변환되어 `HikariMetricSnapshotDto.pending`에 들어간다.
- HIKARI 계열은 `MetricPointMapper.toPoints(..., "pool")`을 사용하므로 Prometheus 응답의 `pool` 라벨이 `PoolMetricPointDto.pool`로 매핑된다.
- `active`, `pending`의 `MetricStatus`를 모아 `SnapshotStatus.from(...)`으로 최종 `HikariMetricSnapshotDto.status`를 계산한다.

---

## 5. DTO 전체 구조 요약

### 5.1 `JvmMetricSnapshotDto`

| 필드 | 타입 | PromQL 출처 |
|------|------|-------------|
| `application` | `String` | 성공 응답의 `application` 라벨 |
| `instance` | `String` | 성공 응답의 `instance` 라벨 |
| `collectedAt` | `Instant` | Assembler 수집 시각 |
| `cpu.usagePercent` | `BigDecimal` | `CPU_USAGE` |
| `memory.heapUsagePercent` | `BigDecimal` | `HEAP_USAGE` |
| `memory.oldGenUsagePercent` | `BigDecimal` | `HEAP_OLD_GEN_USAGE` |
| `gc.avgDurationSeconds` | `BigDecimal` | `GC_AVG_DURATION` |
| `gc.count` | `BigDecimal` | `GC_COUNT` |
| `thread.activeCount` | `Integer` | `ACTIVE_THREADS` |
| `thread.peakCount` | `Integer` | `PEAK_THREADS` |
| `thread.daemonCount` | `Integer` | `DAEMON_THREADS` |
| `status` | `SnapshotStatus` | 하위 DTO 상태 종합 |

### 5.2 `HttpMetricSnapshotDto`

| 필드 | 타입 | PromQL 출처 |
|------|------|-------------|
| `application` | `String` | `p99` 또는 `errorRate` 성공 응답의 `application` 라벨 |
| `instance` | `String` | `p99` 또는 `errorRate` 성공 응답의 `instance` 라벨 |
| `collectedAt` | `Instant` | Assembler 수집 시각 |
| `p99.points[].endpoint` | `String` | `HTTP_P99_RESPONSE_TIME`의 `uri` 라벨 |
| `p99.points[].value` | `BigDecimal` | `HTTP_P99_RESPONSE_TIME` |
| `rps.points[].endpoint` | `String` | `HTTP_RPS`의 `uri` 라벨 |
| `rps.points[].value` | `BigDecimal` | `HTTP_RPS` |
| `errorRate.points[].endpoint` | `String` | `HTTP_ERROR_RATE`의 `uri` 라벨 |
| `errorRate.points[].value` | `BigDecimal` | `HTTP_ERROR_RATE` |
| `status` | `SnapshotStatus` | 하위 DTO 상태 종합 |

### 5.3 `HikariMetricSnapshotDto`

| 필드 | 타입 | PromQL 출처 |
|------|------|-------------|
| `application` | `String` | 성공 응답의 `application` 라벨 |
| `instance` | `String` | 성공 응답의 `instance` 라벨 |
| `collectedAt` | `Instant` | Assembler 수집 시각 |
| `active.points[].pool` | `String` | `HIKARI_ACTIVE_CONNECTIONS`의 `pool` 라벨 |
| `active.points[].value` | `BigDecimal` | `HIKARI_ACTIVE_CONNECTIONS` |
| `active.usageRatio[].pool` | `String` | `HIKARI_USAGE_RATIO`의 `pool` 라벨 |
| `active.usageRatio[].value` | `BigDecimal` | `HIKARI_USAGE_RATIO` |
| `pending.points[].pool` | `String` | `HIKARI_PENDING_CONNECTIONS`의 `pool` 라벨 |
| `pending.points[].value` | `BigDecimal` | `HIKARI_PENDING_CONNECTIONS` |
| `status` | `SnapshotStatus` | 하위 DTO 상태 종합 |
