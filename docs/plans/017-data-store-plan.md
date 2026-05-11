# Task 017: Pythia 메트릭 RDB 저장 — 구현 계획

## Context

Task 013에서 Pythia가 jvm.metrics.raw / http.metrics.raw / hikari.metrics.raw Kafka 토픽을 수신하고 Handler에서 단순 검증 후 ThresholdEvaluator로 위임하는 구조를 갖추었다. 그러나 수신한 스냅샷은 임계값 평가에만 쓰일 뿐 영속화되지 않아 후속 Task(LLM 분석, 이력 조회, 패턴 학습 등)에서 과거 메트릭을 사용할 수 없다.

본 task는 (a) PostgreSQL 연동 인프라(JPA/driver/datasource)를 도입하고, (b) 세 토픽의 스냅샷 DTO를 RDB 테이블로 매핑하며, (c) 각 Handler가 위협 평가와 별개로 저장 Service를 호출하도록 한다. **Controller/API/LLM 분석/조회/range query/집계는 모두 본 task scope 외**이며 단순한 INSERT 위주 구조로 한정한다.

사용자 결정사항 (확정):
- target: pythia
- 입력: 기존 JvmMetricSnapshotDto, HttpMetricSnapshotDto, HikariMetricSnapshotDto (변경 금지)
- 성공 기준: "이후 구현 Task에서 바로 사용 가능" + "RDB에 정상적으로 테이블 생성"

---

## 1. 설계 방식 및 이유

### 방향: metric 신규 패키지 + snapshot당 1행 + 자식 메트릭 포인트는 별도 테이블 + JPA ddl-auto=update + Handler에서 Service 직접 호출

| 결정 | 선택 | 이유 |
|------|------|------|
| 패키지 | com.example.pythia.metric (top-level) | alert, email, kafka와 같은 결. 저장 책임을 단일 패키지에 응집. 하위에 domain / repository / service 분리 (CLAUDE.md 패키지 규칙) |
| 진입점 | 각 Handler handle() 끝에서 MetricStoreService.save(snapshot) 직접 호출 | Task 016 Evaluator 호출 패턴과 동일. 이벤트 발행은 본 task scope 대비 과한 간접화. Handler->Store 의존 1단계만 추가 |
| 저장 호출 위치 | Evaluator 호출 후 save 호출 (별도 try-catch로 격리) | 저장 실패가 Evaluator를 막지 않도록 분리. Handler 내부에서 save 예외는 catch + log.error로만 처리하여 Kafka listener가 멈추지 않도록 함 (자세한 내용은 §4) |
| 테이블 분리 전략 | 메트릭 종류별 snapshot 테이블 3개 (jvm_metric_snapshot, http_metric_snapshot, hikari_metric_snapshot) + 자식 포인트 테이블 (HTTP/Hikari만) | 각 DTO의 스칼라 필드는 normalize한 컬럼으로, list 필드(points)는 별도 자식 테이블로. JVM은 자식 list가 없어 단일 테이블만. denormalize(JSON 한 컬럼)는 후속 Task에서 컬럼 단위 조회 필요 시 변환 비용 발생, 단일 테이블에 모든 메트릭을 합치면 nullable 폭증 -> snapshot 단위 분리 + 자식 1뎁스 normalize가 가장 단순한 균형점 |
| ID 전략 | @GeneratedValue(strategy=IDENTITY) Long id (PostgreSQL BIGSERIAL) | 자연키 후보(application+instance+collectedAt)는 동일 시점 재처리/중복 메시지 시 충돌. surrogate key + nullable composite의 unique 제약 없이 삽입 (중복 메시지는 후속 task의 멱등성 처리 대상으로 분리) |
| 자식 포인트 매핑 | @OneToMany(cascade=PERSIST, orphanRemoval=false, fetch=LAZY) + @JoinColumn(name="snapshot_id") (단방향) | bidirectional은 본 task scope에서 불필요. cascade PERSIST로 부모 저장 시 자식 일괄 INSERT. mappedBy 양방향은 후속 조회 task에서 필요 시 양방향화 |
| Status enum 매핑 | @Enumerated(EnumType.STRING) | SnapshotStatus, MetricStatus를 그대로 컬럼 값(VARCHAR)으로 저장. ORDINAL은 enum 추가/순서 변경 시 데이터 손상 위험 |
| Instant 매핑 | TIMESTAMP WITH TIME ZONE (columnDefinition="TIMESTAMP WITH TIME ZONE") | PostgreSQL 권장. JPA는 Instant <-> timestamptz 매핑 지원 (Hibernate 6.x 기본). collectedAt/measuredAt/포인트별 measuredAt은 모두 timestamptz |
| BigDecimal 매핑 | @Column(precision=19, scale=6) numeric | CPU(%, 0~100), GC seconds(소수 6자리), counts(정수형이지만 DTO가 BigDecimal) 모두 수용. precision=19/scale=6은 실측 가능한 값 범위와 충돌 없음 |
| schema 생성 | spring.jpa.hibernate.ddl-auto=update (개발 H2/PostgreSQL 양쪽 동작) | Task scope에 맞는 가장 단순한 방법. CLAUDE.md "DDL은 JPA ddl-auto로 생성하되 schema 파일도 옵션으로 고려" 가이드 충족. 운영용 SQL 파일은 §2에 옵션으로 추가만 하고, 본 task의 1차 검증은 ddl-auto만으로 충분 |
| schema.sql (옵션) | pythia/src/main/resources/db/schema-postgres.sql (참고용) | 운영 환경에서 ddl-auto를 끄고 수동 적용할 때 사용. spring.sql.init.mode=never 기본 유지로 자동 실행은 X. 본 task에서는 파일 생성만, 실 적용 옵션은 후속 |
| Service 계층 구조 | MetricStoreService (interface) + 단일 구현 MetricStoreServiceImpl | 인터페이스 분리는 추후 mocking/대안 구현(예: outbox 비동기 저장) 도입 시 비용 절감. CLAUDE.md "Controller -> ServiceResponse -> Service -> Repository" 규칙에 ServiceResponse는 API 계층 콘셉트로, 본 task는 Controller 부재 -> Service만 존재 |
| Service 메서드 시그니처 | save(JvmMetricSnapshotDto) / save(HttpMetricSnapshotDto) / save(HikariMetricSnapshotDto) 3개 오버로드 | 메서드 분리로 의존성/테스트 단순. 단일 save(Object) 분기는 안티패턴 |
| DTO -> Entity 변환 | Service 내부 private static factory (toEntity(...)) | 별도 mapper 빈은 본 task scope에 과함. mapping 코드 자체는 단순 - record 접근자 호출 -> builder. 향후 매퍼 클래스 분리 가능 |
| 트랜잭션 경계 | @Transactional을 Service 구현 메서드에 적용 | snapshot + 자식 포인트들이 하나의 단위로 commit/rollback. Kafka listener는 트랜잭션을 propagate하지 않음 (REQUIRES_NEW 불필요) |
| 예외 처리 | MetricStoreException extends CustomException + MetricStoreErrorCode enum | Task 015 EmailSendException 패턴 재사용. Repository에서 발생한 DataAccessException을 Service에서 wrapping. Handler에서는 MetricStoreException을 catch + log.error |
| MISSING/null 메트릭 처리 | null인 자식 객체는 그대로 null 컬럼 저장 (스칼라 필드도 null 허용) | Handler는 이미 status==FAILED || cpu==null 조기 return 적용 중이지만, status가 PARTIAL일 때 일부 메트릭만 null인 경우가 있음 -> snapshot 행은 저장하고 missing 필드만 null. 후속 task에서 결측 분석 가능 |
| Handler 호출 순서 | evaluator.evaluateXxx(snapshot) -> metricStoreService.save(snapshot) | 평가는 동기 + in-memory 빠른 작업, 저장은 DB I/O 포함. 이미 16번 task 디자인이 평가 우선이므로 순서 유지. 둘 사이에 의존성 없음. 저장 실패가 평가 결과에 영향을 주지 않도록 try-catch로 격리 |
| 재시도/DLQ | 본 task scope 외 (단순 catch + log) | Kafka 재처리 인프라(retryable consumer, DLQ topic) 도입은 별도 task. 본 task는 "저장이 정상 케이스에서 동작"이 목표 |
| 인덱스 | PK만 명시. (application, collectedAt) 등 보조 인덱스는 후속 (range query 처리 제외 범위) | Task 명세 "range query 처리는 제외" -> 인덱스 최소화. ddl-auto가 PK 자동 생성 |
| H2 / PostgreSQL 동시 지원 | dialect 자동 감지(Spring Boot 기본). 개발: H2 in-memory(MODE=PostgreSQL), 운영: PostgreSQL 5432 (docker compose) | application.yml에 H2를 default profile, postgres profile 분리 가능하나 본 task에서는 default를 PostgreSQL로 두고 H2는 test 의존성으로만 사용 (CLAUDE.md "DB: PostgreSQL (개발: H2)" 가이드) |
| docker compose | /docker/docker-compose.postgres.yml 신규 | CLAUDE.md "Docker 파일 규칙: /docker 경로에 생성". 기존 kafka compose와 동급으로 분리 |

### 비대상 (의식적 제외)
- API/Controller/Repository range 조회 메서드
- 집계 테이블, 일일/시간별 rollup
- LLM 분석용 별도 컬럼/테이블
- 멀티 노드 aggregation
- 멱등성 처리(중복 메시지 dedup)
- DLQ/retry topic
- audit/soft delete/version 컬럼
- read-only replica 라우팅
- Flyway/Liquibase 마이그레이션 도구


---

## 2. 구성 요소

### 신규 파일

#### Entity (domain)

**JvmMetricSnapshotEntity** (com.example.pythia.metric.domain) — @Entity @Table(name="jvm_metric_snapshot")

---

## 2. 구성 요소

### 신규 파일 - Entity

JvmMetricSnapshotEntity (com.example.pythia.metric.domain) — @Entity @Table(name="jvm_metric_snapshot")
- Long id (PK, IDENTITY)
- String application (@Column(nullable=false, length=128))
- String instance (@Column(nullable=false, length=128))
- Instant collectedAt (@Column(nullable=false, columnDefinition="TIMESTAMP WITH TIME ZONE"))
- SnapshotStatus status (@Enumerated(STRING) @Column(nullable=false, length=16))
- CPU 평탄화: BigDecimal cpuUsagePercent, Instant cpuMeasuredAt, MetricStatus cpuStatus, String cpuMissingReason
- Memory 평탄화: BigDecimal heapUsagePercent, BigDecimal oldGenUsagePercent, Instant memoryMeasuredAt, MetricStatus memoryStatus, String memoryMissingReason
- GC 평탄화: BigDecimal gcAvgDurationSeconds, BigDecimal gcCount, Instant gcMeasuredAt, MetricStatus gcStatus, String gcMissingReason
- Thread 평탄화: Integer threadActiveCount, Integer threadPeakCount, Integer threadDaemonCount, Instant threadMeasuredAt, MetricStatus threadStatus, String threadMissingReason
- JVM은 자식 list가 없어 모두 평탄화. nullable 허용
- Lombok @Getter, @NoArgsConstructor(access=PROTECTED), @AllArgsConstructor(access=PRIVATE), @Builder

HttpMetricSnapshotEntity (com.example.pythia.metric.domain) — @Entity @Table(name="http_metric_snapshot")
- Long id, String application, String instance, Instant collectedAt, SnapshotStatus status (위와 동일)
- p99 메타: Instant p99MeasuredAt, MetricStatus p99Status, String p99MissingReason
- rps 메타: Instant rpsMeasuredAt, MetricStatus rpsStatus, String rpsMissingReason
- errorRate 메타: Instant errorRateMeasuredAt, MetricStatus errorRateStatus, String errorRateMissingReason
- 자식 list (단방향 OneToMany, 옵션 B): List<HttpEndpointMetricPointEntity> points — 단일 list, 자식 entity의 kind 필드(P99/RPS/ERROR_RATE)로 구분
- 채택 이유: 테이블 수 감소(2개 -> 6개 회피), 매핑 단순. 트레이드오프 §6 참조

HikariMetricSnapshotEntity (com.example.pythia.metric.domain) — @Entity @Table(name="hikari_metric_snapshot")
- Long id, String application, String instance, Instant collectedAt, SnapshotStatus status
- active 메타: Instant activeMeasuredAt, MetricStatus activeStatus, String activeMissingReason
- pending 메타: Instant pendingMeasuredAt, MetricStatus pendingStatus, String pendingMissingReason
- 자식 list (옵션 B): List<HikariPoolMetricPointEntity> points — kind ACTIVE/PENDING/USAGE_RATIO

HttpEndpointMetricPointEntity (com.example.pythia.metric.domain) — @Entity @Table(name="http_endpoint_metric_point")
- Long id (IDENTITY)
- Long snapshotId (@Column(name="snapshot_id", nullable=false), 부모 엔티티의 @JoinColumn(name="snapshot_id")로 연결되는 단방향 FK)
- HttpMetricKind kind (@Enumerated(STRING), nullable=false, length=16)
- String endpoint (nullable=false, length=512)
- BigDecimal value (precision=19, scale=6)
- Instant measuredAt (timestamptz)

HikariPoolMetricPointEntity (com.example.pythia.metric.domain) — @Entity @Table(name="hikari_pool_metric_point")
- 동일 패턴: Long id, Long snapshotId, HikariMetricKind kind (ACTIVE/PENDING/USAGE_RATIO), String pool, BigDecimal value, Instant measuredAt

HttpMetricKind enum: P99, RPS, ERROR_RATE
HikariMetricKind enum: ACTIVE, PENDING, USAGE_RATIO

### Repository

- com.example.pythia.metric.repository.JvmMetricSnapshotRepository — extends JpaRepository<JvmMetricSnapshotEntity, Long> (조회 메서드 0개)
- com.example.pythia.metric.repository.HttpMetricSnapshotRepository — extends JpaRepository<HttpMetricSnapshotEntity, Long>
- com.example.pythia.metric.repository.HikariMetricSnapshotRepository — extends JpaRepository<HikariMetricSnapshotEntity, Long>

자식 포인트 Repository는 cascade로 부모와 함께 저장되므로 불필요(저장 only). 후속 조회 task에서 추가.

### Service

MetricStoreService (com.example.pythia.metric.service) — interface
- void save(JvmMetricSnapshotDto snapshot)
- void save(HttpMetricSnapshotDto snapshot)
- void save(HikariMetricSnapshotDto snapshot)

MetricStoreServiceImpl (com.example.pythia.metric.service) — @Service @RequiredArgsConstructor
- 의존: JvmMetricSnapshotRepository, HttpMetricSnapshotRepository, HikariMetricSnapshotRepository
- 각 save(...)는 @Transactional 적용
- 흐름:
  1. application/instance/collectedAt null 가드 -> MetricStoreException(INVALID_SNAPSHOT_PAYLOAD) throw
  2. DTO -> Entity 변환 (private static toEntity(...) 메서드, null-safe)
  3. repository.save(entity) 호출
  4. DataAccessException 발생 시 MetricStoreException(METRIC_PERSIST_FAILED, cause)으로 wrapping -> throw
- 자식 포인트 변환:
  - HTTP: p99.points() -> kind=P99, rps.points() -> RPS, errorRate.points() -> ERROR_RATE. 각 메타가 null이거나 points가 null이면 skip
  - Hikari: active.points() -> ACTIVE, pending.points() -> PENDING, active.usageRatio() -> USAGE_RATIO

### Exception

MetricStoreErrorCode (com.example.pythia.metric.exception) — enum implements ErrorCode
- METRIC_PERSIST_FAILED("METRIC_STORE_001", "metric snapshot persistence failed")
- INVALID_SNAPSHOT_PAYLOAD("METRIC_STORE_002", "snapshot payload missing required fields")

MetricStoreException (com.example.pythia.metric.exception) — extends CustomException, Throwable cause까지 보존하는 생성자 사용

### 설정 변경

pythia/build.gradle (수정)
- implementation: org.springframework.boot:spring-boot-starter-data-jpa
- runtimeOnly: org.postgresql:postgresql
- testRuntimeOnly: com.h2database:h2

pythia/src/main/resources/application.yml (수정) — 다음 키 추가
- spring.datasource.url: jdbc:postgresql://localhost:5432/pythia
- spring.datasource.username: ${DB_USERNAME:pythia}
- spring.datasource.password: ${DB_PASSWORD:pythia}
- spring.datasource.driver-class-name: org.postgresql.Driver
- spring.jpa.hibernate.ddl-auto: update
- spring.jpa.properties.hibernate.dialect: org.hibernate.dialect.PostgreSQLDialect
- spring.jpa.properties.hibernate.format_sql: true
- spring.jpa.show-sql: false
- spring.jpa.open-in-view: false (Spring 권장, LAZY 외부 노출 방지)

pythia/src/main/resources/application-test.yml (신규)
- spring.datasource.url: jdbc:h2:mem:pythia;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE
- spring.datasource.driver-class-name: org.h2.Driver
- spring.datasource.username: sa
- spring.jpa.hibernate.ddl-auto: create-drop
- spring.jpa.properties.hibernate.dialect: org.hibernate.dialect.H2Dialect

pythia/src/main/resources/db/schema-postgres.sql (신규, 옵션 — 운영 수동 적용용)
- 5개 테이블 CREATE TABLE 문 명시 (ddl-auto와 동일 결과)
- 본 파일은 spring.sql.init이 활성화되지 않은 한 자동 실행되지 않음. 운영자가 ddl-auto=none으로 전환할 때 사용

docker/docker-compose.postgres.yml (신규)
- 서비스 1개: postgres:16-alpine, port 5432, volume postgres_data, env POSTGRES_DB=pythia / POSTGRES_USER=pythia / POSTGRES_PASSWORD=pythia
- 기존 docker/docker-compose.kafka.yml과 동급으로 분리 (CLAUDE.md "Docker 파일 규칙: /docker 경로에 생성" 준수)

### 수정 대상 파일

- com.example.pythia.kafka.consumer.JvmMetricSnapshotHandler — MetricStoreService 생성자 주입 추가, evaluator.evaluateJvm(snapshot) 호출 후 try { metricStoreService.save(snapshot); } catch (MetricStoreException e) { log.error(...); } 추가
- com.example.pythia.kafka.consumer.HttpMetricSnapshotHandler — 동일 패턴
- com.example.pythia.kafka.consumer.HikariMetricSnapshotHandler — 동일 패턴
- 코드 5줄 미만이라 인라인 try-catch 권장

### 수정하지 않는 파일 (제약/안전)
- 모든 *MetricSnapshotDto 및 하위 DTO
- SnapshotStatus, MetricStatus enum (entity에서 그대로 사용)
- *MetricSnapshotConsumer 클래스 (Handler만 수정)
- alert 패키지 전체
- email 패키지 전체
- common.exception 패키지

### 신규 테스트 파일

- pythia/src/test/java/.../metric/service/MetricStoreServiceImplTest.java (Mockito)
  - JVM 정상 스냅샷 -> JvmMetricSnapshotRepository.save 1회 호출, ArgumentCaptor로 entity 필드 매핑 검증
  - HTTP 정상 스냅샷 (p99/rps/errorRate 각 endpoint 2개) -> 자식 entity 6개 cascade 저장 확인
  - Hikari 정상 스냅샷 (active/pending/usageRatio 각 pool 2개) -> 자식 6개
  - 일부 메타 null인 부분 결측 케이스 -> snapshot 행은 저장되고 null 컬럼 보존, 자식 list 비어있는 메타는 skip
  - application/instance/collectedAt null -> MetricStoreException(INVALID_SNAPSHOT_PAYLOAD)
  - Repository에서 DataAccessException throw -> MetricStoreException(METRIC_PERSIST_FAILED) wrapping 검증
- pythia/src/test/java/.../metric/service/MetricStoreServiceImplPersistenceTest.java (@DataJpaTest + H2)
  - JVM/HTTP/Hikari 각 1건 저장 후 repository.findAll()로 row 1개 + 자식 row 정확히 매핑됨을 검증
  - 트랜잭션 commit 후 자식 테이블에 expected count 만큼 row 존재 (실 INSERT 검증)
  - @ActiveProfiles("test")로 application-test.yml 사용
- pythia/src/test/java/.../kafka/consumer/JvmMetricSnapshotHandlerTest.java (보강)
  - @Mock MetricStoreService 추가
  - 정상 스냅샷에서 metricStoreService.save(snapshot) 호출 verify
  - MetricStoreException 발생 시 listener 예외 propagate 안 됨 (assertThatNoException) + log.error 발생 verify (ListAppender)
  - FAILED/null 스냅샷에서는 store 미호출 verify
- HttpMetricSnapshotHandlerTest.java, HikariMetricSnapshotHandlerTest.java — 동일 패턴 보강

---

## 3. 데이터 흐름

```
Kafka topic (jvm.metrics.raw / http.metrics.raw / hikari.metrics.raw)
   |
   v
*MetricSnapshotConsumer  (@KafkaListener)  [기존, 변경 없음]
   |
   v
*MetricSnapshotHandler.handle(snapshot)
   |
   |-- status == FAILED || (cpu|p99|active) == null  ->  log.warn + return
   |
   |-- evaluator.evaluateXxx(snapshot)         [Task 016, 변경 없음]
   |
   '-- try {
         metricStoreService.save(snapshot)
       } catch (MetricStoreException e) {
         log.error("metric persist failed: app=... instance=...", e)
       }
                  |
                  v
       MetricStoreServiceImpl.save(...)  @Transactional
                  |
                  |-- application/instance/collectedAt null?
                  |     '-> throw MetricStoreException(INVALID_SNAPSHOT_PAYLOAD)
                  |
                  |-- Entity entity = toEntity(snapshot)  (null-safe 변환)
                  |
                  '-- repository.save(entity)
                          |
                          |-- 성공: log.debug + return (children도 cascade로 INSERT)
                          '-- DataAccessException -> throw MetricStoreException(METRIC_PERSIST_FAILED, cause)
```

JVM 평탄화 매핑 요약:
- snapshot.cpu().usagePercent() / measuredAt() / status() / missingReason() -> entity의 cpu* 컬럼 4개
- snapshot.memory().heapUsagePercent() / oldGenUsagePercent() / measuredAt() / status() / missingReason() -> entity의 heap*, oldGen*, memory* 컬럼
- snapshot.gc().avgDurationSeconds() / count() / measuredAt() / status() / missingReason() -> gc* 컬럼
- snapshot.thread().activeCount() / peakCount() / daemonCount() / measuredAt() / status() / missingReason() -> thread* 컬럼

HTTP 자식 매핑:
- snapshot.p99().points() 순회 -> HttpEndpointMetricPointEntity(kind=P99, endpoint, value, measuredAt) 생성
- snapshot.rps().points() 순회 -> kind=RPS
- snapshot.errorRate().points() 순회 -> kind=ERROR_RATE
- 각 메타가 null이면 해당 그룹 skip, 메타는 존재하나 points가 null/empty면 skip

Hikari 자식 매핑:
- snapshot.active().points() 순회 -> HikariPoolMetricPointEntity(kind=ACTIVE, pool, value, measuredAt)
- snapshot.pending().points() 순회 -> kind=PENDING
- snapshot.active().usageRatio() 순회 -> kind=USAGE_RATIO

테이블 ER 요약:
```
jvm_metric_snapshot           (id PK, application, instance, collected_at, status, cpu_*, memory_*, gc_*, thread_*)

http_metric_snapshot          (id PK, application, instance, collected_at, status, p99_*, rps_*, error_rate_*)
   '-- http_endpoint_metric_point (id PK, snapshot_id FK, kind, endpoint, value, measured_at)

hikari_metric_snapshot        (id PK, application, instance, collected_at, status, active_*, pending_*)
   '-- hikari_pool_metric_point  (id PK, snapshot_id FK, kind, pool, value, measured_at)
```

---

## 4. 예외 처리 전략

| 단계 | 상황 | 처리 |
|------|------|------|
| 부팅 | DataSource 연결 실패 (PostgreSQL 미기동) | Spring Boot 기본 동작 — 컨텍스트 로딩 실패 (의도, fail-fast). 회피하려면 docker compose로 PG 기동 후 앱 부팅 |
| 부팅 | ddl-auto=update가 schema 충돌 감지 | Hibernate 로그로 경고. 운영 마이그레이션은 후속 task에서 Flyway 도입 |
| 변환 | snapshot.application/instance/collectedAt null | Service에서 가드 -> MetricStoreException(INVALID_SNAPSHOT_PAYLOAD) throw. Handler가 catch + log.error |
| 변환 | 자식 메타 객체 (cpu/memory/gc/thread/p99/rps/errorRate/active/pending) 자체가 null | null-safe 변환 — entity 측 컬럼 모두 null 또는 자식 list 비움 |
| 변환 | 자식 list element 일부가 null | null element는 skip. NPE 방지 |
| 저장 | DataIntegrityViolationException (NOT NULL 제약 위반 등) | MetricStoreException(METRIC_PERSIST_FAILED, cause) wrapping -> Handler catch + log.error |
| 저장 | DataAccessResourceFailureException (DB 끊김) | 동일 wrapping. Kafka listener는 ack 정상 처리 (메시지 유실되나 본 task scope는 정상 케이스 동작 보장 — DLQ는 후속) |
| 저장 | 트랜잭션 롤백 후 후속 처리 | 호출 측은 예외만 인지. listener는 멈추지 않음 |
| 동시성 | 같은 snapshot 중복 수신 | unique 제약 미적용 (중복 row 허용). 멱등성은 후속 task |

핵심 원칙:
- 저장 단계는 Kafka listener를 막지 않는다. Service 내부 예외는 Handler에서 catch + log, 절대 listener까지 propagate X. Task 016 Evaluator의 처리 패턴과 동일.
- 부팅 시점 검증은 fail-fast. DB 미연결 상태로 앱이 떠서 메시지를 누락하는 것보다 부팅 실패가 안전.
- 저장 실패는 silent log. 본 task의 손실 정책은 "log + 다음 메시지 처리 계속". DLQ/retry는 후속.
- 트랜잭션 경계는 Service 메서드 단위. 한 snapshot은 자식 포인트와 함께 atomic 저장.

---

## 5. 검증 방법

### 빌드 / 단위 테스트
- ./gradlew :pythia:build -> BUILD SUCCESSFUL
- 위 §2 신규 테스트 파일 2종(Mockito + DataJpaTest) + 보강 3종(Handler) 모두 통과
- DataJpaTest에서 H2 PostgreSQL 호환 모드로 실 INSERT 검증

### 컨텍스트 로딩 검증
- 기존 PythiaApplicationTests 통과 (test profile -> H2로 fallback)
- 본 task에서 Spring Boot test를 띄울 때 @ActiveProfiles("test") 적용

### 통합 검증 (선택, 본 task 권장)
- 로컬 PostgreSQL 기동 후 ./gradlew :pythia:bootRun
- Argus(또는 mock producer)로 Kafka에 jvm.metrics.raw 메시지 1건 publish -> psql로 SELECT * FROM jvm_metric_snapshot row 1건 확인
- HTTP/Hikari 각각 동일 검증
- 자식 테이블 row count = 부모 entity의 list size 합

### Acceptance Criteria 매핑

| AC | 검증 방법 |
|----|----------|
| RDB에 정상적으로 테이블 생성 | bootRun 후 psql의 \d 또는 information_schema.tables 조회로 5개 테이블 확인 (jvm/http/hikari snapshot 3개 + http/hikari point 2개). DataJpaTest의 schema 자동 생성도 반증 |
| 이후 구현 Task에서 바로 사용 가능 | Repository 빈이 컨텍스트에 등록되고 findAll/findById 같은 기본 JPA 메서드로 접근 가능 (JpaRepository 상속). entity 필드가 DTO 모든 정보를 담고 있어 후속 task에서 query 메서드 추가만으로 사용 가능 |

### 정합성 검증
- DTO 필드 1:1 매핑이 entity에 모두 반영되었는지 코드 리뷰 시 1:1 대조표 작성
- ddl-auto=update로 생성된 컬럼 타입이 schema-postgres.sql과 일치하는지 (옵션 schema 파일 사용 시)

---

## 6. 트레이드오프

1. snapshot당 1행 + 자식 별도 테이블 vs JSONB 단일 컬럼 denormalize: JSONB는 schema-less로 단순하지만 후속 task가 endpoint별/pool별/시간 범위별 조회를 추가할 때 인덱싱 비용 증가, JPA mapping 복잡도 상승. 본 task 채택안(normalize)은 INSERT 비용이 약간 더 들지만 후속 query 비용을 낮춘다. 본 task scope에서는 query가 없으므로 양쪽 모두 가능. 채택 이유는 "관계형 보존이 가장 보편적 + JPA 매핑 자연스러움".

2. HTTP/Hikari 자식을 단일 테이블 + kind 컬럼 vs 별도 3개 테이블 (P99/RPS/ERROR_RATE 분리): 단일 테이블은 테이블 수 감소(2개 -> 6개 회피), 매핑 단순. 별도 테이블은 query 단순(WHERE kind=... 불필요)하지만 5종 메트릭에 5개 테이블은 과함. 본 plan은 단일 테이블 + kind 컬럼 채택. 후속 query에서 kind 인덱스 필요 시 추가.

3. @OneToMany 단방향 vs 양방향: 단방향은 mapping 코드 단순, FK는 자식 측에 명시. 양방향은 자식에서 부모 접근 가능하나 본 task scope에서 불필요. 단방향 채택, 양방향화는 후속 task가 필요해질 때.

4. ddl-auto=update vs validate + Flyway: update는 dev 편의성 최고, 운영에서는 schema drift 위험. 본 task는 task scope가 "테이블 정상 생성"이므로 update가 적절. 운영용 schema-postgres.sql 옵션 파일 동봉으로 후속 운영 task 시 ddl-auto=validate + sql.init 또는 Flyway로 전환 가능. CLAUDE.md 가이드("DDL은 ddl-auto로 생성하되 schema 파일도 옵션") 충족.

5. MetricStoreService 인터페이스 분리 vs 구현 단일 클래스: 인터페이스 + 구현 분리는 테스트 시 mocking 단순(@MockBean MetricStoreService), 후속 outbox/async store 도입 시 비용 절감. 클래스 1개 절약 < mock/대안 비용 절감 -> 분리 채택.

6. Service 메서드 3개 오버로드 vs 단일 save(Object) + instanceof 분기: 오버로드는 컴파일 타임 타입 안전, 호출 측 코드 명확. 단일 메서드는 호출 측이 단순해 보이나 instanceof 안티패턴. 오버로드 채택.

7. DTO -> Entity 변환을 Service 내부 private static vs 별도 Mapper 클래스 (예: MapStruct): 별도 Mapper는 코드 양 절감 + 테스트 분리 가능. 본 task scope에서 매핑 코드는 한 번 작성 후 변경 빈도 낮음 -> private static factory가 충분. MapStruct 의존성 추가 비용 < 생성 코드 가독성. 후속에서 매핑이 더 복잡해지면 분리.

8. null 컬럼 다수 허용 vs 부분 entity (Embeddable 분리): @Embeddable Cpu, @Embeddable Memory 등으로 그룹화하면 entity 가독성 상승, 관계 명확. 그러나 컬럼 수는 동일, 매핑 보일러플레이트 추가, JPA 학습 곡선. 본 plan은 평탄화 채택 — entity 1개 = 테이블 1개 = 컬럼 평탄화로 직관적. 후속에서 가독성 이슈가 생기면 Embeddable로 리팩터링.

9. PostgreSQL 단독 vs PostgreSQL + H2 동시 지원: H2를 default로 두면 dev 시 PostgreSQL 미기동 가능하나 운영과 dialect 차이로 false positive. 본 plan은 default=PostgreSQL, test profile=H2(PostgreSQL 호환 모드). 개발자가 로컬 PostgreSQL을 띄우는 것을 가정하며, 미기동 시 docker compose로 빠르게 기동.

10. 저장 호출 위치(Evaluator 후 vs 전): 전에 두면 평가 단계가 저장된 데이터를 활용 가능(예: 직전 N개 평균과 비교). 후에 두면 평가가 저장 실패에 영향 안 받음. 본 task는 평가 로직이 in-memory 카운터 기반이라 저장 데이터 의존 없음 -> "후" 위치가 안전. 후속에서 이력 기반 평가 도입 시 위치 재검토.

11. Handler에서 try-catch 인라인 vs Service 측 silent catch: Service 측에서 모든 예외를 catch하면 Handler가 단순해지나, "Service는 실패를 알리고 호출자가 결정" 원칙에 어긋남. Handler 측 try-catch는 명시적 의사결정 표현 + log 컨텍스트(snapshot 정보) 풍부. 본 plan은 Handler catch 채택.

12. open-in-view: false: Spring Boot 기본은 true. false로 두면 LAZY 로딩 외부 노출 방지(권장). 본 task에 직접 영향은 없지만 베스트 프랙티스 + 후속 Controller task에서 N+1 회피 부담 감소.

13. 인덱스 부재: range query 처리는 제외 범위라 보조 인덱스를 만들지 않음. 후속 조회 task에서 (application, collected_at DESC) 같은 인덱스 추가 필요. 본 task는 PK auto-index만.

14. 테스트 DB로 H2 PostgreSQL 호환 모드 vs Testcontainers PostgreSQL: Testcontainers는 운영 dialect와 100% 일치하나 빌드 환경 docker 의존성 + 빌드 시간 증가. 본 task는 H2 호환 모드로 충분(테이블 생성/INSERT/SELECT 검증 수준). 호환성 한계가 드러나면 후속에서 Testcontainers 도입.

---

## 핵심 파일 경로

신규 (절대 경로):
- C:\side_project\pythia\src\main\java\com\example\pythia\metric\domain\JvmMetricSnapshotEntity.java
- C:\side_project\pythia\src\main\java\com\example\pythia\metric\domain\HttpMetricSnapshotEntity.java
- C:\side_project\pythia\src\main\java\com\example\pythia\metric\domain\HikariMetricSnapshotEntity.java
- C:\side_project\pythia\src\main\java\com\example\pythia\metric\domain\HttpEndpointMetricPointEntity.java
- C:\side_project\pythia\src\main\java\com\example\pythia\metric\domain\HikariPoolMetricPointEntity.java
- C:\side_project\pythia\src\main\java\com\example\pythia\metric\domain\HttpMetricKind.java
- C:\side_project\pythia\src\main\java\com\example\pythia\metric\domain\HikariMetricKind.java
- C:\side_project\pythia\src\main\java\com\example\pythia\metric\repository\JvmMetricSnapshotRepository.java
- C:\side_project\pythia\src\main\java\com\example\pythia\metric\repository\HttpMetricSnapshotRepository.java
- C:\side_project\pythia\src\main\java\com\example\pythia\metric\repository\HikariMetricSnapshotRepository.java
- C:\side_project\pythia\src\main\java\com\example\pythia\metric\service\MetricStoreService.java
- C:\side_project\pythia\src\main\java\com\example\pythia\metric\service\MetricStoreServiceImpl.java
- C:\side_project\pythia\src\main\java\com\example\pythia\metric\exception\MetricStoreErrorCode.java
- C:\side_project\pythia\src\main\java\com\example\pythia\metric\exception\MetricStoreException.java
- C:\side_project\pythia\src\main\resources\application-test.yml
- C:\side_project\pythia\src\main\resources\db\schema-postgres.sql (옵션, 운영 수동 적용용)
- C:\side_project\docker\docker-compose.postgres.yml
- C:\side_project\pythia\src\test\java\com\example\pythia\metric\service\MetricStoreServiceImplTest.java
- C:\side_project\pythia\src\test\java\com\example\pythia\metric\service\MetricStoreServiceImplPersistenceTest.java

수정 (절대 경로):
- C:\side_project\pythia\build.gradle — JPA, postgresql driver, h2 testRuntime 추가
- C:\side_project\pythia\src\main\resources\application.yml — spring.datasource, spring.jpa 추가
- C:\side_project\pythia\src\main\java\com\example\pythia\kafka\consumer\JvmMetricSnapshotHandler.java — Store 주입 + 호출 + try-catch
- C:\side_project\pythia\src\main\java\com\example\pythia\kafka\consumer\HttpMetricSnapshotHandler.java — 동일
- C:\side_project\pythia\src\main\java\com\example\pythia\kafka\consumer\HikariMetricSnapshotHandler.java — 동일
- C:\side_project\pythia\src\test\java\com\example\pythia\kafka\consumer\JvmMetricSnapshotHandlerTest.java — Store mock + verify 보강
- C:\side_project\pythia\src\test\java\com\example\pythia\kafka\consumer\HttpMetricSnapshotHandlerTest.java — 동일
- C:\side_project\pythia\src\test\java\com\example\pythia\kafka\consumer\HikariMetricSnapshotHandlerTest.java — 동일

참조 (수정 없음):
- C:\side_project\docs\tasks\017-data-store.md — 본 task 명세
- C:\side_project\pythia\src\main\java\com\example\pythia\kafka\dto\jvm\*.java
- C:\side_project\pythia\src\main\java\com\example\pythia\kafka\dto\http\*.java
- C:\side_project\pythia\src\main\java\com\example\pythia\kafka\dto\hikari\*.java
- C:\side_project\pythia\src\main\java\com\example\pythia\kafka\dto\SnapshotStatus.java
- C:\side_project\pythia\src\main\java\com\example\pythia\kafka\dto\MetricStatus.java
- C:\side_project\pythia\src\main\java\com\example\pythia\common\exception\CustomException.java
- C:\side_project\pythia\src\main\java\com\example\pythia\common\exception\ErrorCode.java

---

## 후속 작업 (본 task 범위 외)
- 조회/range query 처리 (보조 인덱스 추가, Repository 메서드 확장)
- 멱등성 처리 (중복 메시지 dedup — 자연키 unique 제약 또는 messageKey 추적)
- DLQ / retry topic / 저장 실패 메시지 별도 토픽 publish
- Flyway/Liquibase 마이그레이션 도입, ddl-auto=validate 전환
- 데이터 보존 기간 정책 (TTL/파티셔닝)
- 집계 테이블 (시간/일 단위 rollup) — 본 task 제외 범위 명시
- LLM 분석을 위한 별도 컬럼/테이블 (분석 결과 캐시) — 본 task 제외 범위 명시
- Embeddable로 메트릭 그룹 묶기 리팩터링
- Testcontainers PostgreSQL 도입
- 운영 schema-postgres.sql 자동 적용 (sql.init 활성화) 또는 Flyway 전환
- multi-tenant 분리 (application별 schema/DB)

