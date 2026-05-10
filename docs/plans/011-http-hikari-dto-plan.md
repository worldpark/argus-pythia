# Task 011: HTTP / HikariCP 메트릭 DTO 설계

## Context

`MetricType` enum에는 9개 PromQL이 정의되어 있지만, 현재 어셈블 단계에서 처리되는 것은 JVM 4종(CPU, 메모리, GC, 스레드)뿐이다. 나머지 4개(HTTP_P99_RESPONSE_TIME, HTTP_RPS, HIKARI_ACTIVE_CONNECTIONS, HIKARI_PENDING_CONNECTIONS)는 Task 005에서 PromQL만 선제적으로 등록되고 DTO/Assembler가 비어 있어, Prometheus → Kafka 파이프라인이 절반만 가동되는 상태다.

본 task의 목표는 `JvmMetricSnapshotDto` 패턴(record + sealed interface + 부분 실패 허용)을 그대로 미러링해서 HTTP·HikariCP 측 스냅샷 DTO와 매핑 변환 구조를 작성하는 것이다. 단, JVM 메트릭은 **단일 시계열**인 반면 HTTP는 `uri`별, HikariCP는 `pool`별 **다수 시계열**을 반환한다는 점이 핵심 차이이며, 이를 수용하기 위해 매퍼와 결과 타입을 확장한다.

성공 시 후속 task(스케줄러, 별도 Kafka 토픽, Pythia 분석)에서 그대로 소비할 수 있는 record 구조가 갖춰진다.

---

## 설계 방식 및 이유

### 방향: JVM 패턴 미러링 + 멀티-시계열 확장

| 결정 | 선택 | 이유 |
|------|------|------|
| Snapshot 단위 | **HTTP / Hikari 별도 record** | 폴링 주기는 같지만 도메인이 분리되며, 후속 토픽 분리·소비자 분기가 깔끔함. JVM 1:1 패턴 유지 |
| HTTP 모델링 | **endpoint(uri)별 List 임베드** | PromQL이 이미 uri별로 분리(`sum by ... uri ...`)하므로 endpoint 단위 정보 보존이 본 메트릭의 본질 |
| Hikari 모델링 | **pool별 List 임베드** | HTTP와 일관된 멀티-포인트 표현. 멀티 DataSource 환경(테스트/리포팅용 추가 풀) 대응 |
| Mapper 확장 | **`MetricPointMapper.toPoints(...)` 추가** | PrometheusResponse 파싱 로직(BigDecimal 타임스탬프, NaN/Inf 거부)을 재사용. 별도 클래스로 분리하면 동일 로직 중복 |
| 결과 타입 | **신규 `MultiMappingResult` sealed interface** | 기존 `MappingResult.Success`가 단일 `MetricPointDto`만 보유하므로 List 보유 변형이 필요 |
| 도메인 분리 | **`HttpMetricResult`, `HikariMetricResult` 별도 sealed interface** | `JvmMetricResult`와 동일한 계약(상태/측정시각/missingReason) 제공하면서 permits 제약을 도메인별로 분리 |

### 트레이드오프
1. **신규 sealed interface 2개 추가**: 코드량 증가하지만 JVM과 동일한 정적 보장(컴파일 타임 분기 누락 차단)을 도메인별로 얻음.
2. **MetricPointMapper 비대화**: 단일/다중 두 진입점이 한 파일에 공존 → 단일 책임은 약화되지만 PrometheusResponse 파싱 규칙 일관성이 더 중요.
3. **MetricStatus / SnapshotStatus 재사용**: 기존 enum을 그대로 사용. PARTIAL/EMPTY_RESULT 등이 도메인 의미와 충돌 없음.
4. **HTTP_RPS의 application/instance 부재**: PromQL이 `sum by (uri)`만 있어 application/instance 라벨이 사라짐 → Snapshot의 application/instance는 HTTP_P99 결과로만 채워질 수 있고 P99 실패 시 null 가능. JVM 어셈블러의 LabelAccumulator와 동일한 한계로 수용.
5. **Producer는 본 task 범위 외**: 기존 `JvmMetricSnapshotProducer`가 `KafkaTemplate<String, JvmMetricSnapshotDto>`로 타입 고정 → HTTP/Hikari Producer는 후속 task에서 별도 추가(혹은 generic 일반화). DTO는 Jackson 직렬화 가능 record로 두기만 한다.

---

## 구성 요소

### 신규 파일

**`com.example.argus.dto.metric` 패키지**

| 파일 | 종류 | 역할 |
|------|------|------|
| `EndpointMetricPointDto.java` | record | HTTP 단일 포인트: `endpoint`, `value`, `measuredAt` |
| `PoolMetricPointDto.java` | record | Hikari 단일 포인트: `pool`, `value`, `measuredAt` |
| `HttpResponseTimeDto.java` | record, implements `HttpMetricResult` | `List<EndpointMetricPointDto> points`, `measuredAt`, `status`, `missingReason` + factory(`from`/`success`/`empty`/`queryFailed`/`parseFailed`) |
| `HttpThroughputDto.java` | record, implements `HttpMetricResult` | 동일 구조, RPS 의미 |
| `HikariActiveDto.java` | record, implements `HikariMetricResult` | `List<PoolMetricPointDto> points`, … |
| `HikariPendingDto.java` | record, implements `HikariMetricResult` | 동일 구조 |
| `HttpMetricSnapshotDto.java` | record | `application`, `instance`, `collectedAt`, `p99(HttpResponseTimeDto)`, `rps(HttpThroughputDto)`, `status(SnapshotStatus)` |
| `HikariMetricSnapshotDto.java` | record | `application`, `instance`, `collectedAt`, `active(HikariActiveDto)`, `pending(HikariPendingDto)`, `status(SnapshotStatus)` |
| `HttpMetricResult.java` | sealed interface permits `HttpResponseTimeDto, HttpThroughputDto` | `JvmMetricResult`와 동일 계약 (`status()`, `measuredAt()`, `missingReason()`) + 정적 `from(MultiMappingResult, ...)` 헬퍼 |
| `HikariMetricResult.java` | sealed interface permits `HikariActiveDto, HikariPendingDto` | 동일 |

**`com.example.argus.service.metric.snapshot` 패키지**

| 파일 | 역할 |
|------|------|
| `HttpMetricSnapshotAssembler.java` | `JvmMetricSnapshotAssembler`와 동일한 `resolve` / `withLabels` / `LabelAccumulator` 패턴. P99·RPS 조립 |
| `HikariMetricSnapshotAssembler.java` | Active·Pending 조립. 둘 다 application/instance 라벨 보존되므로 단순 |

### 수정 파일

| 파일 | 변경 |
|------|------|
| `com.example.argus.service.metric.mapper.MetricPointMapper` | (1) 신규 `MultiMappingResult` sealed interface(`Success(List<...>)`, `Empty`, `ParseFailed`, `QueryFailed`). (2) 신규 `toPoints(PrometheusResponse, MetricType, String identifierLabel)` 메서드 — 기존 `toPoint` 로직을 재활용해 모든 result 항목을 순회, identifierLabel(예: "uri", "pool")을 기준으로 endpoint/pool 식별자 추출. (3) 기존 `toPoint`/`MappingResult` 그대로 유지(JVM 어셈블러 무수정) |

### 수정하지 않는 파일 (제약)
- `PrometheusResponse` — 제약상 금지
- `MetricType` (enum) — PromQL 수정 금지
- 기존 JVM DTO들과 `JvmMetricSnapshotAssembler` — 회귀 위험 차단

---

## 데이터 흐름

```
Scheduler (후속)
   │
   ▼
HttpMetricSnapshotAssembler.assemble()
   │  ├─ queryService.queryByMetric(HTTP_P99_RESPONSE_TIME)  → PrometheusResponse
   │  │     └─ MetricPointMapper.toPoints(resp, type, "uri") → MultiMappingResult.Success(List<EndpointMetricPointDto>)
   │  │           └─ HttpResponseTimeDto.from(result)  → status SUCCESS / EMPTY / QUERY_FAILED / PARSE_FAILED
   │  │
   │  ├─ queryService.queryByMetric(HTTP_RPS)
   │  │     └─ toPoints(resp, type, "uri") → HttpThroughputDto.from(result)
   │  │
   │  ├─ LabelAccumulator: P99 첫 Success로부터 application/instance 추출 (RPS는 라벨 부재)
   │  │
   │  └─ SnapshotStatus.from(List.of(p99.status(), rps.status()))
   │         → COMPLETE / PARTIAL / FAILED
   ▼
HttpMetricSnapshotDto
   │
   ▼
(후속 Task: HttpMetricSnapshotProducer → Kafka topic)
```

Hikari 측은 동일한 흐름이며 identifierLabel만 `"pool"`로 바뀌고, 두 메트릭 모두 application/instance 라벨이 보존된다.

---

## 예외 처리 전략

기존 `JvmMetricSnapshotAssembler.resolve(...)` 패턴을 그대로 차용한다.

| 상황 | 처리 |
|------|------|
| `PrometheusQueryException` (HTTP/네트워크/Status fail) | `*.queryFailed(message)` → `MetricStatus.QUERY_FAILED` |
| `MultiMappingResult.Empty` (result[] 비어있음) | `*.empty()` → `MetricStatus.EMPTY_RESULT` |
| 값 파싱 오류 (NaN/Inf/숫자 변환 실패) | `MultiMappingResult.ParseFailed` → `*.parseFailed(reason)` → `MetricStatus.PARSE_FAILED` |
| identifier 라벨 없음 (예: HTTP인데 uri 라벨이 없는 시리즈) | 해당 항목만 스킵하고 로그 경고. 모든 항목 누락 시 `ParseFailed` |
| 기타 RuntimeException | `log.error` + `*.parseFailed(message)` |
| 부분 실패 (P99만 성공 / Active만 성공 등) | Snapshot은 생성, `SnapshotStatus.PARTIAL` |
| 전부 실패 | `SnapshotStatus.FAILED` (Producer 측에서 전송 여부 결정 — 후속 task) |

`HttpResponseTimeDto.from(MultiMappingResult)`은 `JvmMetricResult.from(...)` 정적 헬퍼와 동일한 분기 패턴(`instanceof Success / Empty / ParseFailed / QueryFailed`)을 사용한다. List가 비어 있으면 `Empty`로 강등한다.

---

## 검증 방법

### 단위 테스트 (JUnit 5 + Mockito)

기존 명명 규칙(`from_<상황>_returns<DTO상태>()`) 준수. 패키지 미러링.

| 테스트 클래스 | 커버 항목 |
|---------------|-----------|
| `MetricPointMapperTest` (보강) | `toPoints` — multi result 파싱 / identifier 라벨 정상 추출 / 일부 NaN-Inf 항목 스킵 / 빈 result → Empty / 라벨 누락 항목 처리 |
| `EndpointMetricPointDtoTest`, `PoolMetricPointDtoTest` | record 동등성, Jackson 직렬화 (JavaTimeModule 포함) |
| `HttpResponseTimeDtoTest` / `HttpThroughputDtoTest` | `from(MultiMappingResult)` 4분기 + List 비어있을 때 EMPTY 강등 |
| `HikariActiveDtoTest` / `HikariPendingDtoTest` | 동일 |
| `HttpMetricSnapshotDtoTest` / `HikariMetricSnapshotDtoTest` | record + Jackson round-trip, `SnapshotStatus.from(...)` 검증 |
| `HttpMetricSnapshotAssemblerTest` | `PrometheusQueryService` Mockito 스텁 — COMPLETE / PARTIAL(P99 fail / RPS fail) / FAILED / 일부 EMPTY / `PrometheusQueryException` 시나리오 |
| `HikariMetricSnapshotAssemblerTest` | 동일 |

### 회귀 검증
- 기존 `JvmMetricSnapshotAssemblerTest`, `MetricPointMapperTest`(toPoint), 모든 JVM `*DtoTest`가 그대로 통과해야 함 (`MetricPointMapper` 수정은 add-only).

### End-to-end (수동, 선택)
- Prometheus(localhost:9090)에 actuator 메트릭 노출된 더미 Spring Boot 앱 띄움
- Assembler bean을 임시 컨트롤러에서 호출 → 반환 record를 ObjectMapper로 직렬화 → JSON 구조 육안 확인
- 실제 Kafka 전송은 본 task 범위 외 (후속 Producer task)

---

## 핵심 파일 경로

수정/참조 대상 (절대 경로):
- `C:\side_project\argus\src\main\java\com\example\argus\service\metric\mapper\MetricPointMapper.java` ← `toPoints`/`MultiMappingResult` 추가
- `C:\side_project\argus\src\main\java\com\example\argus\dto\metric\` ← 신규 record 8개 + sealed interface 2개
- `C:\side_project\argus\src\main\java\com\example\argus\service\metric\snapshot\` ← 신규 Assembler 2개
- 참조 (수정 없음):
  - `JvmMetricSnapshotAssembler.java` (패턴 원본)
  - `JvmMetricResult.java`, `GcMetricDto.java` (sealed interface + 다중 결과 조합 패턴)
  - `MetricStatus.java`, `SnapshotStatus.java` (재사용 enum)
  - `PrometheusQueryService.java` (`queryByMetric` 시그니처)

## 후속 작업 (본 task 범위 외)
- HTTP/Hikari Snapshot용 Kafka Producer 클래스 + 토픽 (예: `http.metrics.raw`, `hikari.metrics.raw`) 또는 기존 producer 일반화
- 60초 주기 스케줄러 (Task 009 확장)
- Pythia 측 컨슈머 / 임계값 룰
