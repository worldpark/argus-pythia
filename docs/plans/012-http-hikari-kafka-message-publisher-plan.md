# Task 012: HTTP / HikariCP 전용 Kafka Message Publisher 작성

## Context

Task 011에서 `HttpMetricSnapshotDto` / `HikariMetricSnapshotDto`와 두 어셈블러(`HttpMetricSnapshotAssembler`, `HikariMetricSnapshotAssembler`)가 만들어졌지만, 현재 Kafka 발행 경로는 `JvmMetricSnapshotPublisher` → `JvmMetricSnapshotProducer` → 토픽 `jvm.metrics.raw` 한 줄뿐이다. HTTP/Hikari 스냅샷은 어셈블만 되고 외부로 흘러나가지 못한다.

본 task는 JVM 발행 패턴을 그대로 미러링해 두 도메인용 Producer/Publisher 한 쌍씩을 추가하고, 각각 `http.metrics.raw`·`hikari.metrics.raw` 토픽으로 발행되도록 하는 것이다. 후속 task(스케줄러 확장, Pythia consumer)에서 그대로 호출 가능한 `publish()` 진입점을 제공하는 것이 성공 기준이다.

---

## 설계 방식 및 이유

### 방향: JVM 패턴 1:1 미러링 (도메인별 Producer/Publisher 분리)

| 결정 | 선택 | 이유 |
|------|------|------|
| 발행 단위 | **도메인별 Producer/Publisher 두 쌍 신설** | `JvmMetricSnapshotProducer`/`JvmMetricSnapshotPublisher`와 동일한 1:1 패턴. 컴파일 타임에 DTO ↔ 토픽이 묶여 잘못된 페이로드가 잘못된 토픽으로 가는 사고 차단 |
| 토픽 분리 | **`http.metrics.raw`, `hikari.metrics.raw` 별도 토픽** | Task 명세 그대로. 컨슈머(Pythia) 측에서 도메인별 분기·임계값 룰 적용이 단순해짐. 보존 기간/파티션 수도 도메인별로 조정 가능 |
| Partition Key | **`snapshot.application()` (null이면 `"unknown"`)** | JVM Publisher와 동일 규약. 같은 서비스의 메시지가 동일 파티션에 가서 시간순서 보장 |
| KafkaTemplate 주입 | **각 Producer가 `KafkaTemplate<String, *SnapshotDto>` 주입받는 형태** | 현 자동 구성(`JacksonJsonSerializer`) 환경에서 `KafkaTemplate`은 단일 빈으로 모든 페이로드 타입을 직렬화 가능 → 별도 빈 정의 없이 generic 타입만 선언해도 주입됨 (JVM Producer가 이미 동일 방식으로 동작 중) |
| 토픽 명 주입 | **application.yml `argus.kafka.topic.*` 키 추가 + `@Value`** | 기존 `argus.kafka.topic.metrics-raw` 패턴 미러링. 환경별(dev/staging/prod) 오버라이드 용이 |
| 실패 정책 | **Send 결과 future 반환 + 비동기 콜백 로깅만** | JVM Producer와 동일. SnapshotStatus.FAILED라도 일단 발행(컨슈머가 판단). 이후 Out-of-Scope 정책(재시도/DLQ)는 별 task |

### 트레이드오프
1. **Producer 클래스 3개로 증가**: 매번 도메인 추가 시 클래스 한 쌍을 더 만들어야 함. 그러나 generic Producer로 통합하면 토픽 라우팅 로직(`if instanceof`)이 생기고 컴파일 타임 안전성 상실 → 현 단계에선 명시적 분리가 더 안전.
2. **KafkaTemplate 자동 구성 의존**: Spring Boot 자동 구성이 `KafkaTemplate<Object,Object>` 단일 빈을 만들고, 제네릭 타입은 erasure 처리되어 주입이 동작. 이 동작을 JVM 측에서 이미 검증했으므로 동일 가정이 깨질 위험은 낮음. 향후 타입별 `KafkaTemplate` 빈을 명시 등록하면 더 견고해지나 본 task 범위 외.
3. **토픽 이름 하드코딩 vs 설정 외부화**: 설정 파일로 외부화. JVM과 일관성 우선.
4. **단일 통합 토픽(`metrics.raw`) 옵션 제외**: Pythia 측 라우팅 비용↑, 보존 정책 단일화 강제 → 도메인별 분리가 운영상 유리.
5. **트랜잭션 / Outbox 미적용**: Argus는 DB 저장 없이 Prometheus → Kafka pure pipeline이라 TX/Outbox 도입 동기 없음.

---

## 구성 요소

### 신규 파일

**`com.example.argus.messaging` 패키지**

| 파일 | 종류 | 역할 |
|------|------|------|
| `HttpMetricSnapshotProducer.java` | `@Component` | `KafkaTemplate<String, HttpMetricSnapshotDto>` 주입. `@Value("${argus.kafka.topic.http-metrics-raw}")` 토픽 주입. `send(serviceId, snapshot) → CompletableFuture<SendResult<...>>` 시그니처. `whenComplete`로 성공/실패 로깅 |
| `HikariMetricSnapshotProducer.java` | `@Component` | 동일 구조, `KafkaTemplate<String, HikariMetricSnapshotDto>` + `${argus.kafka.topic.hikari-metrics-raw}` |

**`com.example.argus.service.metric.snapshot` 패키지**

| 파일 | 종류 | 역할 |
|------|------|------|
| `HttpMetricSnapshotPublisher.java` | `@Service` | `HttpMetricSnapshotAssembler` + `HttpMetricSnapshotProducer` 주입. `publish()` → assemble 호출 → `application` 추출(null → `"unknown"`) → producer.send 위임 |
| `HikariMetricSnapshotPublisher.java` | `@Service` | 동일 구조, Hikari 측 |

### 수정 파일

| 파일 | 변경 |
|------|------|
| `src/main/resources/application.yml` | `argus.kafka.topic.http-metrics-raw: http.metrics.raw`, `argus.kafka.topic.hikari-metrics-raw: hikari.metrics.raw` 두 키 추가. 기존 `metrics-raw` 키는 유지(JVM 호환) |

### 수정하지 않는 파일 (제약/안전)
- `JvmMetricSnapshotPublisher`, `JvmMetricSnapshotProducer` — 기존 동작 회귀 차단
- `application.yml`의 기존 `spring.kafka.producer.*` 설정 — JacksonJsonSerializer 그대로 사용 (CLAUDE.md 규칙)
- DTO/Assembler — Task 011 결과물 그대로 사용

---

## 데이터 흐름

```
(후속) Scheduler@60s
   │
   ├─▶ HttpMetricSnapshotPublisher.publish()
   │     │
   │     ├─ HttpMetricSnapshotAssembler.assemble() → HttpMetricSnapshotDto
   │     │       (Task 011: P99/RPS Prometheus 조회 → SnapshotStatus 결정)
   │     │
   │     ├─ serviceId = snapshot.application() ?: "unknown"
   │     │
   │     └─ HttpMetricSnapshotProducer.send(serviceId, snapshot)
   │           │
   │           └─ KafkaTemplate.send("http.metrics.raw", serviceId, snapshot)
   │                 ├─ JacksonJsonSerializer 직렬화
   │                 └─ whenComplete 콜백: 성공 시 debug(offset), 실패 시 error 로깅
   │
   └─▶ HikariMetricSnapshotPublisher.publish()  (대칭, 토픽 = "hikari.metrics.raw")
```

JVM publisher 흐름은 그대로 유지되어 결과적으로 60초 주기당 **최대 3개 토픽**(`jvm.metrics.raw` 15초/4회 + `http.metrics.raw` + `hikari.metrics.raw`)이 발행된다.

---

## 예외 처리 전략

| 단계 | 상황 | 처리 |
|------|------|------|
| Assembler | 임의 RuntimeException | Publisher가 catch 없이 그대로 전파 (호출자/스케줄러가 결정). JVM Publisher와 동일 — `JvmMetricSnapshotPublisherTest.publish_assembler_예외_전파` 패턴 |
| Assembler | SnapshotStatus.FAILED | **여전히 발행**. 컨슈머 측에서 status 보고 무시/알림 결정. 본 task에서 필터링 안 함 (Task 006 8조 정책: PARTIAL/FAILED 모두 전송 가능) |
| Producer | `application` 라벨 null (전부 실패 등) | 키 `"unknown"`으로 발행. 동일 인스턴스 메시지가 한 파티션에 몰리는 부작용은 운영상 수용 |
| Producer | KafkaTemplate.send 동기 예외 (직렬화 실패 등) | future 반환 직전 raw throw → publisher가 그대로 전파 |
| Producer | 비동기 send 실패 (broker down/타임아웃) | `whenComplete`에서 `log.error(...)` — caller에는 future로 전달, swallow 하지 않음 |
| 직렬화 | DTO record가 Jackson 직렬화 불가 (예: 신규 필드 누락) | 동기 `SerializationException` → 예외 전파 + 로깅. 단위 테스트(직렬화 round-trip)에서 사전 검출됨 (Task 011 `*SnapshotDtoTest`) |

JVM 라인의 동작 보존이 핵심 원칙이며, 본 task는 새 제어 흐름을 도입하지 않는다.

---

## 검증 방법

### 단위 테스트 (JUnit 5 + Mockito)

기존 명명 규칙(한글 `_` 분리 + `@DisplayName`) 그대로 사용. JVM 테스트를 그대로 미러링.

| 테스트 클래스 | 커버 항목 |
|---------------|-----------|
| `HttpMetricSnapshotProducerTest` | `send` 호출 시 `KafkaTemplate.send`가 (`http.metrics.raw`, serviceId, snapshot) 인자로 호출됨 — `ArgumentCaptor` 3개 사용. `ReflectionTestUtils.setField`로 `@Value` 토픽 주입 |
| `HikariMetricSnapshotProducerTest` | 동일 — 토픽이 `hikari.metrics.raw`인지 검증 |
| `HttpMetricSnapshotPublisherTest` | (1) `application`이 있으면 그 값을 키로 send, (2) `application`이 null이면 `"unknown"` 키로 send, (3) Assembler 예외 시 그대로 전파 + producer 미호출 |
| `HikariMetricSnapshotPublisherTest` | 동일 3 시나리오 |

### 회귀 검증
- 기존 `JvmMetricSnapshotProducerTest`, `JvmMetricSnapshotPublisherTest`가 그대로 통과해야 함 (JVM 측 무수정).
- `./gradlew test` 전체 BUILD SUCCESSFUL 확인.

### 통합 검증 (수동, 선택 — 본 task 범위 외)
- `docker/docker-compose.kafka.yml` 기동 → Argus 앱 부팅
- `kafka-console-consumer --topic http.metrics.raw --from-beginning` 으로 발행 메시지 raw JSON 확인
- 메시지 구조에 `application`, `instance`, `collectedAt`, `p99.points[*].endpoint`, `rps.points[*].endpoint`, `status` 노출되는지 육안 확인
- Hikari 토픽도 동일하게 `active.points[*].pool` 확인

---

## 핵심 파일 경로

신규/수정 (절대 경로):
- `C:\side_project\argus\src\main\java\com\example\argus\messaging\HttpMetricSnapshotProducer.java` — 신규
- `C:\side_project\argus\src\main\java\com\example\argus\messaging\HikariMetricSnapshotProducer.java` — 신규
- `C:\side_project\argus\src\main\java\com\example\argus\service\metric\snapshot\HttpMetricSnapshotPublisher.java` — 신규
- `C:\side_project\argus\src\main\java\com\example\argus\service\metric\snapshot\HikariMetricSnapshotPublisher.java` — 신규
- `C:\side_project\argus\src\main\resources\application.yml` — 토픽 키 2개 추가
- `C:\side_project\argus\src\test\java\com\example\argus\messaging\HttpMetricSnapshotProducerTest.java` — 신규
- `C:\side_project\argus\src\test\java\com\example\argus\messaging\HikariMetricSnapshotProducerTest.java` — 신규
- `C:\side_project\argus\src\test\java\com\example\argus\service\metric\snapshot\HttpMetricSnapshotPublisherTest.java` — 신규
- `C:\side_project\argus\src\test\java\com\example\argus\service\metric\snapshot\HikariMetricSnapshotPublisherTest.java` — 신규

참조 (수정 없음, 패턴 원본):
- `JvmMetricSnapshotProducer.java` — Producer 패턴
- `JvmMetricSnapshotPublisher.java` — Publisher 패턴
- `JvmMetricSnapshotProducerTest.java`, `JvmMetricSnapshotPublisherTest.java` — 테스트 골격
- Task 011 결과물: `HttpMetricSnapshotDto`, `HikariMetricSnapshotDto`, 두 Assembler

## 후속 작업 (본 task 범위 외)
- Task 009 스케줄러를 60초 주기로 두 Publisher도 호출하도록 확장
- 토픽별 Kafka broker 설정(파티션 수, retention) — `docker/` compose 또는 운영 측에서 별도
- Pythia 측 두 토픽용 컨슈머 + 임계값 룰
- 운영 안정성 향상: SnapshotStatus.FAILED 발행 정책, 재시도/DLQ
