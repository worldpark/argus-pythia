# 023. Argus Prometheus/Kafka 완충용 Redis 버퍼 연동 - Plan

> Source Task: docs/tasks/023-kafka-prometheus-redis-buffer.md
> Target: argus
> 본 Plan 은 **Fallback-Only-On-Failure** 패턴으로, 정상 경로는 기존 Kafka publish 를 그대로 두고 Kafka publish 가 실패했을 때에만 Redis 에 적재하여 후속 재시도를 보장한다.
> 가상 스레드 활성화, Prometheus 병렬화, WebClient→RestClient 교체, Redis HA/Sentinel 인프라는 본 Task 범위 외.

---

## 0. 도메인 컨텍스트와 패턴 결정 근거
- Argus + Pythia 는 **장애 감지/알람 대응 시스템** 이다. 알람 latency 가 SLA 의 핵심이며, 단일 메트릭의 중복은 downstream dedup 으로 흡수 가능.
- 후보 패턴:
  1) **Always-Buffer-First (Outbox)** — 모든 스냅샷을 Redis 에 먼저 적재 후 drain 워커가 Kafka 로 송신.
  2) **Fallback-Only-On-Failure** — 정상 경로는 기존 Kafka publish 유지, 실패한 스냅샷만 Redis 에 적재 후 drain 워커가 재배출. ✅ **채택**
- 채택 근거:
  - 정상 경로의 Kafka send latency 보존 → 장애 발생 시 알람 도달이 가장 빠른 경로 유지.
  - Redis 가 hot-path 밖에 위치 → Redis 자체 장애가 정상 알람 흐름에 영향 0. 장애 감지 시스템의 자가 안정성.
  - Task 의 1차 목표("Kafka 일시 실패 시 유실 방지") 와 의미적으로 정확히 일치.
  - 중복 발송 위험이 outbox 보다 작음(모든 메시지가 아닌 실패 케이스만 노출).
  - 기존 Publisher/Producer 시그니처 변경 불필요 → 회귀 테스트 영향 최소.

---

## 1. 설계 방식 및 이유

### 1.1 패턴: Fallback-Only-On-Failure
- 정상 흐름: `Scheduler → Publisher → Producer.send(Kafka)` (현행 유지).
- 실패 흐름: `Producer.send` 가 반환한 `CompletableFuture` 가 예외 완료되면 → Publisher 내부의 `whenComplete` callback 이 **`MetricBufferService.enqueueOnFailure(type, snapshot)`** 호출 → Redis ZSET 에 적재.
- 재배출: 별도 `@Scheduled` 워커(`MetricBufferDrainScheduler`)가 주기적으로 ZSET 을 peek → `Producer.send` 호출 → ack 성공 시 ZREM, 실패 시 잔존.
- "fallback 적재 책임" 은 Publisher 에 두고, Scheduler 는 단순 trigger 로 유지(단일 책임 원칙).
- Publisher 의 `publish()` 시그니처(`CompletableFuture<SendResult<…>>`) 는 그대로 유지 → 기존 Scheduler 로그 callback 도 그대로 동작.

### 1.2 Redis 자료구조: Sorted Set per 메트릭 타입
- key: `argus:buffer:{jvm|http|hikari}` (3개 키)
- score: enqueue epoch millis (단조 증가)
- member: JSON 직렬화된 `BufferedSnapshotEnvelope` (UUID id + snapshot payload + 직렬화 타입 검증용 type 태그)
- 선택 근거:
  - **List**: ack 받은 후 정확한 항목 제거가 LREM O(N) + 별도 in-flight 리스트가 필요 → 운영 부담 큼.
  - **Stream (XADD/XREADGROUP/XACK)**: consumer-group 보장이 강력하지만 본 Task 단일 인스턴스/단일 컨슈머 가정에 비해 과도. group/PEL 관리 부담.
  - **ZSET**: score 로 TTL 만료 일괄 제거(`ZREMRANGEBYSCORE`), 오래된 순 peek(`ZRANGE`), 정확한 멤버 제거(`ZREM`) 가 한 자료구조로 자연스럽게 표현 가능. 본 패턴(낮은 enqueue rate, 정확한 ack 후 제거) 에 최적.
- 메트릭 타입별로 키를 분리 → envelope 직렬/역직렬 시 타입 분기가 단순(키 자체가 타입을 식별). DTO 가 다른 3종을 단일 키에 섞지 않음.

### 1.3 TTL 적용 방식
- Redis 의 per-member TTL 은 미지원 → **score 기반 만료** 사용.
- Drain 워커가 매 사이클 시작 시 `ZREMRANGEBYSCORE key 0 (now - ttlMillis)` 실행 → 만료 항목 일괄 폐기.
- ZSET 키 자체에 보조적으로 `EXPIRE key (ttlMs * 2 / 1000)` 를 걸어 장기 미사용 시 키 누수 방지.
- 만료 항목은 **Kafka 로 재발송되지 않고 폐기** 한다(60초 메트릭의 수 분 이전 데이터는 알람 가치 없음). 폐기 건수는 INFO 로 노출.

### 1.4 길이 상한 (Overflow)
- enqueue 직후 `ZCARD > maxSize` 면 `ZREMRANGEBYRANK key 0 (ZCARD - maxSize - 1)` 로 오래된 항목부터 drop.
- 정책 키 `argus.buffer.overflow-policy: DROP_OLDEST | DROP_NEWEST | REJECT` 로 외부 설정. **기본 `DROP_OLDEST`** (장애 감지 시스템에서는 최신 상태가 더 가치 있음).
- `REJECT` 선택 시 `MetricBufferException` 을 던지지만, 호출자(Publisher) 는 이를 잡아 WARN 로그만 남기고 정상 종료(아래 4.2).

### 1.5 중복 전송 가정: at-least-once
- 실패 → fallback 적재 → drain → ack 회색 영역(broker 도달 후 ack 미수신) 에서 중복 가능.
- 본 Task 는 **at-least-once** 를 가정. envelope 의 `id` 가 UUID 로 부여되어 downstream(Pythia) 이 향후 멱등 처리에 활용 가능. 본 Task 에서 Pythia 측 변경은 없음.

### 1.6 Redis 장애 시 동작 정책
| 단계 | Redis 장애 시 동작 |
|---|---|
| Fallback enqueue | 호출자(Publisher) 가 `MetricBufferException` 을 잡아 ERROR 로그만 남김. 해당 1회 스냅샷 유실 허용. **정상 경로는 무영향**(Redis 호출은 실패 callback 안에서만 발생). |
| Drain peek/remove | 해당 사이클 skip + ERROR 로그. 다음 사이클에서 자동 재시도. |
| Redis 재시작 후 데이터 유실 | AOF (`--appendonly yes`) 단일 인스턴스 기준 마지막 1초 분량 손실 가능. 본 Task 의 "단기 완충" 범위 내 허용. |

### 1.7 직렬화
- 기존 Kafka 측 `JacksonJsonSerializer` 가 사용하는 ObjectMapper 빈을 그대로 주입하여 직렬화 규칙 일관성 유지.
- envelope String → StringRedisTemplate 의 ZSetOperations 로 적재.
- payload 는 envelope 안에 JSON 문자열로 중첩(타입 변경 시 envelope 포맷은 안정 유지).

### 1.8 단순화 결정
- Resilience4j Retry/CircuitBreaker 미적용. Drain 워커의 주기적 재실행이 자체로 backoff 역할.
- Redisson/분산락 미적용. Argus 단일 인스턴스 가정.
- Spring Boot `spring-boot-starter-data-redis` (Lettuce) 만 의존성 추가 → CLAUDE.md "Redis/Redisson Notes" 의 starter 제약 자동 부합.

---

## 2. 구성 요소

### 2.1 신규 파일

| 경로 | 책임 |
|---|---|
| `argus/src/main/java/com/example/argus/redis/config/RedisConfig.java` | StringRedisTemplate Bean 등록. `@ConditionalOnBean(RedisConnectionFactory.class)`. pythia 패턴 동일. |
| `argus/src/main/java/com/example/argus/config/MetricBufferProperties.java` | `@ConfigurationProperties(prefix = "argus.buffer")`. ttl, maxSize, overflowPolicy, drainBatchSize, drainIntervalMs, keyPrefix. |
| `argus/src/main/java/com/example/argus/dto/metric/buffer/BufferedSnapshotEnvelope.java` | record(id, enqueuedAtEpochMs, type, payloadJson). |
| `argus/src/main/java/com/example/argus/dto/metric/buffer/MetricBufferType.java` | enum(JVM, HTTP, HIKARI). keySuffix 보유. |
| `argus/src/main/java/com/example/argus/service/metric/buffer/MetricBufferStore.java` | StringRedisTemplate ZSetOperations 래퍼. enqueue / peekOldest / remove / evictExpired / size. Redis 직접 접근 계층. |
| `argus/src/main/java/com/example/argus/service/metric/buffer/MetricBufferService.java` | enqueueOnFailure(type, dto), peekBatch(type), removeOnAck(type, member). 직렬/역직렬 + 정책(overflow) 적용. |
| `argus/src/main/java/com/example/argus/service/metric/buffer/MetricBufferDrainService.java` | type 별 peek → 해당 Producer.send → 성공 시 remove. 실패 시 보존. |
| `argus/src/main/java/com/example/argus/scheduler/MetricBufferDrainScheduler.java` | `@Scheduled(fixedDelayString = "${argus.buffer.drain-interval-ms}")` 로 `DrainService.drainAll()` 호출. 기존 SchedulingConfig 의 `@EnableScheduling` 조건에 자동 종속. |
| `argus/src/main/java/com/example/argus/exception/MetricBufferException.java` | `RuntimeException` 상속. Redis/직렬화 실패 래핑. |

### 2.2 수정 파일

| 경로 | 변경 내용 |
|---|---|
| `argus/build.gradle` | `implementation 'org.springframework.boot:spring-boot-starter-data-redis'` 추가. |
| `argus/src/main/resources/application.yml` | `spring.data.redis.host/port/timeout`, `argus.buffer.*` 추가. |
| `argus/src/test/resources/application-test.yml` | `spring.autoconfigure.exclude` 에 `RedisAutoConfiguration`, `RedisRepositoriesAutoConfiguration` 추가. `argus.scheduling.enabled: false` 는 유지. |
| `argus/src/main/java/com/example/argus/service/metric/snapshot/JvmMetricSnapshotPublisher.java` | `producer.send(...)` 가 반환한 future 에 `whenComplete((r, ex) -> { if (ex != null) bufferService.enqueueOnFailure(JVM, snapshot); })` 추가. 시그니처 유지. |
| `argus/src/main/java/com/example/argus/service/metric/snapshot/HttpMetricSnapshotPublisher.java` | 동일 패턴(`HTTP`). |
| `argus/src/main/java/com/example/argus/service/metric/snapshot/HikariMetricSnapshotPublisher.java` | 동일 패턴(`HIKARI`). |

**`MetricSnapshotScheduler` 는 변경 없음** — Publisher 가 fallback 책임을 내부 callback 으로 흡수하므로 Scheduler 의 로그 callback 은 그대로 둔다.

### 2.3 신규 테스트

| 경로 | 검증 |
|---|---|
| `argus/src/test/java/com/example/argus/service/metric/buffer/MetricBufferStoreTest.java` | Mockito mock StringRedisTemplate + ZSetOperations 로 enqueue→ZADD, peek→ZRANGE, remove→ZREM, evictExpired→ZREMRANGEBYSCORE, overflow→ZREMRANGEBYRANK, EXPIRE 호출 검증. |
| `argus/src/test/java/com/example/argus/service/metric/buffer/MetricBufferServiceTest.java` | enqueueOnFailure 정상 흐름, Jackson 직렬화 실패 → `MetricBufferException`, overflow=REJECT 시 예외 동작, peekBatch 결과 역직렬화 검증. |
| `argus/src/test/java/com/example/argus/service/metric/buffer/MetricBufferDrainServiceTest.java` | evictExpired 우선 호출, peek → producer.send → ack 성공 future → remove 호출, ack 실패 future → remove 미호출, 역직렬화 실패 member → 즉시 remove + WARN(poison-pill 회피). |
| 기존 `JvmMetricSnapshotPublisherTest` / `HttpMetricSnapshotPublisherTest` / `HikariMetricSnapshotPublisherTest` 수정 | future 가 정상 완료 → bufferService 미호출 검증. future 가 예외 완료 → `bufferService.enqueueOnFailure` 호출 검증. 기존 producer.send 호출 검증은 그대로 유지. |
| 기존 `MetricSnapshotSchedulerTest` | **수정 불필요** (Scheduler 미변경). |

### 2.4 패키지 배치
- `service/metric/buffer/` 하위에 Store/Service/DrainService 동거. argus 의 기존 `service/metric/snapshot/` 패턴과 일치.
- `MetricBufferStore` 는 Redis 직접 접근이지만 argus 에 `repository/` 트리가 없으므로 클래스명(`Store`) 으로 외부연동 계층 표시. Pythia `ViolationStateStore` 와 동일 컨벤션.
- `MetricBufferDrainScheduler` 만 별도 `scheduler/` 패키지(argus 에 아직 없으면 신설). 기존 `MetricSnapshotScheduler` 는 `service/metric/snapshot/` 위치이지만 본 Task 에서 scheduler 디렉터리를 새로 만들지는 않고, **기존 위치와 일관성 유지를 위해 `MetricBufferDrainScheduler` 도 `service/metric/buffer/` 하위에 둔다**. (Argus 내부 컨벤션 통일성이 패키지 분리보다 우선.)

---

## 3. 데이터 흐름

### 3.1 정상 경로 (기존, 변경 없음)
```
MetricSnapshotScheduler.triggerSnapshot()
  ├─ JvmMetricSnapshotPublisher.publish()
  │    ├─ assembler.assemble() → JvmMetricSnapshotDto
  │    ├─ future = producer.send(serviceId, dto)
  │    ├─ future.whenComplete((r, ex) -> { if (ex != null) bufferService.enqueueOnFailure(JVM, dto); })  // 신규
  │    └─ return future
  ├─ HttpMetricSnapshotPublisher.publish()  (동일 패턴)
  └─ HikariMetricSnapshotPublisher.publish()  (동일 패턴)
```
- 정상 ack 시: fallback callback 분기 미진입 → Redis 호출 0.
- 정상 경로 latency 변화 0.

### 3.2 실패 경로 (Kafka publish 실패 시)
```
producer.send 가 반환한 future 가 예외 완료
  └─ Publisher 내부 whenComplete:
       └─ bufferService.enqueueOnFailure(type, snapshot)
            ├─ envelope = new BufferedSnapshotEnvelope(UUID, nowMs, type, ObjectMapper.writeValueAsString(snapshot))
            ├─ envelopeJson = ObjectMapper.writeValueAsString(envelope)
            └─ store.enqueue(type, envelopeJson, nowMs)
                 ├─ ZADD argus:buffer:{type} <nowMs> <envelopeJson>
                 ├─ overflow 처리:
                 │     DROP_OLDEST : if ZCARD > maxSize → ZREMRANGEBYRANK 0 (ZCARD-maxSize-1)
                 │     DROP_NEWEST : if ZCARD > maxSize → ZREM 신규 envelope
                 │     REJECT      : if ZCARD > maxSize → throw MetricBufferException
                 └─ EXPIRE argus:buffer:{type} (ttlMs * 2 / 1000)  // 키 누수 방지
```

### 3.3 Drain 경로
```
MetricBufferDrainScheduler  (fixedDelay = argus.buffer.drain-interval-ms)
  └─ MetricBufferDrainService.drainAll()
       for each MetricBufferType (JVM, HTTP, HIKARI):
         try:
           ├─ store.evictExpired(type, nowMs - ttlMs)  → ZREMRANGEBYSCORE
           ├─ List<String> members = store.peekOldest(type, drainBatchSize)  → ZRANGE 0 (batchSize-1)
           └─ for each member:
                try:
                  envelope = ObjectMapper.readValue(member, BufferedSnapshotEnvelope.class)
                  dto      = ObjectMapper.readValue(envelope.payloadJson(), type.dtoClass())
                  future   = producerOf(type).send(serviceIdOf(dto), dto)
                  future.whenComplete((r, ex) -> {
                    if (ex == null) store.remove(type, member);  // ZREM
                    else            log.warn("drain send failed, will retry");
                  });
                catch (JsonProcessingException e):
                  store.remove(type, member); log.warn("poison-pill removed");
         catch (DataAccessException | MetricBufferException e):
           log.error("drain cycle failed for {}", type, e);
```

### 3.4 상태 전이
| 사건 | 상태 변화 |
|---|---|
| Kafka publish 성공 | ZSET 변동 없음 (애초에 적재 안 됨) |
| Kafka publish 실패 → fallback enqueue | envelope ∈ ZSET |
| enqueue 시 ZCARD > maxSize, DROP_OLDEST | 가장 score 작은 항목 제거, 신규 유지 |
| Drain peek + Kafka ack | envelope ∉ ZSET |
| Drain peek + Kafka 실패 | envelope 잔존, 다음 사이클 재시도 |
| TTL 만료 (score < now - ttl) | drain 사이클의 evictExpired 단계에서 폐기 (Kafka 송신 없이) |
| Redis 장애 (fallback enqueue 측) | Publisher 가 잡아 ERROR 로그. 1회 유실 허용. 정상 경로 무영향. |
| Redis 장애 (drain 측) | 해당 사이클 skip, 다음 사이클 재시도 |

---

## 4. 예외 처리 전략

### 4.1 신규 예외
- `MetricBufferException extends RuntimeException`
  - cause 보존, 메시지 prefix `metric-buffer:` 로 grep 용이.
  - 발생 지점: Jackson 직렬화 실패, Redis ZADD/ZREM 실패(DataAccessException 래핑), overflow=REJECT 정책 적용 시.

### 4.2 Fallback enqueue 단계 정책 (fail-open)
- Publisher 의 `whenComplete` callback 안에서 `bufferService.enqueueOnFailure` 가 `MetricBufferException` 을 던지면 **callback 안에서 잡아 ERROR 로그만 남기고 무시**.
- 이유: callback 안에서 던진 예외는 KafkaTemplate 의 future 스케줄러 스레드까지 전파되어 운영 노이즈만 키운다. 정상 경로의 동작에 영향을 주어서도 안 됨.
- 이유: 메트릭은 60초 주기로 재측정되므로 1회 fallback 적재 실패는 운영상 감내 가능.

### 4.3 Drain 단계 정책 (fail-and-leave)
- 메트릭 타입 단위 try/catch 로 격리 → 한 타입 실패가 다른 타입 drain 을 막지 않게 함.
- Kafka send future 가 예외 완료 → `remove` 미호출 → ZSET 잔존 → 다음 사이클 재시도 (at-least-once).
- 역직렬화 실패(envelope 포맷 손상) → **해당 member 즉시 ZREM + WARN** (poison-pill 회피).
- Redis DataAccessException → 사이클 종료 + ERROR 로그.

### 4.4 TTL 만료 정책
- evictExpired 가 일괄 폐기. Kafka 재발송 없음. 폐기 건수 INFO 로그(`evicted={n}`).

### 4.5 Redis 데이터 유실 범위 (문서화 항목)
- AOF appendonly + 단일 인스턴스 기준, fsync 정책에 따라 마지막 1초 분량 손실 가능.
- 본 Task 의 "단기 완충" 정의가 위 손실을 명시적으로 허용. 영구 저장은 Out of Scope.

### 4.6 중복 배출 허용 여부 / Downstream 가정
- at-least-once. 중복 가능. envelope.id(UUID) 가 멱등키 후보. 본 Task 에서 Pythia 측 멱등 처리 변경은 없음.

### 4.7 CLAUDE.md 부합
- "모든 예외는 RuntimeException 상속 CustomException 또는 도메인별 커스텀 예외로 변환" → `MetricBufferException` 신규.
- "외부 API 오류는 별도 Exception 으로 변환" → Redis DataAccessException 을 MetricBufferException 으로 래핑.
- "Scheduler -> Service" 계층 → DrainScheduler 는 DrainService 만 호출, Store(Redis) 직접 접근은 Service 경유.

---

## 5. 검증 방법

### 5.1 빌드 / 기존 회귀
- `./gradlew :argus:build` 통과.
- 기존 모든 단위 테스트 GREEN (Publisher 3종은 시그니처 유지하므로 기존 assertion 그대로, 새 assertion 만 추가).
- `application-test.yml` 의 Redis autoconfig exclude 로 Lettuce 가 connection 시도 안 함 → 외부 의존성 0 유지.

### 5.2 신규 단위 테스트 (필수)
| 시나리오 | 위치 |
|---|---|
| ZADD + overflow(ZREMRANGEBYRANK or ZREM or throw) | MetricBufferStoreTest |
| ZRANGE 결과 반환 | MetricBufferStoreTest |
| ZREM 호출 | MetricBufferStoreTest |
| ZREMRANGEBYSCORE 호출 | MetricBufferStoreTest |
| EXPIRE 호출 (키 누수 방지) | MetricBufferStoreTest |
| 직렬화 실패 → MetricBufferException | MetricBufferServiceTest |
| 정상 enqueueOnFailure → store.enqueue 호출 | MetricBufferServiceTest |
| Redis DataAccessException → MetricBufferException 변환 | MetricBufferServiceTest |
| Kafka 성공 future → remove 호출 | MetricBufferDrainServiceTest |
| Kafka 실패 future → remove 미호출 | MetricBufferDrainServiceTest |
| 역직렬화 실패 → 즉시 remove + WARN | MetricBufferDrainServiceTest |
| evictExpired 가 peek 보다 먼저 호출 | MetricBufferDrainServiceTest |
| Publisher: 정상 future → bufferService 미호출 | *MetricSnapshotPublisherTest |
| Publisher: 실패 future → bufferService.enqueueOnFailure 호출 | *MetricSnapshotPublisherTest |

### 5.3 수동 통합 검증 (선택)
- `docker compose -f docker/redis/docker-compose.yml up -d` (이미 존재) + Kafka 컨테이너 기동.
- `./gradlew :argus:bootRun` → 정상 상태에서 Redis CLI `ZCARD argus:buffer:jvm` 은 **0 유지**(정상 경로에 Redis 미관여 검증).
- Kafka broker 일시 정지 → ZCARD 증가 → broker 재기동 → drain 사이클에서 항목 소진 확인.

### 5.4 Acceptance Checklist
- [ ] build.gradle 에 spring-boot-starter-data-redis 추가
- [ ] application.yml 에 spring.data.redis.* 와 argus.buffer.* 설정 추가
- [ ] application-test.yml 에 RedisAutoConfiguration/RedisRepositoriesAutoConfiguration exclude 추가
- [ ] RedisConfig 가 StringRedisTemplate Bean 을 `@ConditionalOnBean(RedisConnectionFactory.class)` 로 등록
- [ ] MetricBufferProperties 가 ttl/maxSize/overflowPolicy/drainBatchSize/drainIntervalMs/keyPrefix 노출
- [ ] MetricBufferStore 가 ZADD/ZRANGE/ZREM/ZREMRANGEBYSCORE/ZREMRANGEBYRANK/EXPIRE 만 사용
- [ ] MetricBufferService 가 Jackson 직렬/역직렬, 예외를 MetricBufferException 으로 래핑, overflow 정책 적용
- [ ] MetricBufferDrainService 가 evictExpired → peekOldest → producer.send → ack 성공 시 remove 구현
- [ ] MetricBufferDrainScheduler 가 `argus.scheduling.enabled` 조건 자동 부합 (기존 SchedulingConfig 의 `@EnableScheduling`)
- [ ] 3개 Publisher 가 future.whenComplete 에 fallback enqueue 추가 + 기존 시그니처 유지
- [ ] MetricSnapshotScheduler 변경 없음 (회귀 영향 0)
- [ ] 신규/수정 테스트 GREEN
- [ ] ./gradlew :argus:build 성공

---

## 6. 트레이드오프

### 6.1 Fallback-Only vs Outbox
- **선택: Fallback-Only**.
- Pro: 정상 경로 latency 보존(장애 감지 SLA 핵심). Redis 가 hot-path 밖. 코드 변경 폭 작음. 중복 위험 작음.
- Con: ack 회색 영역(broker 도달 후 ack 미수신) 에서 fallback 적재가 곧 중복으로 이어질 수 있음 → at-least-once 명시로 흡수.
- Con: 가상 스레드 도입 후 순간 부하 흡수에는 outbox 가 더 강함. 다만 본 Task 의 명시 목표("Kafka 일시 실패 시 유실 방지")는 fallback 으로 충분히 달성.

### 6.2 자료구조 (ZSET vs Stream vs List)
- **선택: ZSET**. 1.2 참조. 단일 인스턴스/낮은 enqueue rate(실패 케이스 한정) 도메인에 최적.

### 6.3 Overflow 정책: 기본 DROP_OLDEST
- 장애 감지 시스템에서 최신 상태가 더 가치 있음. 옛 데이터를 흘려보내는 편이 안전.
- REJECT 는 fallback 적재 자체를 거부 → 본 Task 의 "유실 방지" 의도와 충돌. 가능은 하지만 기본값 아님.

### 6.4 Fallback 책임 위치: Scheduler vs Publisher
- **선택: Publisher**.
- Pro: Publisher 의 단일 책임("Kafka 로 보내는 것까지") 안에 fallback 흡수. Scheduler 가 단순 trigger 로 유지.
- Con: Publisher 가 BufferService 의존성을 가짐(테스트 mock 1개 증가). 수용 가능 비용.

### 6.5 Resilience4j / Redisson 미도입
- Drain 워커 주기적 재실행이 자체 backoff. Argus 단일 인스턴스 가정에서 분산락 불필요.
- 후속에 운영 이슈 발생 시 별도 Task 로 분리.

### 6.6 per-member TTL vs score 기반 만료
- per-member TTL 미지원. score 기반 만료가 의미적으로 정확하며 drain 첫 단계에 무료 통합 가능.

---

## 7. CLAUDE.md 컨벤션 체크
- 패키지 구조: `service/metric/buffer/` 가 기존 `service/metric/snapshot/`, `service/metric/mapper/` 와 동일 계층.
- Scheduler → Service → (Repository/Store) 흐름 준수.
- 외부 연동 규칙: Kafka Producer 는 기존 `messaging/` 유지. Redis 접근은 service/metric/buffer 의 Store 경유.
- Kafka 규칙: 기존 JacksonJsonSerializer 유지, 변경 없음.
- Redis/Redisson Notes: redisson-spring-boot-starter 미사용, Spring Data Redis Lettuce 만 사용.
- 테스트 규칙: 신규 Service 3종 단위 테스트 추가. 수정 Publisher 단위 테스트 보강.
- "테스트 없이 기능 추가 금지" 준수.

---

## 8. 작업 순서 (구현 단계 시 참고)
1. 의존성/설정 (build.gradle, application.yml, application-test.yml, RedisConfig)
2. Properties + Envelope + Type enum + Exception
3. MetricBufferStore + 단위 테스트
4. MetricBufferService + 단위 테스트
5. MetricBufferDrainService + 단위 테스트
6. MetricBufferDrainScheduler
7. 3개 Publisher 에 fallback callback 추가 + 기존 테스트 보강
8. 전체 `./gradlew :argus:build` 통과 확인
