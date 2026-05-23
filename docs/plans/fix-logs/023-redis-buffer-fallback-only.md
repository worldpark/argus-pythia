# 023 Task §1 / §4 / §5 수정 — Outbox → Fallback-Only-On-Failure 패턴 전환

> 작성일: 2026-05-23
> 관련 Task: `docs/tasks/023-kafka-prometheus-redis-buffer.md`
> 관련 Plan: `docs/plans/023-kafka-prometheus-redis-buffer-plan.md`

## 배경
초기 Task 023 §4 / §5 는 다음과 같은 표현으로 적재 시점을 기술하고 있었다.

> §4 "메트릭 스냅샷을 **Kafka publish 전에** Redis 버퍼에 적재할 수 있는 저장 구조를 정의한다"
> §5 "Argus에서 **Kafka publish 이전에** 메트릭 스냅샷을 Redis 버퍼에 저장할 수 있다"

이 문구는 **Always-Buffer-First (Outbox) 패턴** 을 강하게 시사한다. 이에 따라 1차 plan (`docs/plans/023-kafka-prometheus-redis-buffer-plan.md`) 은 모든 스냅샷을 Redis ZSET 에 먼저 적재한 뒤 별도 drain 워커가 Kafka 로 송신하는 outbox 구조로 작성되었다.

## 검토 경위
Plan 검토 단계에서 다음 후속 논의가 이어졌다.

1. "Outbox 대신 Fallback-Only 패턴으로 바꾸면 별로인가" — 양 패턴의 장단점 비교가 이어졌다.
2. Outbox 의 정상 경로 latency 증가, Redis hot-path 노출, 중복 발송 위험이 모든 메시지에 적용되는 점이 지적되었다.
3. **현재 시스템이 장애 감지/알람 대응 시스템이며 데이터 정합성보다 데이터를 빠르게 보는 것이 우선** 이라는 도메인 컨텍스트가 사용자로부터 공유되었다.
4. 본 도메인에서 Outbox 의 평균 `drain-interval / 2` latency 증가가 알람 SLA 를 침해함을 확인했다.

## 결정
사용자 결정으로 다음 변경을 적용한다.

1. **패턴 변경**: `Always-Buffer-First (Outbox)` → `Fallback-Only-On-Failure`.
2. **Task §1 / §4 / §5 문구 수정**:
   - §1: "메트릭 수집 및 Kafka publish 경로 **사이**에 Redis 버퍼를 도입" → "Kafka publish 경로의 **fallback 경로** 로 Redis 버퍼를 도입". 정상 경로 latency 보존 목표를 명시.
   - §4: "Kafka publish **전에** 적재" → "Kafka publish 가 **실패한** 스냅샷을 적재". 재배출 사이클의 의미를 명확화.
   - §5: "Kafka publish **이전에** 저장" → "Kafka publish **실패 시** 저장". 정상 ack 시 Redis 호출이 없어야 함을 새 acceptance 항목으로 추가.
3. **Plan 재작성**: `docs/plans/023-kafka-prometheus-redis-buffer-plan.md` 전체 재작성.
   - 정상 경로: 기존 `Publisher → Producer.send` 유지(시그니처/로깅 변경 없음).
   - Fallback 경로: Publisher 내부 `future.whenComplete` callback 에서 예외 완료 시에만 `MetricBufferService.enqueueOnFailure(type, snapshot)` 호출.
   - 재배출: `MetricBufferDrainScheduler` 가 ZSET → `Producer.send` → ack 성공 시 `ZREM`, 실패 시 잔존.
   - 자료구조(ZSET per type), TTL 정책(score 기반 `ZREMRANGEBYSCORE`), Overflow(`DROP_OLDEST` 기본), at-least-once 가정은 outbox plan 에서 그대로 승계.
   - `MetricSnapshotScheduler` 변경 없음. 회귀 영향 최소화.

## 사유
- **알람 latency 가 SLA 의 핵심**. 정상 경로에 Redis 호출이 끼면 평균 `drain-interval / 2` 만큼 알람 도달 지연 → 본 시스템의 1차 목표(빠른 장애 인지)와 직접 충돌.
- **Redis 가 hot-path 밖**: Redis 자체 장애가 정상 경로에 영향을 주지 않음 → 장애 감지 시스템의 자가 안정성 향상.
- **중복 발송 위험 축소**: ack 회색 영역(broker 도달 후 ack 미수신)은 fallback-only 에서도 존재하지만, outbox 처럼 **모든 메시지가 노출되지 않고 실패 케이스만 노출**됨.
- **Task 의 1차 목표 "Kafka 일시 실패 시 유실 방지" 와 정확히 등치** → 과설계 회피.
- **변경 폭 최소화**: Publisher 시그니처(`CompletableFuture<SendResult<…>>`) 와 Scheduler 코드를 그대로 두고 callback 1개만 추가 → 기존 단위 테스트가 시그니처 회귀 없이 보강 형태로 확장 가능.
- 데이터 정합성(중복)은 downstream(Pythia) 의 dedup 책임으로 정의 가능. envelope.id(UUID) 를 멱등키 후보로 부여하여 향후 확장 여지를 남김.

## 트레이드오프 (재확인)
- **가상 스레드 도입 후 순간 부하 흡수**는 outbox 가 더 강함. 본 Task 의 명시 목표("Kafka 일시 실패 시 유실 방지") 는 fallback 으로 충분하므로 후속 Task 에서 가상 스레드/병렬화 실측 후 패턴 재검토 가능.
- **중복 발송**: at-least-once 명시. downstream 멱등 처리는 후속 Task 의 책임.
- **fallback callback 안의 ZADD latency**: Publisher 의 callback 스레드(Kafka producer thread) 에서 Redis ZADD 1회 수행. 실패 케이스 한정이라 평소 부하 0. 실패 빈도가 높아질 경우 별도 executor 분리 검토 가능(본 Task 범위 외).
- **Publisher → BufferService 의존성 추가**: 단위 테스트 mock 1개 증가. 수용 가능 비용.

## 후속 조치
1. `docs/tasks/023-kafka-prometheus-redis-buffer.md` §1 / §4 / §5 문구 수정 완료.
2. 본 fix-log 작성 완료 (`docs/plans/fix-logs/023-redis-buffer-fallback-only.md`).
3. `docs/plans/023-kafka-prometheus-redis-buffer-plan.md` 재작성 완료 — Fallback-Only 기반 설계 / 구성 요소 / 데이터 흐름 / 예외 처리 / 검증 / 트레이드오프 / 완료 조건.
4. 사용자 확인 후 implementor → reviewer → fixer 워크플로우 진입.

## 변경 외 유지 사항 (재확인)
- 메트릭 계산 규칙, PromQL, DTO 필드 구조, Kafka 메시지 포맷 변경 없음.
- 기존 `MetricSnapshotScheduler` / `*MetricSnapshotProducer` 시그니처 변경 없음.
- 기존 `*MetricSnapshotPublisher.publish()` 반환 타입(`CompletableFuture<SendResult<…>>`) 유지 → 기존 Scheduler 로그 callback 그대로 동작.
- Redis 자료구조 (ZSET per metric type), 키 prefix(`argus:buffer:`), TTL/maxSize/overflow/drainInterval 설정 키, at-least-once 가정 등 outbox plan 의 보조 설계는 그대로 승계.
- CLAUDE.md 의 Redis/Redisson Notes 준수 (redisson-spring-boot-starter 미사용, Lettuce 기반).
- 테스트 전략: Mockito 단위 테스트 중심, Testcontainers 미도입.
