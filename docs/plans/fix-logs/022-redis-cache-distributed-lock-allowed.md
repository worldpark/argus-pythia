# 022 Task §6 수정 — Distributed Lock 도입 허용

> 작성일: 2026-05-23
> 관련 Task: `docs/tasks/022-redis-cache.md`
> 관련 Plan: `docs/plans/022-redis-cache-plan.md`

## 배경
초기 Task 022 §6 (Out of Scope) 는 다음과 같이 명시되어 있었다.

> * Redis Pub/Sub, Stream, Distributed Lock 도입

본 항목에 따라 1차 plan 은 원자성 보장 방식으로 **Lua script (`RedisScript<Long>`)** 를 채택했다 (`docs/plans/022-redis-cache-plan.md`, §2.1). Lua script 후보 비교 표에서 (C) Redisson 분산 락은 "Out of Scope (Task §6 절)" 사유로 제외했다.

## 검토 경위
Plan 검토 단계에서 다음 후속 질문이 이어졌다.

1. "Lua 스크립트가 필요한 이유가 무엇인가" — 다중 명령 사이의 race 발생 가능성과 원자성 보장이 필요한 이유 설명 (단일 round-trip + 서버 측 단일 스레드 보장).
2. "스크립트 말고 애플리케이션 코드로 구현이 불가능한 것인가" — Java-only 대안으로 `WATCH/MULTI/EXEC` (낙관적 잠금) 가능함을 설명. Lua 대비 코드량/재시도 정책/테스트 용이성 트레이드오프 정리.
3. "Redisson 분산 락으로는 불가능한가" — 기능적으로는 가능하지만 Task §6 에 명시적으로 "Distributed Lock 도입" 이 Out of Scope 로 못박혀 있어 Task 변경 합의가 선행되어야 함을 안내.

## 결정
사용자 결정으로 다음 두 가지 변경을 적용한다.

1. **Task 022 §6 수정**: "Redis Pub/Sub, Stream, Distributed Lock 도입" 항목에서 **Distributed Lock 도입**을 제거한다. 수정 후 항목은 "Redis Pub/Sub, Stream 도입" 으로 축소된다.
2. **Plan 재작성**: `docs/plans/022-redis-cache-plan.md` 의 원자성 보장 방식을 **Lua script → Redisson 분산 락** 으로 교체한다. 자료구조(Hash), Key 변환 규칙, TTL slide, 예외 처리, 검증 방법 등 Lua 외 설계는 유지하되, 원자성 메커니즘과 그에 종속된 구현 상세(Lua 파일, RedisScript Bean, 단위 테스트 옵션) 는 분산 락 기준으로 갱신한다.

## 사유
- Java 코드만으로 구현 가능 (Lua 별도 파일 없음, 디버깅/로그 추가 용이) → 운영/유지보수 관점의 단순성을 우선시.
- Redisson 의 `RLock.tryLock(wait, lease, TimeUnit)` 패턴은 try-finally 만으로 정상/실패 모두 안전한 해제 보장 → 코드가 직관적.
- 다중 인스턴스 환경에서도 키 단위 lock 으로 분산 일관성 확보 (Task §1 목표 부합).
- 알림 평가 빈도가 낮아 키 단위 시리얼라이즈로 인한 throughput 저하가 실질적으로 무시 가능.
- Lua 자체 검증의 통합 테스트 부재 리스크(별도 task 분리 필요) 회피.

## 트레이드오프 (재확인)
- 신규 의존성: `org.redisson:redisson-spring-boot-starter` 추가 (약 6MB, 자체 Redis 클라이언트 보유).
- Round-trip 증가: Lua 1회 vs Redisson 3회 이상 (lock 획득 / 작업 / unlock). 단, 알림 평가 호출 빈도가 낮아 실제 영향 미미.
- 락 leaseMs 정책 결정 필요: 락 만료 시간을 너무 짧게 잡으면 작업 중 락 자동 만료, 너무 길게 잡으면 장애 시 해제 지연. 본 시스템에서는 작업 자체가 수 ms 안에 끝나는 단순 HMGET → HSET 흐름이므로 lease = 수 초 수준이면 충분.
- Lettuce(StringRedisTemplate) 와 Redisson 이 별도 connection 풀을 가지게 됨 → 리소스 중복. 본 task 범위 외에서 단일화 검토 가능.

## 후속 조치
1. `docs/tasks/022-redis-cache.md` §6 수정 적용 완료.
2. 본 fix-log 작성 완료 (`docs/plans/fix-logs/022-redis-cache-distributed-lock-allowed.md`).
3. `docs/plans/022-redis-cache-plan.md` 재작성 — Redisson 기반 설계 / 구성 요소 / 데이터 흐름 / 예외 처리 / 검증 / 트레이드오프 / 완료 조건 갱신.
4. 사용자 재확인 후 implementor → reviewer → fixer 워크플로우 진입.

## 변경 외 유지 사항 (재확인)
- ViolationStateStore public API (`shouldSend(ViolationKey, Severity, int)`, `clear(ViolationKey)`) 시그니처 유지.
- Redis Hash 자료구조 (`warningCount`, `criticalCount`, `lastSent` 3 필드) 유지.
- Key prefix `pythia:alert:violation:` 유지.
- TTL slide 1h 유지.
- 예외 변환 정책 (`ViolationStateException` + `ViolationStateErrorCode`) 유지.
- Mockito 기반 단위 테스트 전략 유지. Lua 검증 한계는 사라지고, Redisson 의 `RLock` 등은 Mockito 모킹으로 단위 테스트 가능 → Lua 대비 단위 테스트 커버리지 개선.
- `PythiaApplicationTests.contextLoads` 영향: `ViolationStateStore` 가 `RedissonClient` 등 신규 Bean 을 요구하므로 테스트 컨텍스트에서 해당 Bean 제공이 필요. 구체 방안은 재작성된 plan 의 검증 절에서 명시할 예정.
