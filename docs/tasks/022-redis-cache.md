# 022. ViolationStateStore의 인메모리 store를 Redis 기반 저장소로 대체한다

## Target
pythia

## 1. 목표 (Goal)
- `ViolationStateStore`가 현재 사용하는 인메모리 `ConcurrentHashMap` 저장소를 Redis 기반 저장소로 대체한다
- 애플리케이션 재시작이나 다중 인스턴스 환경에서도 임계치 위반 상태를 일관되게 유지할 수 있도록 한다
- 기존 알림 전송 판단 로직의 동작 의미를 유지하면서 저장소 구현만 Redis 기반으로 전환한다

## 2. 입력 (Input)
- 현재 `ViolationStateStore`는 `ViolationKey`를 key로 하고 `ViolationState`를 value로 가지는 인메모리 store를 사용한다
- `shouldSend(ViolationKey key, Severity severity, int window)` 에서 위반 횟수 누적과 마지막 발송 severity를 함께 관리한다
- `clear(ViolationKey key)` 호출 시 해당 상태를 제거한다
- Redis 연동 기반과 구현 계획은 이미 프로젝트에 추가되어 있다

## 3. 제약 (Constraints)
* 이번 Task는 `ViolationStateStore` 저장소 대체에만 집중한다
* Alert 판단 규칙 자체는 변경하지 않는다
* Controller/API는 구현하지 않는다
* Repository 계층 대신 Redis Client 또는 Redis 전용 접근 계층을 사용한다
* 기존 `ViolationStateStore` public API는 가능한 한 유지한다
* Redis 저장 및 갱신은 동시성 환경에서 일관성이 깨지지 않도록 처리한다

## 4. 작업 내용 (Implementation Scope)
- `ViolationStateStore`의 내부 store를 `ConcurrentHashMap`에서 Redis 기반 구현으로 전환한다
- `ViolationKey`를 Redis key로 변환하는 규칙을 정의한다
- `ViolationState`를 Redis에 저장/조회할 수 있는 직렬화 구조를 정의한다
- `shouldSend(...)` 수행 시 위반 횟수 증가, 마지막 발송 severity 갱신, 발송 여부 판단이 원자적으로 처리되도록 구현한다
- `clear(...)` 수행 시 Redis에서 해당 상태를 제거하도록 변경한다
- 상태 데이터의 TTL 필요 여부를 정의하고, 필요하다면 Redis key 만료 정책을 추가한다
- Redis 장애 또는 직렬화 오류 발생 시 프로젝트 예외 규칙에 맞는 커스텀 예외로 변환한다
- Redis 기반 동작을 검증하는 테스트를 추가하거나 기존 테스트를 Redis 연동 기준으로 갱신한다

## 5. 성공 기준 (Acceptance Criteria)
- `ViolationStateStore`가 더 이상 인메모리 `ConcurrentHashMap`에 의존하지 않는다
- 동일한 `ViolationKey`와 `Severity` 조합에 대해 기존과 동일한 알림 발송 판단 결과를 유지한다
- 다중 호출 또는 동시 호출 상황에서도 위반 횟수 누락이나 중복 발송 판정이 발생하지 않는다
- `clear(...)` 호출 시 Redis 상태가 정상적으로 제거된다
- Redis 기반 구현 추가 후 기존 알림 관련 테스트가 통과하고, 필요한 신규 테스트가 포함된다

## 6. 제외 범위 (Out of Scope)
* Alert 임계치 정책 변경
* 다른 state store 또는 cache 사용처까지 Redis로 일괄 전환
* Redis Pub/Sub, Stream 도입
* Alert 이력 영속 저장소 설계 변경
* 운영 모니터링, Redis 클러스터 구성, 장애조치 전략 수립

> 변경 이력: 초기 §6에 "Redis Pub/Sub, Stream, Distributed Lock 도입" 으로 명시되어 있었으나, 원자성 보장 방식을 Redisson 분산 락으로 채택하면서 Distributed Lock 항목을 Out of Scope에서 제외함. 상세 경위는 `docs/plans/fix-logs/022-redis-cache-distributed-lock-allowed.md` 참조.
