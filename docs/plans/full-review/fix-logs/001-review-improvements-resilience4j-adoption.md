# Review Improvements Plan - Resilience4j 통합 도입 전환

> 작성일: 2026-05-23
> 관련 Plan: `docs/plans/review-improvements-plan.md`
> 영향 항목: 우선순위 H1(이메일 재시도), H3(Lock 재시도), M3(LLM 응답 검증)

## 배경
직전 health-check 리뷰의 우선순위 7개 권고 사항(H1/H2/H3/M4/M3/H4-M1/M5)을 통합 구현하기 위한 1차 plan 이 작성됨. 1차 plan 의 기본 방침은 다음과 같았다.

- H1(이메일 재시도): `EmailService.send` 내부에 동기 retry loop 직접 구현 (Thread.sleep + 지수 백오프)
- H3(Lock 재시도): `ViolationStateStore.shouldSend` 내부에서 `tryLock` 재시도 루프 직접 구현
- M3(LLM 검증): `MetricAnalysisService.analyze` 응답에 크기/제어문자 검증만 추가, retry 미적용
- M4(Kafka 멱등성): Redis SETNX 기반 dedup. fault tolerance 미적용 (fail-open)
- 외부 라이브러리(Resilience4j, Spring Retry 등) 도입 회피 — 의존성 / Spring Boot 4 호환성 부담 회피 목적

## 검토 경위
Plan 검토 단계에서 다음 질문이 이어졌다.

1. "이메일 발송 실패 재시도 부분에서 SMTP/API 호출 실패에 대한 재시도에 Resilience4j 적용은 불가능한가" — 기능적으로 가능하며, `resilience4j-spring-boot3` starter + `@Retry` 어노테이션 + `application.yml` 설정으로 단순 적용 가능함을 안내. 단, Resilience4j 의 공식 starter 는 Spring Boot 3 까지 명시 지원이며 Boot 4 호환성은 검증 필요. AOP self-invocation 이슈, 메트릭 자동 노출 장점, 다른 fault tolerance 패턴(Circuit Breaker, Bulkhead, Rate Limiter) 통합 확장성 등을 비교.

2. "Resilience4j 도입 범위 선택" — 사용자 결정:
   - **선택**: "Resilience4j 도입 (4개 외부 호출 통합)" — EmailService + MetricAnalysisService(LLM) + ViolationStateStore(Redis Lock) + Redisson dedup 모두 Resilience4j Retry/CircuitBreaker 패턴으로 통일. plan 재작성 필요 + Boot 4 호환성 사전 검증 필요.

## 결정
사용자 결정으로 다음 두 가지 변경을 적용한다.

1. **Plan 재작성**: `docs/plans/review-improvements-plan.md` 의 H1/H3/M3 항목과 통합 설계를 Resilience4j 기반으로 전면 교체.
2. **본 fix-log 작성**: 1차 plan → 2차 plan 전환 경위와 트레이드오프를 본 문서에 기록.

## 적용 매트릭스 (2차 plan 기준)

| 외부 호출 | Retry | CircuitBreaker | 적용 방식 | Resilience4j 인스턴스명 |
|---|---|---|---|---|
| `EmailService.send` (SMTP) | O | X (Phase 2) | 어노테이션 `@Retry` | `emailSender` |
| `MetricAnalysisService.analyze` (LLM API) | O | O | 어노테이션 `@Retry @CircuitBreaker` | `llmAnalysis` |
| `ViolationStateStore` lock acquire (Redisson `RLock.tryLock`) | O | X | **Functional API** (`RetryRegistry.retry(...).executeCallable(...)`) | `violationLock` |
| `MessageDeduplicator.markProcessed` (Redis SETNX) | X | X | 미적용 (fail-open) | - |

### 적용 방식 결정 사유
- **어노테이션 채택 (Email, LLM)**: 외부 호출이 단일 public 메서드 boundary 에 위치. AOP self-invocation 이슈 무관.
- **Functional API 채택 (Lock)**: `ViolationStateStore.shouldSend` 내부에서 lock 획득 부분만 retry 대상. self-invocation 이슈 회피 위해 별도 헬퍼 메서드(`tryAcquireLock`) 추출 + Functional Retry 적용. 별도 Bean 신설은 과도한 설계로 판단.
- **CircuitBreaker 적용 (LLM만)**: OpenAI 등 외부 LLM 의 장기 장애 시 retry 폭주 → 알람 발송 흐름 보호. SMTP/Redis 는 자체 fail-fast 동작과 retry 만으로 충분.
- **dedup 미적용**: M4 의 fail-open 정책상 retry/CB 모두 의미 없음. `markProcessed` 가 실패하면 그냥 `true` 반환하여 처리 진행.

## 변경점 요약

### 의존성
- 추가: `io.github.resilience4j:resilience4j-spring-boot3:2.2.0`
- 폴백 (Boot 4 호환 실패 시): 코어 모듈(`resilience4j-retry`, `resilience4j-circuitbreaker`, `resilience4j-micrometer`) + 명시적 `RetryRegistry`/`CircuitBreakerRegistry` Bean 구성 + 모든 호출 지점을 Functional API 로 전환. 폴백 사유는 별도 fix-log (`review-improvements-resilience4j-fallback.md`) 에 기록.

### application.yml
- 신규: `resilience4j.retry.instances.emailSender|llmAnalysis|violationLock`
- 신규: `resilience4j.circuitbreaker.instances.llmAnalysis`
- 제거: `pythia.email.retry.*` (1차 plan 에서 도입 예정이었던 항목 → Resilience4j 설정으로 일원화)
- 유지: `pythia.alert.violation-state.lockWait/lockLease` (Redisson `tryLock` 인자, Retry 횟수와 별개)

### 코드
- `EmailService.send` → `@Retry(name = "emailSender")` 부착, 내부 retry loop 제거
- `MetricAnalysisService.analyze` → `@Retry + @CircuitBreaker(name = "llmAnalysis")` 부착, `AiAnalysisException`은 `ignore-exceptions` 로 retry 제외
- `ViolationStateStore.shouldSend` → `tryAcquireLock(...)` 헬퍼 메서드 추출 + sentinel `LockAcquisitionRetryException` 신설 + Functional Retry 적용
- 신규 sentinel exception: `com.example.pythia.alert.exception.LockAcquisitionRetryException`
- 기존 `@Component`, `@Service` 어노테이션 유지

## 트레이드오프 (재확인)

| 항목 | Resilience4j 도입 (2차 plan) | 내부 retry loop (1차 plan) |
|---|---|---|
| 신규 의존성 | resilience4j-spring-boot3 (~2-3MB) | 없음 |
| Spring Boot 4 호환성 | 공식 starter는 Boot 3 까지 — 검증 필요 | 확실 |
| 코드 침입성 | 어노테이션 + Functional 일부 | 메서드 본문 변경 |
| 메트릭 자동 노출 | `/actuator/metrics` 자동 | 수동 구현 필요 |
| Circuit Breaker 등 확장성 | 통합 관리 | 별도 구현 부담 |
| 학습 곡선 | 라이브러리 학습 필요 | 단순 |
| AOP self-invocation 이슈 | 있음 (어노테이션) → Functional 회피 | 없음 |
| 단위 테스트 | `RetryRegistry` 주입/모킹 필요 | 단순 |

### 채택 사유
- 운영 가시성 (메트릭 자동 노출) 확보
- 다른 외부 호출(LLM, Redis)도 동일 패턴으로 통일 → 유지보수 단순화
- 향후 Bulkhead/RateLimiter 등 패턴 도입 시 동일 라이브러리에서 확장 가능
- AOP self-invocation 이슈는 Functional API 로 회피 가능 (Lock 적용 사례)

### 리스크 / 완화
- **Spring Boot 4 호환성**: implementor 가 의존성 추가 직후 `./gradlew :pythia:compileJava :pythia:test :pythia:bootRun` 검증. 호환 안 될 시 폴백 plan (코어 모듈 + Functional API) 적용 + 별도 fix-log 기록.
- **메서드 동작 변경**: `EmailService.send` 가 이전에는 1회 시도 후 즉시 실패였으나, Resilience4j 적용 후 최대 3회 재시도. 호출자(`AlertNotifier`) 의 catch 흐름은 변경 없음. 단, 응답 지연이 길어질 수 있음 (최대 wait-duration * (max-attempts - 1) ≈ 1.5초).
- **CircuitBreaker open 시 LLM 분석 누락**: 알람 발송 자체는 `analyzeQuietly` 의 fallback 으로 계속 진행되므로 운영 영향 없음. 다만 LLM 분석이 누락된 상태로 알람이 발송될 수 있음을 PR description 에 명시.

## 후속 조치
1. 본 fix-log 작성 완료 (`docs/plans/fix-logs/review-improvements-resilience4j-adoption.md`).
2. `docs/plans/review-improvements-plan.md` 가 Resilience4j 기반으로 재작성 완료.
3. 사용자 승인 후 implementor 진입 — 의존성 추가 → Boot 4 호환성 검증 → 우선순위 1~7 순차 구현.
4. Boot 4 호환 실패 시 폴백 plan 적용 + `review-improvements-resilience4j-fallback.md` 신규 작성.

## 변경 외 유지 사항 (재확인)
- 우선순위 7개 항목 자체는 변경 없음 (H1, H2, H3, M4, M3, H4/M1, M5).
- H2(프롬프트 인젝션), M4(Kafka dedup), H4/M1(Lombok/Properties), M5(JPA 인덱스) 는 Resilience4j 와 무관 — 1차 plan 설계 유지.
- M3 의 LLM 응답 검증 로직(maxResponseChars, 제어문자 제거, `AiErrorCode.RESPONSE_TOO_LARGE` 추가)은 그대로 유지. Resilience4j Retry/CB 는 LLM 호출 자체에만 적용.
- `MessageDeduplicator` fail-open 정책 유지.
- 신규 properties: `pythia.ai.max-response-chars=5000`, `pythia.kafka.dedup.ttl=24h` 그대로 도입.
