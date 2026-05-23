# Review Improvements Plan - Resilience4j 통합 도입 기반 재작성

## 1. 개요

### 1.1 재작성 배경
직전 health-check 리뷰 우선순위 7개를 단일 묶음으로 처리하는 plan 이었으나, 사용자 결정으로 **외부 호출에 대한 fault tolerance 패턴을 Resilience4j 로 통합 도입**하기로 함. 기존 plan 의 "내부 retry loop + Spring Retry/Resilience4j 도입 회피" 방침을 폐기하고, 다음 3개 외부 호출에 Resilience4j Retry, 그 중 1개에 추가로 CircuitBreaker 를 적용한다.

| 외부 호출 | Retry | CircuitBreaker | 비고 |
|---|---|---|---|
| `EmailService.send` (SMTP) | O | X (Phase 2) | 일시적 SMTP 장애 |
| `MetricAnalysisService.analyze` (LLM API) | O | O | timeout/rate limit, 장기 장애 격리 |
| `ViolationStateStore` lock acquire (Redis/Redisson) | O | X | lock 경합 일시 충돌, fail-open 정책으로 CB 불필요 |
| `MessageDeduplicator.markProcessed` (Redis SETNX) | X | X | fail-open. fault tolerance 미적용 |

### 1.2 전체 목표
- 운영 안정성(이메일 재시도, Lock 재시도, Kafka 멱등성) 강화 - **재시도 책임을 Resilience4j 로 일원화**
- LLM 연동 보안/안정성(프롬프트 인젝션 방어, 응답 검증, 장애 격리) 보강
- 컨벤션 통일(Lombok 일관 적용 + Redis properties 스타일 정합)
- 조회 성능(Metric Entity 인덱스) 개선

### 1.3 우선순위 (구현 순서 고정)
| 순번 | 식별자 | 제목 | 카테고리 | Resilience4j 관여 |
|---|---|---|---|---|
| 1 | H1 | 이메일 발송 실패 재시도 | 안정성 | Retry |
| 2 | H2 | 프롬프트 인젝션 방어 | 보안 | - |
| 3 | H3 | Lock 획득 실패 시 지수 백오프 재시도 | 안정성 | Retry (Functional) |
| 4 | M4 | Kafka 멱등성 | 안정성 | - (fail-open) |
| 5 | M3 | LLM 응답 검증 | 안정성 | Retry + CircuitBreaker |
| 6 | H4/M1 | 컨벤션 통일 (Lombok / PythiaRedisProperties) | 품질 | - |
| 7 | M5 | JPA 쿼리 인덱스 | 성능 | - |

### 1.4 범위 외(Out of Scope)
- 전역 예외 핸들러(GlobalExceptionHandler) 신설/리팩토링
- EmailService CircuitBreaker(Phase 2)
- 이메일 영속 실패 큐(재시도 모두 실패한 알림을 별도 저장)
- Redisson lock 자체의 redis-side retry 튜닝
- 다른 도메인(예: argus) 리팩토링
- Spring Retry, Failsafe 등 Resilience4j 외 라이브러리

---

## 2. 의존성 + Spring Boot 4 호환성 검증 절차

### 2.1 권장 의존성
- **주요(권장)**: `io.github.resilience4j:resilience4j-spring-boot3:2.2.0`
  - Spring Boot 3 starter 이지만 Spring Boot 4 와 ABI 호환 여부는 미확정. 도입 직후 검증 필수.
  - 검증 명령:
    ```
    ./gradlew :pythia:compileJava
    ./gradlew :pythia:test
    ./gradlew :pythia:bootRun --args='--spring.profiles.active=local'
    ```
  - 검증 통과 조건: 컴파일 성공 + 컨텍스트 로딩 성공 + 기존 테스트 회귀 0개.

### 2.2 폴백 시나리오 (호환 안 될 때)
- **폴백**: 코어 모듈만 추가
  - `io.github.resilience4j:resilience4j-retry:2.2.0`
  - `io.github.resilience4j:resilience4j-circuitbreaker:2.2.0`
  - `io.github.resilience4j:resilience4j-micrometer:2.2.0` (optional, metrics)
- **수동 구성 빈**: `RetryRegistry`, `CircuitBreakerRegistry` 를 `@Configuration` 클래스로 직접 생성.
- **어노테이션 미지원**: AOP starter 없이는 `@Retry`, `@CircuitBreaker` 동작 안 함. 그 경우 모든 호출 지점은 Functional API 로 전환:
  - `Retry retry = registry.retry("emailSender");`
  - `retry.executeRunnable(() -> mailSender.send(...));`
- **폴백 적용 시 산출물**: `docs/plans/fix-logs/review-improvements-resilience4j-fallback.md` 에 사유, 채택 모듈, 변경 지점 기록.

### 2.3 build.gradle 변경 (pythia 모듈)
```gradle
dependencies {
    // ... 기존
    implementation "io.github.resilience4j:resilience4j-spring-boot3:2.2.0"
    // 폴백 시:
    // implementation "io.github.resilience4j:resilience4j-retry:2.2.0"
    // implementation "io.github.resilience4j:resilience4j-circuitbreaker:2.2.0"
    // implementation "io.github.resilience4j:resilience4j-micrometer:2.2.0"
}
```
- Spring AOP 의존성은 `resilience4j-spring-boot3` 가 transitive 로 포함. 별도 추가 불필요.

---

## 3. 4개 외부 호출 적용 매트릭스

| 호출 지점 | 호출 메서드 | 적용 패턴 | 적용 방식 | Retry 인스턴스명 | CB 인스턴스명 |
|---|---|---|---|---|---|
| EmailService | `send(EmailRequest)` | Retry | 어노테이션 | `emailSender` | - |
| MetricAnalysisService | `analyze(MetricAnalysisRequest)` | Retry + CircuitBreaker | 어노테이션 | `llmAnalysis` | `llmAnalysis` |
| ViolationStateStore | `tryAcquireLock(...)` (내부 헬퍼) | Retry | **Functional API** | `violationLock` | - |
| MessageDeduplicator | `markProcessed(...)` | (미적용, fail-open) | - | - | - |

### 3.1 적용 방식 결정 사유
- **어노테이션 채택 (Email, LLM)**: 외부 호출이 단일 public 메서드 boundary 에 위치. AOP self-invocation 이슈 무관.
- **Functional API 채택 (Lock)**: `ViolationStateStore.shouldSend` 내부에서 lock 획득 부분만 retry 대상. self-invocation 이슈 회피 위해 별도 메서드 분리 + 어노테이션 대신 `RetryRegistry.retry("violationLock").executeCallable(...)` 사용. 별도 Bean 신설은 과도한 설계로 판단해 제외.
- **미적용 (Dedup)**: fail-open 정책상 retry/CB 모두 불필요. 실패 시 `markProcessed` 가 `true` 반환하여 처리 진행.

### 3.2 self-invocation 회피 원칙
- 모든 `@Retry`, `@CircuitBreaker` 는 외부에서 호출되는 public 메서드에만 부착.
- 동일 클래스 내부에서 호출하는 메서드에는 어노테이션 부착 금지(AOP proxy bypass).
- 위반 시 Functional API 로 전환.

---

## 4. application.yml - Resilience4j 설정 명세

```yaml
resilience4j:
  retry:
    instances:
      emailSender:
        max-attempts: 3
        wait-duration: 500ms
        enable-exponential-backoff: true
        exponential-backoff-multiplier: 2.0
        retry-exceptions:
          - org.springframework.mail.MailException
        ignore-exceptions:
          - java.lang.InterruptedException
      llmAnalysis:
        max-attempts: 3
        wait-duration: 1s
        enable-exponential-backoff: true
        exponential-backoff-multiplier: 2.0
        retry-exceptions:
          - org.springframework.ai.retry.TransientAiException
          - java.io.IOException
        ignore-exceptions:
          - com.example.pythia.ai.exception.AiAnalysisException
      violationLock:
        max-attempts: 3
        wait-duration: 50ms
        enable-exponential-backoff: true
        exponential-backoff-multiplier: 2.0
        retry-exceptions:
          - com.example.pythia.alert.exception.LockAcquisitionRetryException
        ignore-exceptions:
          - java.lang.InterruptedException
  circuitbreaker:
    instances:
      llmAnalysis:
        failure-rate-threshold: 50
        wait-duration-in-open-state: 30s
        sliding-window-size: 10
        sliding-window-type: COUNT_BASED
        minimum-number-of-calls: 5
        permitted-number-of-calls-in-half-open-state: 3
        automatic-transition-from-open-to-half-open-enabled: true
        ignore-exceptions:
          - com.example.pythia.ai.exception.AiAnalysisException
```

### 4.1 기존 properties 통폐합 정책
- (A) **권장**: Resilience4j 와 의미 중복되는 기존 properties 제거, Resilience4j 설정으로 일원화.
- (B) 기존 유지 + 병렬 사용 - 의미 중복으로 혼란. 제외.

#### 채택: (A)
- 제거 대상:
  - `pythia.email.retry.*` (전부 제거)
- **유지 대상** (Resilience4j 와 의미가 다름):
  - `pythia.alert.violation-state.lockWait` - Redisson `tryLock(waitTime, leaseTime, unit)` 의 `waitTime` 인자. Retry attempt 와 별개.
  - `pythia.alert.violation-state.lockLease` - Redisson lease time. 동일.
- **이관 대상** (기존 -> Resilience4j):
  - `pythia.alert.violation-state.lockMaxAttempts` (해당 필드가 본 plan 이전에 추가된 경우) -> `resilience4j.retry.instances.violationLock.max-attempts`
  - 백오프 관련 필드 -> `resilience4j.retry.instances.violationLock.wait-duration` + `exponential-backoff-multiplier`

### 4.2 신규 properties (Resilience4j 외)
- `pythia.ai.max-response-chars=5000` - M3 LLM 응답 크기 상한 (신규 `AiAnalysisProperties`)
- `pythia.kafka.dedup.ttl=24h` - M4 dedup 키 TTL (신규 `KafkaDedupProperties`)

---

## 5. 항목별 상세 (우선순위 순)

### 5.1 H1 - 이메일 발송 실패 재시도 (Resilience4j Retry)

#### 설계
- `EmailService.send(EmailRequest)` 에 `@Retry(name = "emailSender")` 부착.
- Resilience4j 가 `MailException` 발견 시 `wait-duration` x `multiplier^(attempt-1)` 만큼 대기 후 재시도.
- `max-attempts` 도달 시 마지막 `MailException` propagate -> caller (`AlertNotifier`) 에서 `EmailSendException(SMTP_FAILURE)` 변환 또는 기존 catch/log 흐름 유지.
- 기존 내부 retry loop / `Thread.sleep` 제거 (해당 코드가 없는 경우 단순 단발 호출 유지).

#### EmailSendException 의 retry-trigger 충돌 회피
- 본 메서드가 `MailException` 을 즉시 catch 해서 `EmailSendException` 으로 wrapping 하면 Resilience4j 가 retry 트리거를 인식하지 못함 (wrapping 된 예외는 `retry-exceptions` 목록에 미포함).
- **해결**: `send` 메서드는 `MailException` 을 catch 하지 않고 그대로 propagate. caller(`AlertNotifier`) 가 변환 책임을 가짐.

#### 영향 파일
- 신규: 없음
- 수정:
  - `pythia/src/main/java/com/example/pythia/email/EmailService.java` - `@Retry` + `@RequiredArgsConstructor` (6번 항목과 통합), `MailException` catch 제거
  - `pythia/src/main/java/com/example/pythia/email/config/EmailProperties.java` - `Retry` 중첩 클래스가 있다면 제거 (없으면 변경 없음)
  - `pythia/src/main/java/com/example/pythia/alert/service/AlertNotifier.java` - EmailService 호출부 catch 분기에서 `MailException` 또는 그대로 throw 된 예외 처리
  - `pythia/src/main/resources/application.yml` - `resilience4j.retry.instances.emailSender` 추가, `pythia.email.retry.*` 제거
  - `pythia/build.gradle` - 의존성 추가
- 신규 테스트:
  - `pythia/src/test/java/com/example/pythia/email/EmailServiceRetryTest.java`

#### EmailService 의사 코드 (최종)
```java
@Service
@RequiredArgsConstructor
@Slf4j
public class EmailService {

    private final JavaMailSender mailSender;
    private final EmailProperties properties;

    @Retry(name = "emailSender")
    public void send(EmailRequest request) {
        MimeMessage message = buildMimeMessage(request);
        mailSender.send(message);  // MailException propagate -> Retry trigger
    }
}
```

#### AlertNotifier 변환 의사 코드
```java
try {
    emailService.send(emailRequest);
} catch (MailException ex) {
    log.error("Email send failed after retries: {}", ex.getMessage(), ex);
    // 기존 정책 유지: 알림 실패는 throw 하지 않고 log 만
}
```

#### 테스트
- mailSender 가 N-1회 `MailException` -> N회째 성공 시 호출 횟수 = N.
  - `@SpringBootTest(classes = {ResilienceAutoConfiguration, EmailService})` 또는 `RetryRegistry` 를 직접 주입해 `Retry.decorateRunnable(...)` 로 단위 테스트.
- mailSender 가 항상 `MailException` 시 N회 호출 후 `MailException` propagate.
- 정상 응답 시 호출 횟수 = 1.

#### 트레이드오프 - Resilience4j 어노테이션 vs 내부 retry loop
| 항목 | Resilience4j 어노테이션 | 내부 retry loop |
|---|---|---|
| 코드 양 | 적음 (어노테이션 1줄) | 많음 (try/catch + sleep + 백오프 계산) |
| 설정 외재화 | yml 완전 외재화 | properties + 코드 양쪽 |
| 메트릭 | 자동 노출 (Micrometer) | 수동 구현 |
| 테스트 | `RetryRegistry` 주입 또는 `@SpringBootTest` 필요 | 단순 mockito |
| AOP 의존 | O | X |
| Boot 4 호환 | 검증 필요 | 무관 |

#### 그 외 트레이드오프
- `MailException` 을 caller 가 처리하도록 변경 -> `AlertNotifier` 코드 변경 필요.
- 동기 retry 로 호출 스레드 점유 시간 증가. 알림 스레드 풀 모니터링은 별도 task.

---

### 5.2 H2 - 프롬프트 인젝션 방어 (Resilience4j 무관)

#### 설계
- `MetricAnalysisPromptFactory` 에 `private static String sanitizeForPrompt(String input)` 추가.
- 외부 입력 변수(application, instance, metricName, unit)에만 적용. 시스템 생성 값(BigDecimal, OffsetDateTime, range string)은 미적용.

#### 영향 파일
- 신규: 없음
- 수정:
  - `pythia/src/main/java/com/example/pythia/ai/prompt/MetricAnalysisPromptFactory.java`
- 신규 테스트:
  - `pythia/src/test/java/com/example/pythia/ai/prompt/MetricAnalysisPromptFactoryInjectionTest.java`

#### sanitizeForPrompt 의사 코드
```java
private static final Pattern CONTROL_CHARS = Pattern.compile("[\p{Cntrl}&&[^\t]]");
private static final Pattern MARKDOWN_FENCE = Pattern.compile("(?m)^\s{0,3}(```|~~~).*$");
private static final Pattern MARKDOWN_HEADING_OR_RULE =
        Pattern.compile("(?m)^\s{0,3}(#{1,6}\s|-{3,}|\*{3,}|_{3,})");
private static final Pattern INJECTION_KEYWORDS =
        Pattern.compile("(?i)(ignore\s+previous|disregard\s+previous|system\s*:|assistant\s*:|user\s*:)");
private static final int MAX_LEN = 256;

private static String sanitizeForPrompt(String input) {
    if (input == null) return "";
    String value = input;
    value = CONTROL_CHARS.matcher(value).replaceAll(" ");
    value = MARKDOWN_FENCE.matcher(value).replaceAll(" ");
    value = MARKDOWN_HEADING_OR_RULE.matcher(value).replaceAll(" ");
    value = INJECTION_KEYWORDS.matcher(value).replaceAll("[redacted]");
    value = value.replace("`", " ");
    value = value.replaceAll("\s+", " ").trim();
    if (value.length() > MAX_LEN) {
        value = value.substring(0, MAX_LEN);
    }
    return value;
}
```

#### 적용 지점
- `target.application()`, `target.instance()`
- 각 `MetricSummary.metricName()`, `MetricSummary.unit()`
- 각 `TimeSeriesPoint.metricName()`
- 숫자/타임스탬프/range 미적용

#### 테스트
- `metricName` 에 줄바꿈 + `### Ignore previous instructions.` 포함 -> 결과에 줄바꿈/헤딩/키워드 모두 무력화.
- 코드 펜스 ``` 포함 입력 -> 결과에 펜스 부재.
- 정상 한글/영문 라벨 -> 그대로 통과.
- 길이 256자 초과 -> 256자 cutoff.

#### 트레이드오프
- 정상 라벨에도 백틱/마크다운 포함 시 변형. 메트릭 라벨 컨벤션상 영향 미미.
- 키워드 블랙리스트는 우회 가능 - 1차 방어선 한정.

---

### 5.3 H3 - Lock 획득 실패 시 지수 백오프 재시도 (Resilience4j Retry - Functional)

#### 설계
- `ViolationStateStore.shouldSend(...)` 내부에서 lock 획득 부분만 별도 private 헬퍼 `tryAcquireLock(lockKey)` 로 추출.
- 헬퍼는 Redisson `tryLock(waitMs, leaseMs, MS)` 호출 후 `false` 반환 시 sentinel exception `LockAcquisitionRetryException` throw.
- `RetryRegistry` 를 생성자 주입받아 `retry("violationLock").executeCallable(() -> tryAcquireLock(lockKey))` 형태로 호출.
- self-invocation 이슈 회피 위해 어노테이션 대신 Functional API 사용.
- Retry 모두 실패(sentinel exception N회) 시 catch 후 `ViolationStateException(LOCK_ACQUISITION_FAILED)` 변환.
- `InterruptedException` 은 `ignore-exceptions` 로 retry 제외 + 즉시 `Thread.currentThread().interrupt()` + `ViolationStateException(LOCK_INTERRUPTED)` 변환.

#### 영향 파일
- 신규:
  - `pythia/src/main/java/com/example/pythia/alert/exception/LockAcquisitionRetryException.java` (sentinel, `RuntimeException`)
- 수정:
  - `pythia/src/main/java/com/example/pythia/alert/state/ViolationStateStore.java` - Functional Retry 적용
  - `pythia/src/main/java/com/example/pythia/alert/exception/ViolationStateErrorCode.java` - `LOCK_INTERRUPTED` 추가 (이미 있으면 변경 없음)
  - `pythia/src/main/java/com/example/pythia/alert/config/ViolationStateProperties.java` - retry 관련 필드 제거 (Resilience4j 로 이관). `lockWait`, `lockLease` 만 유지
  - `pythia/src/main/resources/application.yml` - `resilience4j.retry.instances.violationLock` 추가, 기존 lock retry 필드 제거
- 신규/수정 테스트:
  - `pythia/src/test/java/com/example/pythia/alert/state/ViolationStateStoreLockRetryTest.java`

#### LockAcquisitionRetryException
```java
package com.example.pythia.alert.exception;

// sentinel - Resilience4j Retry 트리거 전용. 외부로 노출되지 않음.
public class LockAcquisitionRetryException extends RuntimeException {
    public LockAcquisitionRetryException() {
        super("Lock acquisition returned false");
    }
}
```

#### ViolationStateStore.shouldSend 의사 코드
```java
@Component
@RequiredArgsConstructor
@Slf4j
public class ViolationStateStore {

    private final RedissonClient redissonClient;
    private final ViolationStateProperties properties;
    private final RetryRegistry retryRegistry;
    // 기존 의존성 ...

    public boolean shouldSend(ViolationKey key, ...) {
        String lockKey = lockKeyFor(key);
        RLock lock = null;
        try {
            lock = retryRegistry.retry("violationLock")
                    .executeCallable(() -> tryAcquireLock(lockKey));
        } catch (LockAcquisitionRetryException ex) {
            throw new ViolationStateException(ViolationStateErrorCode.LOCK_ACQUISITION_FAILED, ex);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new ViolationStateException(ViolationStateErrorCode.LOCK_INTERRUPTED, ie);
        } catch (Exception ex) {
            throw new ViolationStateException(ViolationStateErrorCode.LOCK_ACQUISITION_FAILED, ex);
        }
        try {
            // 기존 TTL 조회/판정/저장 로직
        } finally {
            if (lock != null && lock.isHeldByCurrentThread()) lock.unlock();
        }
    }

    private RLock tryAcquireLock(String lockKey) throws InterruptedException {
        RLock lock = redissonClient.getLock(lockKey);
        boolean acquired = lock.tryLock(
                properties.getLockWait().toMillis(),
                properties.getLockLease().toMillis(),
                TimeUnit.MILLISECONDS);
        if (!acquired) {
            throw new LockAcquisitionRetryException();
        }
        return lock;
    }
}
```
- `executeCallable` 은 checked exception 도 propagate. `InterruptedException` 은 Resilience4j 의 `ignore-exceptions` 설정으로 retry 제외.
- `Callable.call()` 시그니처가 `throws Exception` 이므로 호출자에서 일반 `Exception` catch 필요.

#### application.yml 변경
```yaml
pythia:
  alert:
    violation-state:
      ttl: 1h
      key-prefix: pythia:alert:violation
      lock-key-prefix: pythia:alert:violation:lock
      lock-wait: 200ms
      lock-lease: 3s
      # 제거: lock-max-attempts, lock-retry-* (Resilience4j 로 이관)
```

#### 테스트
- Redisson mock 의 `tryLock` 이 2회 false -> 3회째 true -> 정상 진행, 호출 횟수 = 3.
- `tryLock` 이 항상 false -> `LOCK_ACQUISITION_FAILED` + 호출 횟수 = max-attempts.
- `tryLock` 첫 호출에서 `InterruptedException` -> `LOCK_INTERRUPTED` + `Thread.interrupted() == true` + retry 미수행(호출 횟수 = 1).
- `RetryRegistry` 는 테스트에서 `RetryRegistry.of(RetryConfig.custom()...)` 으로 직접 생성 가능.

#### 트레이드오프
- Functional Retry 채택으로 별도 Bean 분리 회피. 코드 침입성 약간 증가하지만 self-invocation 우려 없음.
- worst-case 지연 시간 = `max-attempts * lockWait + sum(backoff)` ~= `3 * 200 + (50 + 100) = 750ms`. SLA 영향 무시 가능.
- Redisson watchdog/lease 와 충돌 없음.

---

### 5.4 M4 - Kafka 멱등성 (Resilience4j 미적용, fail-open)

#### 설계
- 신규 컴포넌트 `MessageDeduplicator` (`com.example.pythia.kafka.consumer.MessageDeduplicator`).
- 각 Handler `handle(snapshot)` 진입 직후 `markProcessed(topic, app, inst, collectedAt)` 호출 -> false 반환 시 즉시 return.
- dedup key: `pythia:kafka:processed:{topic}:{application}:{instance}:{collectedAtEpochMillis}`.
- TTL: `pythia.kafka.dedup.ttl=24h`.
- Redis 장애 시 fail-open: catch `DataAccessException` -> true 반환 + warn 로그.
- **Resilience4j fault tolerance 미적용**: dedup 자체가 best-effort. retry 하지 않고 즉시 fail-open 이 정책.

#### 영향 파일
- 신규:
  - `pythia/src/main/java/com/example/pythia/kafka/consumer/MessageDeduplicator.java`
  - `pythia/src/main/java/com/example/pythia/kafka/consumer/KafkaDedupProperties.java`
- 수정:
  - `pythia/src/main/java/com/example/pythia/kafka/consumer/JvmMetricSnapshotHandler.java`
  - `pythia/src/main/java/com/example/pythia/kafka/consumer/HttpMetricSnapshotHandler.java`
  - `pythia/src/main/java/com/example/pythia/kafka/consumer/HikariMetricSnapshotHandler.java`
  - `pythia/src/main/java/com/example/pythia/kafka/config/KafkaConsumerConfig.java` - `@EnableConfigurationProperties(KafkaDedupProperties.class)` 추가
  - `pythia/src/main/resources/application.yml`
- 신규 테스트:
  - `pythia/src/test/java/com/example/pythia/kafka/consumer/MessageDeduplicatorTest.java`
  - 각 Handler 테스트에 dedup hit 케이스 추가

#### MessageDeduplicator 의사 코드
```java
@Component
@RequiredArgsConstructor
@Slf4j
public class MessageDeduplicator {

    private final StringRedisTemplate redisTemplate;
    private final KafkaDedupProperties properties;

    public boolean markProcessed(String topic, String application, String instance, OffsetDateTime collectedAt) {
        String key = "pythia:kafka:processed:%s:%s:%s:%d"
                .formatted(topic, application, instance, collectedAt.toInstant().toEpochMilli());
        try {
            Boolean firstTime = redisTemplate.opsForValue()
                    .setIfAbsent(key, "1", properties.getTtl());
            return Boolean.TRUE.equals(firstTime);
        } catch (DataAccessException ex) {
            log.warn("Dedup Redis failure, fail-open: key={}, err={}", key, ex.getMessage());
            return true;
        }
    }
}
```

#### KafkaDedupProperties 의사 코드
```java
@ConfigurationProperties(prefix = "pythia.kafka.dedup")
@Getter
@Setter
public class KafkaDedupProperties {
    private Duration ttl = Duration.ofHours(24);
}
```

#### Handler 통합 의사 코드 (JVM 예시)
```java
public void handle(JvmMetricSnapshotDto snapshot) {
    if (!deduplicator.markProcessed(
            JvmMetricTopic.NAME, snapshot.application(), snapshot.instance(), snapshot.collectedAt())) {
        log.debug("Skip duplicate JVM snapshot: app={}, inst={}, ts={}",
                snapshot.application(), snapshot.instance(), snapshot.collectedAt());
        return;
    }
    // 기존 흐름
}
```

#### application.yml 추가
```yaml
pythia:
  kafka:
    dedup:
      ttl: 24h
```

#### 테스트
- `setIfAbsent` true -> 처리 진행, save/evaluate 호출 1회.
- `setIfAbsent` false -> save/evaluate 미호출.
- 동일 메시지 2회 호출 시 두 번째 스킵.
- Redis `DataAccessException` -> true 반환 + warn 로그 (fail-open).

#### 트레이드오프
- Redis 장애 시 fail-open -> 중복 알림 가능. 무손실 우선 정책.
- collectedAt 정밀도 ms - 동일 ms 메시지가 있을 시 dedup 충돌 가능하지만 스케줄 발행 패턴상 사실상 미발생.
- Resilience4j 적용 시 retry 폭주 위험 + 처리 지연 -> 미적용 결정.

---

### 5.5 M3 - LLM 응답 검증 (Resilience4j Retry + CircuitBreaker)

#### 설계
- `MetricAnalysisService.analyze(MetricAnalysisRequest)` 에 `@Retry(name = "llmAnalysis")` + `@CircuitBreaker(name = "llmAnalysis")` 부착.
- Retry/CB 와 별개로 응답 검증 로직(maxResponseChars + 제어문자 제거) 추가.
- 검증 실패 (`RESPONSE_TOO_LARGE`) 는 `AiAnalysisException` throw -> `ignore-exceptions` 로 retry/CB 미적용.

#### Resilience4j 어노테이션 순서
- Resilience4j 기본 priority: Retry 가 outer, CircuitBreaker 가 inner. 한 번의 외부 호출당 1번의 CB call. Retry 가 outer 로 CB 결과 보고 재시도.
- 본 plan 에서는 기본 priority 채택.

#### 영향 파일
- 신규:
  - `pythia/src/main/java/com/example/pythia/ai/config/AiAnalysisProperties.java`
- 수정:
  - `pythia/src/main/java/com/example/pythia/ai/exception/AiErrorCode.java` - `RESPONSE_TOO_LARGE` 추가
  - `pythia/src/main/java/com/example/pythia/ai/service/MetricAnalysisService.java`
  - `pythia/src/main/java/com/example/pythia/ai/config/ChatClientConfig.java` - `@EnableConfigurationProperties(AiAnalysisProperties.class)` 추가
  - `pythia/src/main/resources/application.yml`
- 신규 테스트:
  - `pythia/src/test/java/com/example/pythia/ai/service/MetricAnalysisServiceResponseValidationTest.java`

#### AiErrorCode 추가
```java
RESPONSE_TOO_LARGE("AI_004", "LLM response exceeds size limit");
```

#### AiAnalysisProperties
```java
@ConfigurationProperties(prefix = "pythia.ai")
@Getter
@Setter
public class AiAnalysisProperties {
    private int maxResponseChars = 5000;
}
```

#### MetricAnalysisService.analyze 의사 코드
```java
@Service
@RequiredArgsConstructor
@Slf4j
public class MetricAnalysisService {

    private final ChatClient chatClient;
    private final MetricAnalysisPromptFactory promptFactory;
    private final AiAnalysisProperties properties;

    private static final Pattern CONTROL_EXCEPT_NL_TAB =
            Pattern.compile("[\p{Cntrl}&&[^\n\t]]");

    @Retry(name = "llmAnalysis")
    @CircuitBreaker(name = "llmAnalysis")
    public String analyze(MetricAnalysisRequest request) {
        String prompt = promptFactory.build(request).getContents();
        String content;
        try {
            content = chatClient.prompt(prompt).call().content();
        } catch (TransientAiException | IOException ex) {
            throw ex;  // retry-exceptions 에 해당 -> Resilience4j 가 재시도
        } catch (Exception ex) {
            throw new AiAnalysisException(AiErrorCode.LLM_CALL_FAILURE, ex);
        }
        if (content == null || content.isBlank()) {
            throw new AiAnalysisException(AiErrorCode.EMPTY_RESPONSE);
        }
        String sanitized = CONTROL_EXCEPT_NL_TAB.matcher(content).replaceAll("");
        if (sanitized.length() > properties.getMaxResponseChars()) {
            throw new AiAnalysisException(AiErrorCode.RESPONSE_TOO_LARGE);
        }
        return sanitized;
    }
}
```
- `AiAnalysisException` 은 `ignore-exceptions` 로 retry/CB 미적용 -> blank/too-large 검증 실패는 즉시 caller 에게 전달.
- `TransientAiException`, `IOException` 은 `retry-exceptions` 로 재시도 대상.
- 그 외 일반 Exception 은 `AiAnalysisException(LLM_CALL_FAILURE)` 로 변환 후 ignore -> retry 없음.

#### 의사 코드 보완 - retry 트리거 정확성
- Spring AI 의 실제 예외 계층 확인 후 `retry-exceptions` 목록 조정 필요. 구현 단계에서 `org.springframework.ai.retry.TransientAiException` 등 정확한 클래스명 확인 후 yml 업데이트.

#### application.yml 추가
```yaml
pythia:
  ai:
    max-response-chars: 5000
```
(Resilience4j 설정은 4절 통합 yml 에 포함.)

#### 테스트
- 정상 응답 (5000자 이하, 제어문자 없음) -> 그대로 반환.
- 5001자 응답 -> `AiAnalysisException(RESPONSE_TOO_LARGE)` + retry 미수행(호출 횟수 = 1).
- 제어문자 포함 -> 제거 후 반환.
- `
`, `	` 는 유지.
- blank/null -> `AiAnalysisException(EMPTY_RESPONSE)` + retry 미수행.
- `IOException` 2회 -> 3회째 성공 시 호출 횟수 = 3, 정상 반환.
- `IOException` N회 -> 최종 throw + CB count 증가.
- CB open 상태에서 호출 -> `CallNotPermittedException` 전파.

#### 트레이드오프
- Retry + CB 조합으로 인해 호출 latency 증가 (worst-case max-attempts * wait-duration).
- CB sliding window 가 작아 (10) 초기 장애에 민감 -> minimum-number-of-calls=5 로 완화.
- 정규식 화이트리스트 검증 제외. 필요 시 후속 task 에서 schema-driven validation.

---

### 5.6 H4/M1 - 컨벤션 통일 (Lombok / PythiaRedisProperties)

#### 6.1 Lombok 일관 적용
- 대상:
  - `pythia/src/main/java/com/example/pythia/email/EmailService.java`
  - `pythia/src/main/java/com/example/pythia/ai/service/MetricAnalysisService.java`
  - `pythia/src/main/java/com/example/pythia/alert/service/AlertNotifier.java`
  - `pythia/src/main/java/com/example/pythia/alert/service/ThresholdEvaluator.java`
- 변경: `@RequiredArgsConstructor` 추가, 수동 생성자 제거, final 필드 순서 유지.
- 이미 `@Slf4j` 적용된 클래스는 유지. 미적용 클래스는 본 plan 범위 외.

#### 6.2 PythiaRedisProperties
- 옵션 (A) **채택**: 기존 클래스 유지 + `@Getter @Setter` 적용, 수동 getter/setter 제거.
- 옵션 (B) 보류: record 변환 - `@ConfigurationProperties(prefix = "spring.data.redis")` 가 Spring Boot 자체 `RedisProperties` 와 동일 prefix 사용 시 빈 충돌 위험. nested 바인딩(cluster, sentinel) 호환성도 추가 검증 필요.
- 채택 (A) 사유: 변경 범위 최소화 + 충돌 위험 회피.
- 충돌 발생 시 prefix 를 `pythia.redis.connection` 등으로 변경 + `RedissonConfig` 주입 조정 + `docs/plans/fix-logs/` 기록.

#### 영향 파일
- 수정:
  - 위 4개 서비스 + `pythia/src/main/java/com/example/pythia/redis/config/PythiaRedisProperties.java`
- 회귀 테스트:
  - 기존 Redisson/Redis 컨텍스트 로딩 테스트 (없다면 수동 검증)

#### 의사 코드
```java
@Getter
@Setter
@ConfigurationProperties(prefix = "spring.data.redis")
public class PythiaRedisProperties {
    private String host = "localhost";
    private int port = 6379;
    private String username;
    private String password;
    private int database = 0;
    private Duration timeout = Duration.ofSeconds(3);
    private Cluster cluster = new Cluster();
    private Sentinel sentinel = new Sentinel();

    @Getter @Setter
    public static class Cluster { /* 기존 필드 유지 */ }
    @Getter @Setter
    public static class Sentinel { /* 기존 필드 유지 */ }
}
```

#### 트레이드오프
- Lombok 일괄 적용으로 IDE 의존성 증가, 프로젝트 전반이 이미 Lombok 사용 중이므로 영향 없음.
- record 변환은 후속 task.

---

### 5.7 M5 - JPA 쿼리 인덱스

#### 설계
- 3개 Metric Entity 의 `@Table` 에 `(application, instance, collected_at)` 복합 인덱스 추가.
- prod (`ddl-auto: none`) 환경은 별도 DDL 마이그레이션 안내(스크립트 작성은 본 plan 범위 외).
- test/dev (`ddl-auto: create-drop`) 자동 생성.

#### 영향 파일
- 수정:
  - `pythia/src/main/java/com/example/pythia/metric/domain/JvmMetricSnapshotEntity.java`
  - `pythia/src/main/java/com/example/pythia/metric/domain/HttpMetricSnapshotEntity.java`
  - `pythia/src/main/java/com/example/pythia/metric/domain/HikariMetricSnapshotEntity.java`
- 신규 테스트: 인덱스 검증은 통상 단위 테스트 범위 외. 기존 Repository 테스트 회귀로 갈음.

#### Entity 변경 의사 코드
```java
@Table(
    name = "jvm_metric_snapshot",
    indexes = {
        @Index(
            name = "idx_jvm_metric_app_inst_collected_at",
            columnList = "application, instance, collected_at"
        )
    }
)
@Entity
public class JvmMetricSnapshotEntity { /* ... */ }
```
- Http/Hikari 동일 패턴 (`idx_http_metric_app_inst_collected_at`, `idx_hikari_metric_app_inst_collected_at`).

#### 운영 DDL 안내 (별도 task)
```sql
CREATE INDEX IF NOT EXISTS idx_jvm_metric_app_inst_collected_at
    ON jvm_metric_snapshot (application, instance, collected_at);
CREATE INDEX IF NOT EXISTS idx_http_metric_app_inst_collected_at
    ON http_metric_snapshot (application, instance, collected_at);
CREATE INDEX IF NOT EXISTS idx_hikari_metric_app_inst_collected_at
    ON hikari_metric_snapshot (application, instance, collected_at);
```

#### 트레이드오프
- 인덱스 추가는 쓰기 비용 증가. 본 테이블은 시계열 append-only 라 영향 미미.
- 카디널리티 낮은 application/instance 가 선두 컬럼 - 쿼리 패턴과 정렬 컬럼 일치.

---

## 6. 통합 데이터 흐름 영향

### 6.1 Kafka Consumer -> Threshold -> Alert 흐름
```
Kafka 메시지 수신
  -> Handler.handle(snapshot)
      -> [M4 신규] MessageDeduplicator.markProcessed(...) - fail-open
          - false -> return (스킵)
          - true  -> 후속 처리
      -> ThresholdEvaluator.evaluate(snapshot)
          -> ViolationStateStore.shouldSend(...)
              -> [H3 변경] tryAcquireLock - Resilience4j Functional Retry (violationLock)
          -> AlertNotifier.notify(...)
              -> EmailService.send(...)
                  -> [H1 변경] @Retry(emailSender) - MailException 발생 시 AOP 재시도
      -> MetricStoreService.save(snapshot)
          -> [M5 영향] 인덱스 동반 INSERT
```

### 6.2 AI 분석 흐름
```
MetricAnalysisService.analyze(request)
  -> @Retry(llmAnalysis) outer -> @CircuitBreaker(llmAnalysis) inner
  -> [H2 변경] PromptFactory.build -> 외부 입력 sanitize
  -> ChatClient.call
      - TransientAiException/IOException -> Retry trigger
      - 정상 응답 -> [M3 신규] 응답 검증(size + control char)
          - 검증 실패 -> AiAnalysisException(RESPONSE_TOO_LARGE) -> ignore-exceptions, retry 없음
  -> return sanitized content
```

### 6.3 AOP vs Functional 선택 사유
- AOP (어노테이션): EmailService, MetricAnalysisService - 외부에서 호출되는 public 메서드 boundary 에 위치, self-invocation 무관.
- Functional: ViolationStateStore - 동일 클래스 내부에서 호출되는 헬퍼 메서드 대상. AOP 적용 시 self-invocation 으로 proxy 우회 발생.

### 6.4 self-invocation 회피
- `@Retry`, `@CircuitBreaker` 는 Spring Bean 외부에서 호출되는 public 메서드에만 부착.
- ViolationStateStore 의 `tryAcquireLock` 같은 내부 헬퍼는 Functional API 로 처리.

### 6.5 Redis 키 공간
- 기존: `pythia:alert:violation:*`, `pythia:alert:violation:lock:*`
- 신규: `pythia:kafka:processed:*` (TTL 24h)
- 키 prefix 충돌 없음.

---

## 7. 예외 처리 매핑 - Resilience4j -> 도메인 예외

| 발생 지점 | Resilience4j 처리 | 최종 변환 예외 | 사유 |
|---|---|---|---|
| EmailService.send (MailException) | Retry trigger | 최종 실패 시 caller (`AlertNotifier`) 가 catch + log (기존 정책 유지) | retry 모두 실패 |
| EmailService.send (InterruptedException) | ignore-exceptions | propagate | sleep 도중 interrupt |
| MetricAnalysisService.analyze (TransientAiException/IOException) | Retry trigger | 최종 실패 시 caller wrap | retry 모두 실패 |
| MetricAnalysisService.analyze (AiAnalysisException) | ignore-exceptions | 그대로 propagate | blank/too-large 검증 실패 - retry 무의미 |
| MetricAnalysisService.analyze (CB open) | CallNotPermittedException | caller 에서 catch + log 또는 `AiAnalysisException(LLM_CALL_FAILURE)` 변환 | CB open 상태 |
| ViolationStateStore.tryAcquireLock (LockAcquisitionRetryException) | Retry trigger (sentinel) | `ViolationStateException(LOCK_ACQUISITION_FAILED)` | retry 모두 실패 |
| ViolationStateStore.tryAcquireLock (InterruptedException) | ignore-exceptions | `ViolationStateException(LOCK_INTERRUPTED)` + interrupt flag 복원 | tryLock 도중 interrupt |
| MessageDeduplicator.markProcessed (DataAccessException) | (Resilience4j 미적용) | catch -> true 반환 + warn 로그 | fail-open |

### 신규 ErrorCode
```java
LOCK_INTERRUPTED("VIO_002", "Interrupted while acquiring violation-state lock");
RESPONSE_TOO_LARGE("AI_004", "LLM response exceeds size limit");
```
(코드 번호는 enum 의 다음 순번. 충돌 시 구현 단계 조정.)

### 신규 Exception 클래스
- `LockAcquisitionRetryException` (sentinel, package-private 가능)

---

## 8. 검증 방법

### 8.1 Spring Boot 4 호환성 검증 (Resilience4j 의존성 추가 직후)
1. `./gradlew :pythia:compileJava` - 컴파일 성공.
2. `./gradlew :pythia:test` - 기존 테스트 회귀 0개.
3. `./gradlew :pythia:bootRun --args='--spring.profiles.active=local'` - 컨텍스트 로딩 + Actuator `/actuator/health` 200.
4. **실패 시**: 폴백 시나리오 (코어 모듈 + 수동 Bean 구성) 적용 + `docs/plans/fix-logs/review-improvements-resilience4j-fallback.md` 작성.

### 8.2 빌드/테스트
- `./gradlew :pythia:build` - 전체 컴파일 + 테스트 + 정적 분석.
- 개별 실행: `./gradlew :pythia:test --tests "*EmailServiceRetryTest"` 등.

### 8.3 신규 테스트 목록
| 항목 | 테스트 클래스 |
|---|---|
| H1 | `EmailServiceRetryTest` (Resilience4j 컨텍스트 또는 RetryRegistry 직접 주입) |
| H2 | `MetricAnalysisPromptFactoryInjectionTest` |
| H3 | `ViolationStateStoreLockRetryTest` (RetryRegistry 주입) |
| M4 | `MessageDeduplicatorTest`, 각 Handler 테스트 dedup 케이스 |
| M3 | `MetricAnalysisServiceResponseValidationTest` (Retry/CB 동작 확인 포함) |
| H4/M1 | 신규 없음(Lombok), 기존 컨텍스트 로딩 회귀 |
| M5 | 신규 없음(인덱스), 기존 Repository 테스트 회귀 |

### 8.4 기존 테스트 영향
- Handler 단위 테스트: `MessageDeduplicator` mock 추가 stub (`markProcessed -> true`).
- AlertNotifier 테스트: EmailService mock 호출 횟수는 1 유지 (재시도가 EmailService 내부로 이동했기 때문). 단, `MailException` propagation 변경 시 catch 분기 검증 추가.
- ViolationStateStore 테스트: `RetryRegistry` mock 또는 실제 인스턴스 주입 + `tryLock` mock stub 재시도 횟수 조정.
- MetricAnalysisService 테스트: `@SpringBootTest` 또는 Resilience4j registry 직접 주입.

### 8.5 수동 검증
- Resilience4j Actuator endpoint (`/actuator/retryevents`, `/actuator/circuitbreakerevents`) 노출 시 동작 확인.
- 로컬 PostgreSQL 인덱스 수동 생성 후 `EXPLAIN` 으로 쿼리 플랜 확인.
- Redis CLI 로 dedup 키 TTL 확인.

---

## 9. 트레이드오프 통합

### 9.1 Resilience4j vs 내부 retry loop 비교
| 항목 | Resilience4j | 내부 loop |
|---|---|---|
| 코드 양 | 적음 (어노테이션) | 많음 |
| 외부 설정 | yml 완전 외재화 | properties + 코드 양쪽 |
| 메트릭/관측성 | Micrometer 자동 노출 | 수동 |
| CircuitBreaker | 내장 | 직접 구현 시 복잡 |
| 테스트 | 컨텍스트 또는 registry 주입 | 단순 mockito |
| AOP self-invocation | 어노테이션 한정 위험 | 무관 |
| Boot 4 호환성 | 검증 필요 | 무관 |
| 의존성 추가 | O | X |

### 9.2 항목별 채택 옵션
| 항목 | 채택 | 보류 | 보류 사유 |
|---|---|---|---|
| H1 | Resilience4j @Retry | 내부 retry loop | 코드 감소 + 외재화 + Micrometer |
| H2 | 블랙리스트 sanitize | LLM 기반 인젝션 탐지 | 비용/지연 |
| H3 | Resilience4j Functional Retry | 어노테이션 @Retry | self-invocation 회피 |
| H3 | sentinel exception | tryLock boolean -> Retry result predicate | result predicate 는 설정 복잡, sentinel 이 명료 |
| M4 | Redis SETNX (fail-open, fault tolerance 없음) | Kafka manual ack / Resilience4j 적용 | 구현 규모/지연 |
| M3 | Retry + CB | Retry 만 | LLM 장기 장애 격리 효과 |
| M3 | size + control char | JSON schema 검증 | LLM 응답 자유 형식 |
| H4/M1 | 클래스 유지 + Lombok | record 변환 | prefix 충돌 위험 |
| M5 | `@Table.indexes` | Flyway 마이그레이션 | 본 plan 범위 외 |
| Resilience4j 의존성 | resilience4j-spring-boot3 | 코어 모듈 + 수동 Bean | Boot 4 호환 시 starter 가 간결 |

---

## 10. 완료 조건 체크리스트

### 10.1 의존성 / 호환성
- [ ] `pythia/build.gradle` 에 `resilience4j-spring-boot3:2.2.0` 추가
- [ ] `./gradlew :pythia:compileJava` 통과
- [ ] `./gradlew :pythia:test` 회귀 0
- [ ] `./gradlew :pythia:bootRun` 컨텍스트 로딩 성공
- [ ] (실패 시) 폴백 시나리오 적용 + fix-log 작성

### 10.2 H1 (이메일 재시도)
- [ ] `EmailService.send` 에 `@Retry(name = "emailSender")` 부착
- [ ] `MailException` catch 제거, propagation 으로 변경
- [ ] `AlertNotifier` 호출부 catch 분기 검증 및 필요 시 수정
- [ ] `application.yml` `resilience4j.retry.instances.emailSender` 추가
- [ ] `pythia.email.retry.*` 제거
- [ ] `EmailServiceRetryTest` 통과 (성공 후 종료 / 모두 실패)
- [ ] 기존 `AlertNotifier` 테스트 회귀 통과

### 10.3 H2 (프롬프트 인젝션)
- [ ] `sanitizeForPrompt` 추가 + 모든 외부 입력 적용
- [ ] `MetricAnalysisPromptFactoryInjectionTest` 통과 (제어문자/마크다운/키워드/길이/정상값)
- [ ] 기존 PromptFactory 테스트 회귀 통과

### 10.4 H3 (Lock 재시도)
- [ ] `LockAcquisitionRetryException` 신규 작성
- [ ] `ViolationStateStore.tryAcquireLock` private 헬퍼 추출
- [ ] `RetryRegistry.retry("violationLock").executeCallable(...)` 적용
- [ ] `InterruptedException` ignore + LOCK_INTERRUPTED 변환
- [ ] `ViolationStateErrorCode.LOCK_INTERRUPTED` 존재 확인 (없으면 추가)
- [ ] `ViolationStateProperties` 의 lock retry 필드 제거 (있다면)
- [ ] `application.yml` `resilience4j.retry.instances.violationLock` 추가
- [ ] `ViolationStateStoreLockRetryTest` 통과 (성공 / 실패 / interrupted)

### 10.5 M4 (Kafka 멱등성)
- [ ] `MessageDeduplicator`, `KafkaDedupProperties` 신규 작성
- [ ] `@EnableConfigurationProperties(KafkaDedupProperties.class)` 등록
- [ ] 3개 Handler 에 dedup 진입 가드 적용
- [ ] `application.yml` `pythia.kafka.dedup.ttl` 기본값
- [ ] `MessageDeduplicatorTest` 통과 (setIfAbsent true/false + fail-open)
- [ ] 각 Handler 테스트 dedup 케이스 추가 통과

### 10.6 M3 (LLM 응답 검증 + Retry/CB)
- [ ] `AiErrorCode.RESPONSE_TOO_LARGE` 추가
- [ ] `AiAnalysisProperties` 신규 작성
- [ ] `MetricAnalysisService.analyze` 에 `@Retry` + `@CircuitBreaker` 부착
- [ ] 응답 검증 로직 추가 (size + control char)
- [ ] `application.yml` `resilience4j.retry.instances.llmAnalysis` + `resilience4j.circuitbreaker.instances.llmAnalysis` + `pythia.ai.max-response-chars` 추가
- [ ] `MetricAnalysisServiceResponseValidationTest` 통과 (정상/초과/제어문자/blank/retry/CB open)

### 10.7 H4/M1 (컨벤션 통일)
- [ ] 4개 서비스 클래스 `@RequiredArgsConstructor` 적용 + 수동 생성자 제거
- [ ] `PythiaRedisProperties` `@Getter @Setter` 적용 + 수동 getter/setter 제거
- [ ] 기존 컨텍스트 로딩/회귀 테스트 통과
- [ ] (충돌 발생 시) prefix 변경 + `docs/plans/fix-logs/` 기록

### 10.8 M5 (인덱스)
- [ ] 3개 Entity `@Table.indexes` 추가
- [ ] 기존 Repository 테스트 회귀 통과
- [ ] prod DDL 마이그레이션 안내가 plan 에 명시 (본 문서 5.7 절)

### 10.9 통합
- [ ] `./gradlew :pythia:build` 통과
- [ ] 신규 yml 키가 모두 `application.yml` 에 반영
- [ ] 신규 ErrorCode 가 enum 에 모두 등록
- [ ] 본 plan 의 모든 항목이 commit/PR 단위로 분리 가능한지 점검 (우선순위 순)
- [ ] Resilience4j Actuator endpoint 노출 여부 결정 (별도 task 가능)

---

## 11. 폴백 시나리오 - Spring Boot 4 호환 안 될 때

### 11.1 트리거 조건
- `./gradlew :pythia:compileJava` 또는 `./gradlew :pythia:test` 또는 `bootRun` 실패 (Resilience4j Spring Boot 3 starter 가 Boot 4 와 비호환).
- 증상 예시: `ClassNotFoundException`, `NoSuchMethodError`, AutoConfiguration 로딩 실패.

### 11.2 폴백 절차
1. `resilience4j-spring-boot3` 제거.
2. 코어 모듈 추가:
   - `io.github.resilience4j:resilience4j-retry:2.2.0`
   - `io.github.resilience4j:resilience4j-circuitbreaker:2.2.0`
   - `io.github.resilience4j:resilience4j-micrometer:2.2.0` (optional)
3. 수동 구성 빈 작성:
   - `pythia/src/main/java/com/example/pythia/common/resilience/ResilienceConfig.java`
   - `RetryRegistry`, `CircuitBreakerRegistry` Bean 직접 생성. yml 의 `resilience4j.*` 값을 `@ConfigurationProperties` 로 바인딩.
4. 어노테이션 (`@Retry`, `@CircuitBreaker`) 미동작 -> 모든 적용 지점을 Functional API 로 전환:
   - EmailService: `retryRegistry.retry("emailSender").executeRunnable(() -> mailSender.send(...));`
   - MetricAnalysisService: CB + Retry 조합. `Retry.decorateCallable(retry, CircuitBreaker.decorateCallable(cb, callable)).call()`.
   - ViolationStateStore: 기존 Functional Retry 그대로.
5. 폴백 사유, 채택 모듈, 변경 지점을 `docs/plans/fix-logs/review-improvements-resilience4j-fallback.md` 에 기록.

### 11.3 폴백 시 플랜 영향
- 어노테이션 기반 H1, M3 의 의사 코드가 Functional 로 전환.
- 테스트는 동일 (RetryRegistry 직접 주입 형태).
- yml 설정은 그대로 사용 (직접 바인딩).
- 완료 조건 체크리스트의 "어노테이션 부착" 항목은 "Functional 적용" 으로 치환.
