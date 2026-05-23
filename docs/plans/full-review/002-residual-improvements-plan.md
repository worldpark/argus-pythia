# Residual Improvements Plan

> 작성일: 2026-05-23
> 관련 모듈: pythia
> 선행 작업: docs/plans/full-review/001-review-improvements-plan.md, docs/plans/fix-logs/023-boot4-resilience4j-startup-fix.md
> 범위: 직전 재리뷰에서 발견된 잔존/신규 약점 8건 통합 개선 (구현 순서 고정)
> 비고: 본 문서는 plan 문서이며 구현은 별도 지시 시 진행한다.

---

## 1. 개요

### 1.1 배경
직전 작업(review-improvements-plan 7항목)과 후속 호환성 수정(023-boot4-resilience4j-startup-fix) 이후 수행한 재리뷰에서 잔존 약점(F1~F4)과 신규 약점(N1, N4, N5, N8)이 식별되었다. 본 plan 은 이 8건을 우선순위에 따라 통합 개선한다.

### 1.2 우선순위 요약표

| #  | ID  | 영역                              | 우선순위 | 비고                                                |
|----|-----|-----------------------------------|----------|-----------------------------------------------------|
| 1  | F1  | application-test.yml resilience4j | HIGH     | 운영/테스트 환경 일치                                |
| 2  | F2  | Spring AI TransientAiException    | HIGH     | 의존성 명시 또는 fix-log                            |
| 3  | N1  | MessageDeduplicator key sanitize  | HIGH     | 콜론 충돌 방지                                       |
| 4  | F3  | llmAnalysis ignore-exceptions     | MEDIUM   | 이미 적용됨 - 사전 검증 후 plan 에서 제외 결정       |
| 5  | N5  | ViolationStateProperties Lombok   | MEDIUM   | 보일러플레이트 감소                                  |
| 6  | N4  | KafkaDedupProperties 패키지 이동  | MEDIUM   | 일관성 향상                                          |
| 7  | N8  | ThresholdEvaluator catch 분리     | MEDIUM   | 로그 노이즈 감소                                     |
| 8  | F4  | Actuator/Micrometer 자동 노출     | MEDIUM   | Boot 4 호환성 검증 필요 (폴백 fix-log 계획 포함)     |

> F3 사전 검증 결과: 현재 application.yml resilience4j.retry.instances.llmAnalysis.ignore-exceptions (line 152~153) 및 resilience4j.circuitbreaker.instances.llmAnalysis.ignore-exceptions (line 173~174) 양쪽에 com.example.pythia.ai.exception.AiAnalysisException 가 이미 등록되어 있다. 본 plan 의 구현 단계에서는 F3 를 제외하며, 완료 조건 체크리스트에 "F3: 사전 적용 확인 완료" 항목으로만 남긴다.

### 1.3 Out of Scope
다음 항목은 본 plan 에서 다루지 않는다. 별도 task 분리 권장.

- N3: 이메일 영속 실패 DLQ (신규 인프라 필요 - Kafka topic 또는 DB)
- N7: Redis 장애 시 dedup + lock 상호작용 (설계 변경 필요)
- LOW 항목: Class.forName ClassLoader 지정, CallNotPermittedException 별도 catch, 응답 검증 JavaDoc, ConfigurationProperties 검증 어노테이션, executeSupplier checked exception 보완

---

## 2. 항목별 상세

### 2.1 F1 - application-test.yml resilience4j 설정 추가 (HIGH)

#### 설계
운영 application.yml 의 resilience4j.retry.instances.*, resilience4j.circuitbreaker.instances.* 정의 누락으로 인해 테스트 컨텍스트(@SpringBootTest + ActiveProfiles("test"))에서 ResilienceConfig 가 빈 instances Map 으로 RetryRegistry / CircuitBreakerRegistry Bean 을 생성한다. 운영과 동일한 instance 키(emailSender, llmAnalysis, violationLock)를 테스트 프로파일에도 정의하여 컨텍스트 로딩 시점에서의 동작을 일치시킨다.

테스트 환경 특성상 다음을 적용한다.
- max-attempts 는 운영과 동일 유지 (실제 재시도 동작 검증 가능)
- wait-duration 은 테스트 속도를 위해 짧게 축소 (10ms~50ms 범위)
- enable-exponential-backoff 는 운영과 동일

#### 영향 파일
- 수정: pythia/src/test/resources/application-test.yml

#### 의사 코드 (yaml)
```yaml
resilience4j:
  retry:
    instances:
      emailSender:
        max-attempts: 3
        wait-duration: 10ms
        enable-exponential-backoff: true
        exponential-backoff-multiplier: 2.0
        retry-exceptions:
          - org.springframework.mail.MailException
        ignore-exceptions:
          - java.lang.InterruptedException
      llmAnalysis:
        max-attempts: 3
        wait-duration: 10ms
        enable-exponential-backoff: true
        exponential-backoff-multiplier: 2.0
        retry-exceptions:
          - org.springframework.ai.retry.TransientAiException
        ignore-exceptions:
          - com.example.pythia.ai.exception.AiAnalysisException
      violationLock:
        max-attempts: 3
        wait-duration: 10ms
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
        wait-duration-in-open-state: 1s
        sliding-window-size: 10
        sliding-window-type: COUNT_BASED
        minimum-number-of-calls: 5
        permitted-number-of-calls-in-half-open-state: 3
        automatic-transition-from-open-to-half-open-enabled: true
        ignore-exceptions:
          - com.example.pythia.ai.exception.AiAnalysisException
```

#### 테스트
- 기존 PythiaApplicationTests.contextLoads 가 PASS 유지되는지 확인
- 신규 단위 테스트는 불필요 (yml 만 변경하는 환경 일치 작업). 회귀 영향은 전체 테스트 실행으로 검증.

#### 트레이드오프
- 장점: 컨텍스트 로딩 검증 단계에서 운영과 동등한 Bean 구성 보장. 향후 @MockitoBean 없이 RetryRegistry 사용 통합 테스트 작성 가능.
- 단점: 테스트 yml 이 운영 yml 과 중복 정의됨. 운영 정책 변경 시 두 곳을 모두 수정해야 한다 (drift 위험). 본 plan 에서는 두 파일을 명시적으로 관리하는 단순 접근을 채택한다 (Spring profile import 등 추가 메커니즘 도입은 과도한 설계로 판단).

---

### 2.2 F2 - Spring AI TransientAiException 의존성 명시 (HIGH)

#### 설계
MetricAnalysisService 는 org.springframework.ai.retry.TransientAiException 을 import 하고, application.yml 의 llmAnalysis.retry-exceptions 에도 동일 FQCN 이 등록되어 있다. 이 클래스는 현재 spring-ai-starter-model-openai 의 transitive 의존을 통해 클래스패스에 들어오며, 명시적 의존성이 선언되어 있지 않다.

구현 단계에서 다음 순서로 진행한다.
1. `./gradlew :pythia:dependencyInsight --configuration runtimeClasspath --dependency spring-ai-retry` (또는 `--dependency org.springframework.ai`) 로 TransientAiException 가 어느 모듈에 존재하는지 확인한다.
2. 모듈 식별이 끝나면 다음 중 하나를 선택한다.
   - A안: build.gradle 에 명시적 의존성 추가. BOM 사용 중이므로 버전은 생략.
   - B안: transitive 가 안정적임이 확인되면 의존성 추가 없이 docs/plans/fix-logs/ 에 결정 사유 기록.
3. 구현 에이전트는 1번 결과에 따라 A/B 안을 결정하고, 본 plan 의 완료 조건을 충족시킨다.

#### 영향 파일
- 조사: pythia/build.gradle (의존성 트리 확인)
- 수정 (A안 선택 시): pythia/build.gradle 에 implementation 'org.springframework.ai:<resolved-module>' 추가
- 신규 (B안 선택 시): docs/plans/fix-logs/024-spring-ai-retry-transitive-decision.md

#### 의사 코드 (groovy)
```groovy
// A안: build.gradle dependencies 블록 추가 (모듈명은 조사 결과로 확정)
implementation 'org.springframework.ai:spring-ai-retry'
```

#### 테스트
- MetricAnalysisServiceTest, MetricAnalysisServiceResponseValidationTest PASS 유지
- 컴파일/패키징 시 TransientAiException 미해결 없음 확인

#### 트레이드오프
- A안 장점: 향후 starter 변경/제거 시에도 명시적 의존성으로 안정적.
- A안 단점: BOM 의 모듈 명칭이 milestone(2.0.0-M6) 단계에서 변경될 가능성 존재.
- B안 장점: 의존성 트리 단순.
- B안 단점: starter 가 내부 모듈 구성 변경 시 컴파일 실패 위험.

---

### 2.3 N1 - MessageDeduplicator Redis key sanitize (HIGH)

#### 설계
현재 MessageDeduplicator.markProcessed 가 Redis key 를 다음 형식으로 생성한다.

```
pythia:kafka:processed:{topic}:{application}:{instance}:{epochMs}
```

application 또는 instance 값에 콜론(:)이 포함되면 인접 segment 와 구분이 모호해진다 (예: instance="localhost:8080" 시 epoch ms 와 경계 충돌). ViolationStateStore.sanitize(String) 가 이미 콜론을 언더스코어로 치환하는 동일 패턴을 적용 중이므로, 동일 방식을 재사용한다.

topic 은 Kafka 명명 규약상 일반적으로 콜론을 포함하지 않으나, 일관성을 위해 동일하게 sanitize 한다. epochMs 는 숫자이므로 sanitize 불필요.

코드 중복을 피하기 위해 ViolationStateStore.sanitize 를 별도 utility 로 이동하는 방안도 가능하나, 본 plan 의 "신규 Bean/패키지 최소화" 제약에 따라 MessageDeduplicator 내부에 동일 로직의 private static helper 를 둔다. 향후 sanitize 가 3개 이상의 클래스에서 사용되면 그 시점에 utility 추출을 별도 task 로 진행한다.

#### 영향 파일
- 수정: pythia/src/main/java/com/example/pythia/kafka/consumer/MessageDeduplicator.java
- 수정: pythia/src/test/java/com/example/pythia/kafka/consumer/MessageDeduplicatorTest.java

#### 의사 코드 (java)
```java
public boolean markProcessed(String topic, String application, String instance,
    OffsetDateTime collectedAt) {
  String key = "pythia:kafka:processed:%s:%s:%s:%d"
      .formatted(
          sanitize(topic),
          sanitize(application),
          sanitize(instance),
          collectedAt.toInstant().toEpochMilli());
  // ... 기존 try/catch 동일
}

private static String sanitize(String value) {
  if (value == null) {
    return "-";
  }
  return value.replace(":", "_");
}
```

#### 테스트
기존 MessageDeduplicatorTest.redis_key_구성_검증 는 localhost:8080 이 key 에 그대로 포함되는 것을 검증하므로 sanitize 적용 후 깨진다. 다음과 같이 수정 + 추가한다.

1. redis_key_구성_검증 의 contains assertion 을 localhost_8080 으로 변경
2. 신규 테스트: application 에 콜론 포함 시 언더스코어 치환
   - application="my:app", instance="host", key 가 my_app 포함, my:app 미포함 검증
3. 신규 테스트: application 또는 instance 가 null 일 경우 - 로 치환
   - 양쪽 모두 null 시 key 에 - 토큰이 포함됨 검증

기존 setIfAbsent_*, DataAccessException_발생시_fail_open_*, 두번째_호출시_중복_감지 테스트들은 instance="localhost:8080" 이 sanitize 후 "localhost_8080" 으로 변환되더라도 동작은 동일하므로 영향 없음. (assertion 이 key 내용을 검증하지 않음.)

#### 트레이드오프
- 장점: epoch ms segment 경계가 명확해지고, 향후 instance 명명 규약 변경에도 안전.
- 단점: 기존 운영 Redis 에 localhost:8080 형식으로 저장된 key 와 새 key (localhost_8080) 가 공존 가능. dedup TTL(24h) 경과 후 자연 해소되므로 별도 마이그레이션 불필요. fix-log 에 명시.

---

### 2.4 F3 - llmAnalysis ignore-exceptions AiAnalysisException 추가 (MEDIUM, 사전 적용 확인)

#### 사전 검증 결과
application.yml 현재 상태 확인 결과 다음 두 위치에 이미 com.example.pythia.ai.exception.AiAnalysisException 가 등록되어 있다.

- resilience4j.retry.instances.llmAnalysis.ignore-exceptions (line 152~153)
- resilience4j.circuitbreaker.instances.llmAnalysis.ignore-exceptions (line 173~174)

따라서 본 항목은 구현 작업 없음. 구현 단계에서 yml 재확인만 수행하고 완료 조건에서 "사전 적용 확인 완료" 로 기록한다.

#### 영향 파일
- 없음

---

### 2.5 N5 - ViolationStateProperties Lombok 적용 (MEDIUM)

#### 설계
현재 ViolationStateProperties 는 5개 필드에 대해 수동 getter/setter 10개 메서드(getter 5 + setter 5)를 보유한다. 프로젝트의 다른 ConfigurationProperties 클래스(PythiaRedisProperties, KafkaDedupProperties 등)가 이미 @Getter @Setter 를 사용 중이므로 동일 패턴을 적용하여 보일러플레이트를 제거한다.

ConfigurationProperties 바인딩은 setter 기반(JavaBean) 으로 동작하므로 Lombok @Setter 가 정상 생성하는 setter 시그니처와 호환된다.

#### 영향 파일
- 수정: pythia/src/main/java/com/example/pythia/alert/config/ViolationStateProperties.java

#### 의사 코드 (java)
```java
package com.example.pythia.alert.config;

import java.time.Duration;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "pythia.alert.violation-state")
@Getter
@Setter
public class ViolationStateProperties {

  private Duration ttl = Duration.ofHours(1);
  private String keyPrefix = "pythia:alert:violation:";
  private String lockKeyPrefix = "pythia:alert:violation:lock:";
  private Duration lockWait = Duration.ofMillis(200);
  private Duration lockLease = Duration.ofMillis(3000);
}
```

#### 테스트
- public API(getter/setter 시그니처)는 동일하게 유지되므로 기존 ViolationStateStore 및 ViolationStateStoreTest 영향 없음.
- 신규 테스트 불필요 (Lombok 생성 코드는 컴파일러 보장).
- 회귀: 컨텍스트 로딩 시 properties 바인딩이 정상 동작하는지 PythiaApplicationTests.contextLoads 로 검증.

#### 트레이드오프
- 장점: 5개 필드 → getter/setter 10개 메서드 제거. 향후 필드 추가 시 유지보수 비용 절감.
- 단점: Lombok 의존 (이미 프로젝트 전반 적용 중이므로 추가 비용 없음).

---

### 2.6 N4 - KafkaDedupProperties 패키지 이동 (MEDIUM)

#### 설계
현재 KafkaDedupProperties 는 kafka/consumer/ 에 위치하나, 다른 Kafka 관련 ConfigurationProperties 및 설정 클래스(KafkaConsumerConfig)는 kafka/config/ 패키지에 있다. 일관성을 위해 kafka/config/ 로 이동한다.

#### 영향 파일
- 신규: pythia/src/main/java/com/example/pythia/kafka/config/KafkaDedupProperties.java
- 삭제: pythia/src/main/java/com/example/pythia/kafka/consumer/KafkaDedupProperties.java
- 수정 (import 변경):
  - pythia/src/main/java/com/example/pythia/kafka/config/KafkaConsumerConfig.java
  - pythia/src/main/java/com/example/pythia/kafka/consumer/MessageDeduplicator.java
  - pythia/src/test/java/com/example/pythia/kafka/consumer/MessageDeduplicatorTest.java

#### 의사 코드 (java)
```java
// 신규: pythia/src/main/java/com/example/pythia/kafka/config/KafkaDedupProperties.java
package com.example.pythia.kafka.config;

import java.time.Duration;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "pythia.kafka.dedup")
@Getter
@Setter
public class KafkaDedupProperties {
  private Duration ttl = Duration.ofHours(24);
}
```

```java
// KafkaConsumerConfig.java: import 변경
// 제거: import com.example.pythia.kafka.consumer.KafkaDedupProperties;
// 추가: import com.example.pythia.kafka.config.KafkaDedupProperties;
```

```java
// MessageDeduplicator.java: 패키지 동일(kafka.consumer), import 추가
// 추가: import com.example.pythia.kafka.config.KafkaDedupProperties;
```

```java
// MessageDeduplicatorTest.java: import 추가
// 추가: import com.example.pythia.kafka.config.KafkaDedupProperties;
```

#### 테스트
- 기존 MessageDeduplicatorTest 6개 케이스 PASS 유지
- 컨텍스트 로딩 (PythiaApplicationTests.contextLoads) PASS 유지
- 신규 테스트 불필요 (단순 이동).

#### 트레이드오프
- 장점: Kafka 설정 클래스 위치 일관성.
- 단점: 이동 시 누락된 참조가 있으면 컴파일 실패 - 구현 단계에서 grep 으로 전체 참조 재확인 필요.

---

### 2.7 N8 - ThresholdEvaluator catch 분리 (MEDIUM)

#### 설계
현재 ThresholdEvaluator.evaluateOne (line 160~162) 가 모든 RuntimeException 을 동일 로그 레벨(error)로 처리한다. ViolationStateException 은 Redis 장애 등 예상된 상위 카테고리이며, 기타 RuntimeException 은 코드 결함 또는 알 수 없는 외부 이슈일 가능성이 높다. 두 경우를 분리하여 로그 메시지/레벨을 차별화한다.

- ViolationStateException: warn 레벨 + 에러 코드 포함 (Redis 장애로 분류, 운영 알람 정책상 별도 채널)
- 기타 RuntimeException: error 레벨 + stack trace 포함 (의도치 않은 결함)

두 경우 모두 단일 메트릭 평가에서 발생한 예외를 swallow 하여 다른 메트릭 평가는 계속 진행하도록 한다 (기존 동작 유지).

#### 영향 파일
- 수정: pythia/src/main/java/com/example/pythia/alert/service/ThresholdEvaluator.java
- 신규: pythia/src/test/java/com/example/pythia/alert/service/ThresholdEvaluatorExceptionHandlingTest.java

#### 의사 코드 (java)
```java
private void evaluateOne(MetricKind kind, String app, String instance, String sub,
    BigDecimal value, MetricStatus status, Limit limit) {
  try {
    // ... 기존 로직 동일
  } catch (ViolationStateException e) {
    log.warn("Violation state operation failed: kind={} app={} instance={} sub={} code={} msg={}",
        kind, app, instance, sub, e.getErrorCode().code(), e.getMessage());
  } catch (RuntimeException e) {
    log.error("Unexpected error evaluating {}: app={} instance={} sub={}",
        kind, app, instance, sub, e);
  }
}
```

> 주의: e.getErrorCode().code() 호출 가능 여부는 CustomException 의 API 에 따른다. 구현 단계에서 시그니처 확인 후 동일 의미 추출 방식을 사용한다. 메서드명이 다르면 그에 맞춰 조정한다.

#### 테스트
신규 테스트 클래스 ThresholdEvaluatorExceptionHandlingTest:
- ViolationStateException 발생 시 다른 메트릭 평가 계속 진행
  - mock ViolationStateStore.shouldSend 가 첫 호출에서 ViolationStateException throw, 두 번째 호출에서 정상 동작
  - JVM 스냅샷에 cpu + memory 양쪽 데이터 포함
  - notifier 가 두 번째 메트릭에 대해 정상 호출됨 검증
- 기타 RuntimeException 발생 시 swallow 후 다음 메트릭 계속
  - mock 이 IllegalStateException throw
  - 동일 시나리오로 두 번째 메트릭 정상 처리 검증

로그 레벨 검증은 본 plan 의 범위 외(테스트 비용 대비 가치 낮음).

#### 트레이드오프
- 장점: 운영 로그에서 Redis 장애와 코드 결함을 구분 가능.
- 단점: catch 블록 두 개로 증가. catch 순서 의존성 발생 (ViolationStateException -> RuntimeException). 테스트 1개 클래스 신규 추가.

---

### 2.8 F4 - Actuator + resilience4j-micrometer 노출 (MEDIUM, Boot 4 호환성 검증 포함)

#### 설계
Resilience4j Boot 3 starter 제거(023) 이후 Retry/CircuitBreaker 의 metric 자동 노출이 사라졌다. Spring Boot Actuator 와 resilience4j-micrometer 모듈을 추가하여 다음을 노출한다.

- resilience4j.retry.calls (성공/실패/재시도 횟수)
- resilience4j.circuitbreaker.state, resilience4j.circuitbreaker.calls 등

구현 단계 순서:
1. build.gradle 에 의존성 추가 -> ./gradlew :pythia:build 로 컴파일/테스트 검증
2. 컴파일 성공 시 ResilienceConfig 에서 RetryRegistry/CircuitBreakerRegistry 에 TaggedRetryMetrics / TaggedCircuitBreakerMetrics 를 MeterRegistry 와 bind 하는 Bean 추가
3. application.yml 에 management.endpoints.web.exposure.include 설정 추가
4. 통합 테스트로 metric endpoint 노출 확인

Boot 4 호환성 폴백 plan: 의존성 추가 후 컴파일 또는 컨텍스트 로딩이 실패하면 다음 폴백을 적용한다.
- micrometer 변경 보류
- docs/plans/fix-logs/025-boot4-resilience4j-micrometer-incompat.md 작성: 실패 로그, 시도한 의존성 조합, 회피 결정 기록
- F4 완료 조건은 "fix-log 작성으로 대체 완료" 로 처리

#### 영향 파일
- 수정: pythia/build.gradle
  - 신규: implementation 'org.springframework.boot:spring-boot-starter-actuator'
  - 신규: implementation 'io.github.resilience4j:resilience4j-micrometer:2.2.0'
- 수정: pythia/src/main/resources/application.yml
- 수정: pythia/src/main/resources/application-test.yml (필요 시 endpoint exposure 만)
- 수정: pythia/src/main/java/com/example/pythia/resilience/config/ResilienceConfig.java
- 신규: pythia/src/test/java/com/example/pythia/resilience/MetricsExposureTest.java
- (폴백 시) 신규: docs/plans/fix-logs/025-boot4-resilience4j-micrometer-incompat.md

#### 의사 코드 (groovy)
```groovy
// build.gradle dependencies
implementation 'org.springframework.boot:spring-boot-starter-actuator'
implementation 'io.github.resilience4j:resilience4j-micrometer:2.2.0'
```

#### 의사 코드 (java)
```java
// ResilienceConfig.java 추가
@Bean
public TaggedRetryMetrics taggedRetryMetrics(
    RetryRegistry retryRegistry, MeterRegistry meterRegistry) {
  TaggedRetryMetrics metrics = TaggedRetryMetrics.ofRetryRegistry(retryRegistry);
  metrics.bindTo(meterRegistry);
  return metrics;
}

@Bean
public TaggedCircuitBreakerMetrics taggedCircuitBreakerMetrics(
    CircuitBreakerRegistry circuitBreakerRegistry, MeterRegistry meterRegistry) {
  TaggedCircuitBreakerMetrics metrics =
      TaggedCircuitBreakerMetrics.ofCircuitBreakerRegistry(circuitBreakerRegistry);
  metrics.bindTo(meterRegistry);
  return metrics;
}
```

#### 의사 코드 (yaml)
```yaml
# application.yml 추가
management:
  endpoints:
    web:
      exposure:
        include: health,metrics
```

#### 테스트
신규 MetricsExposureTest (@SpringBootTest + @AutoConfigureMockMvc + ActiveProfiles("test")):
- GET /actuator/metrics 200 응답 검증
- 응답 본문에 resilience4j.retry.calls 또는 resilience4j.circuitbreaker.calls 명 포함 검증

기존 테스트:
- PythiaApplicationTests.contextLoads PASS 유지
- 모든 기존 테스트 PASS 유지

폴백 적용 시:
- 신규 테스트 작성 불필요 (의존성 미추가). fix-log 가 변경사항을 대체한다.

#### 트레이드오프
- 장점: 운영 환경에서 Retry/CircuitBreaker 상태 관측 가능 (Grafana 연동 기반 마련).
- 단점: Actuator endpoint 노출 시 보안 정책 필요. 본 plan 에서는 health, metrics 만 노출하며 /actuator/env 등 민감 endpoint 는 노출하지 않는다. 운영 환경에서는 별도 task 로 actuator 보안(Spring Security 적용)을 정의할 필요가 있다 - 본 plan 범위 외.
- Boot 4 호환성 미확정: 폴백 plan 으로 위험 흡수.

---

## 3. 신규 의존성 영향

### 3.1 추가 의존성 (F2, F4)

| 모듈                                                      | 항목 | 사유                          |
|-----------------------------------------------------------|------|-------------------------------|
| org.springframework.ai:spring-ai-retry (또는 동등 모듈)   | F2   | TransientAiException 명시 의존 (조사 후 확정) |
| org.springframework.boot:spring-boot-starter-actuator     | F4   | Actuator metric endpoint      |
| io.github.resilience4j:resilience4j-micrometer:2.2.0      | F4   | Resilience4j -> Micrometer bind |

### 3.2 의존성 충돌/호환성 검증
- F4 두 모듈 추가 후 `./gradlew :pythia:dependencies --configuration runtimeClasspath` 로 micrometer-core 버전 충돌 확인
- Spring Boot 4 가 관리하는 micrometer 버전과 resilience4j-micrometer:2.2.0 의 호환성이 깨질 경우 폴백 plan(fix-log 작성)으로 전환

---

## 4. 통합 데이터 흐름 영향

### 4.1 변경 없는 흐름
- Kafka consume -> MessageDeduplicator -> ThresholdEvaluator -> ViolationStateStore -> AlertNotifier -> MetricAnalysisService -> EmailService
  - 본 plan 의 모든 항목은 비즈니스 로직 변경 없음
  - public API 시그니처 유지

### 4.2 영향 부분
- N1: Redis key 형식 변경 (localhost:8080 -> localhost_8080). 기존 운영 key 와 일시 공존. dedup TTL(24h) 경과 후 자연 해소.
- N4: 패키지 이동. 외부 코드 영향 없음 (모든 참조는 동일 모듈 내).
- N8: 로그 메시지 변경. 로그 파싱 도구 사용 시 영향 가능 (현재 미사용).
- F4: 신규 endpoint /actuator/health, /actuator/metrics 노출. 외부 접근 시 보안 고려 필요 (별도 task).

---

## 5. 검증 방법

### 5.1 빌드/테스트
```powershell
.\gradlew.bat :pythia:build
```

세부 단계:
```powershell
# F1: 컨텍스트 로딩
.\gradlew.bat :pythia:test --tests com.example.pythia.PythiaApplicationTests

# N1: MessageDeduplicator 테스트
.\gradlew.bat :pythia:test --tests com.example.pythia.kafka.consumer.MessageDeduplicatorTest

# N5: ViolationStateStore 회귀
.\gradlew.bat :pythia:test --tests com.example.pythia.alert.state.ViolationStateStoreTest

# N4: 컴파일 검증 + Kafka consumer 통합 영향
.\gradlew.bat :pythia:compileJava
.\gradlew.bat :pythia:test --tests com.example.pythia.kafka.consumer.*

# N8: 신규 예외 처리 테스트
.\gradlew.bat :pythia:test --tests com.example.pythia.alert.service.ThresholdEvaluatorExceptionHandlingTest

# F4: metric endpoint
.\gradlew.bat :pythia:test --tests com.example.pythia.resilience.MetricsExposureTest
```

### 5.2 회귀 영향
- EmailServiceRetryTest, MetricAnalysisServiceTest, MetricAnalysisServiceResponseValidationTest, ViolationStateStoreTest 전체 PASS 유지
- ./gradlew :pythia:build BUILD SUCCESSFUL

### 5.3 의존성 검증 (F2, F4)
```powershell
.\gradlew.bat :pythia:dependencyInsight --configuration runtimeClasspath --dependency spring-ai-retry
.\gradlew.bat :pythia:dependencies --configuration runtimeClasspath
```

---

## 6. 트레이드오프 통합

| 항목 | 주요 트레이드오프                                                                      |
|------|----------------------------------------------------------------------------------------|
| F1   | 운영/테스트 yml 중복 발생 - drift 위험 vs 명확성. 명확성 우선.                          |
| F2   | A안(명시 의존) 안정성 우위 vs 의존 트리 증가. B안(transitive 유지) 단순 vs 변경 위험.    |
| N1   | 기존 key 형식 일시 공존 (TTL 24h 내 자연 해소).                                          |
| F3   | 이미 적용됨 - 추가 작업 없음.                                                            |
| N5   | Lombok 의존 (이미 전역 적용 중이므로 비용 없음).                                         |
| N4   | 외부 영향 없음. 누락 import 위험은 grep 으로 보완.                                       |
| N8   | catch 블록 분기 증가 vs 운영 로그 가독성. 가독성 우선.                                   |
| F4   | Boot 4 호환성 미확정 - 폴백 fix-log 으로 위험 흡수. Actuator 보안은 별도 task.            |

---

## 7. 완료 조건 체크리스트

### F1 - application-test.yml resilience4j
- [ ] application-test.yml 에 resilience4j.retry.instances.emailSender/llmAnalysis/violationLock 추가
- [ ] application-test.yml 에 resilience4j.circuitbreaker.instances.llmAnalysis 추가
- [ ] PythiaApplicationTests.contextLoads PASS

### F2 - Spring AI TransientAiException
- [ ] gradle dependencyInsight 로 모듈 식별 완료
- [ ] A안(build.gradle 명시 의존성 추가) 또는 B안(fix-log 작성) 중 결정 적용
- [ ] MetricAnalysisServiceTest, MetricAnalysisServiceResponseValidationTest PASS

### N1 - MessageDeduplicator sanitize
- [ ] MessageDeduplicator.markProcessed 에 sanitize(topic/application/instance) 적용
- [ ] private static helper sanitize(String) 추가 (null -> -, : -> _)
- [ ] 기존 테스트 redis_key_구성_검증 assertion 갱신 (localhost_8080)
- [ ] 신규 테스트 2건 추가 (콜론 sanitize, null sanitize)
- [ ] MessageDeduplicatorTest 8건 PASS

### F3 - llmAnalysis ignore-exceptions
- [x] application.yml 양쪽 위치(retry, circuitbreaker)에 AiAnalysisException 등록 확인 완료 (사전 검증)
- [x] 사전 적용 확인 완료 (코드 변경 없음)

### N5 - ViolationStateProperties Lombok
- [ ] 수동 getter/setter 10개 제거
- [ ] @Getter @Setter 추가
- [ ] PythiaApplicationTests.contextLoads PASS
- [ ] ViolationStateStoreTest PASS 유지

### N4 - KafkaDedupProperties 패키지 이동
- [ ] kafka/config/KafkaDedupProperties.java 신규 생성
- [ ] kafka/consumer/KafkaDedupProperties.java 삭제
- [ ] KafkaConsumerConfig, MessageDeduplicator, MessageDeduplicatorTest import 변경
- [ ] grep 으로 잔여 참조 0건 확인
- [ ] 컴파일 및 MessageDeduplicatorTest, PythiaApplicationTests PASS

### N8 - ThresholdEvaluator catch 분리
- [ ] evaluateOne 에 ViolationStateException 별도 catch 추가 (warn 레벨)
- [ ] 기타 RuntimeException catch 유지 (error 레벨)
- [ ] 신규 테스트 ThresholdEvaluatorExceptionHandlingTest 2건 추가
- [ ] 신규 테스트 PASS

### F4 - Actuator + Micrometer
- [ ] build.gradle 에 actuator + resilience4j-micrometer 의존성 추가
- [ ] 컴파일 + 컨텍스트 로딩 성공 시:
  - [ ] ResilienceConfig 에 TaggedRetryMetrics, TaggedCircuitBreakerMetrics Bean 추가
  - [ ] application.yml 에 management.endpoints.web.exposure.include: health,metrics 추가
  - [ ] 신규 MetricsExposureTest 작성 및 PASS
- [ ] 컴파일/로딩 실패 시:
  - [ ] 의존성 변경 롤백
  - [ ] docs/plans/fix-logs/025-boot4-resilience4j-micrometer-incompat.md 작성 (실패 로그, 결정 사유)

### 전체 회귀
- [ ] ./gradlew :pythia:build BUILD SUCCESSFUL
- [ ] 직전 작업 fix-log(023) 검증 명령 4개 PASS 유지
