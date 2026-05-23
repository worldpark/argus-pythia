# 023 Spring Boot 4 Resilience4j ApplicationContext 시작 오류 수정

> 작성일: 2026-05-23
> 관련 증상: `ViolationStateStore` 생성자 해석 실패로 인한 `ApplicationContext` 시작 실패
> 관련 모듈: `pythia`

## 배경
애플리케이션 시작 시 다음 흐름으로 `ApplicationContext` 생성이 실패했다.

- `ThresholdEvaluator` Bean 생성 중 생성자 파라미터인 `ViolationStateStore` 주입 필요
- `ViolationStateStore` Bean 생성 중 declared constructors 해석 실패
- 결과적으로 `UnsatisfiedDependencyException` 및 `BeanCreationException` 발생

로그상 직접 실패 지점은 `ViolationStateStore`였지만, 클래스의 생성자 파라미터 자체는 다음과 같이 정상적인 Spring Bean 의존성 구조였다.

- `StringRedisTemplate`
- `RedissonClient`
- `ViolationStateProperties`
- `RetryRegistry`

따라서 단순한 Bean 누락보다, 생성자 파라미터 타입 또는 자동 설정 클래스패스 해석 과정에서 런타임 호환성 문제가 발생했을 가능성이 높다고 판단했다.

## 원인 판단
프로젝트는 Spring Boot `4.0.6`을 사용하고 있었고, Resilience4j 의존성은 `resilience4j-spring-boot3:2.2.0` starter를 사용하고 있었다.

Spring Boot 4 환경에서 Boot 3 전용 starter 자동 설정을 그대로 로딩하면 Spring Framework / Spring Boot 자동 설정 API 변경으로 인해 Bean 생성 전 단계에서 클래스 해석 실패가 발생할 수 있다. 이번 케이스에서는 `RetryRegistry`를 필요로 하는 `ViolationStateStore` 생성 시점에 문제가 드러났지만, 실제 위험 범위는 다음 사용처 전체에 걸쳐 있었다.

- `ViolationStateStore`: `RetryRegistry` Functional API 사용
- `EmailService`: `@Retry(name = "emailSender")` 사용
- `MetricAnalysisService`: `@Retry`, `@CircuitBreaker` 사용

## 결정
Spring Boot 4에서 호환성이 불확실한 `resilience4j-spring-boot3` starter를 제거하고, 필요한 core 모듈만 명시적으로 사용하도록 전환했다.

- 제거: `io.github.resilience4j:resilience4j-spring-boot3:2.2.0`
- 추가: `io.github.resilience4j:resilience4j-retry:2.2.0`
- 추가: `io.github.resilience4j:resilience4j-circuitbreaker:2.2.0`

starter 자동 설정에 의존하지 않도록 `RetryRegistry`와 `CircuitBreakerRegistry`를 프로젝트 설정 클래스로 직접 등록했다. 기존 `application.yml`의 `resilience4j.retry.instances.*`, `resilience4j.circuitbreaker.instances.*` 설정 구조는 유지했다.

## 변경 사항
### Resilience 설정
신규 설정 클래스를 추가했다.

- `pythia/src/main/java/com/example/pythia/resilience/config/RetryProperties.java`
- `pythia/src/main/java/com/example/pythia/resilience/config/CircuitBreakerProperties.java`
- `pythia/src/main/java/com/example/pythia/resilience/config/ResilienceConfig.java`

`ResilienceConfig`는 YAML 설정을 읽어 다음 Bean을 명시적으로 등록한다.

- `RetryRegistry`
- `CircuitBreakerRegistry`

지원하는 설정 항목은 현재 프로젝트에서 사용 중인 범위로 제한했다.

- Retry: `max-attempts`, `wait-duration`, `enable-exponential-backoff`, `exponential-backoff-multiplier`, `retry-exceptions`, `ignore-exceptions`
- CircuitBreaker: `failure-rate-threshold`, `wait-duration-in-open-state`, `sliding-window-size`, `sliding-window-type`, `minimum-number-of-calls`, `permitted-number-of-calls-in-half-open-state`, `automatic-transition-from-open-to-half-open-enabled`, `ignore-exceptions`

### EmailService
`@Retry` AOP 의존을 제거하고, `RetryRegistry`를 주입받아 `sendToOperator`에서 Functional API 방식으로 재시도하도록 변경했다.

변경 후 흐름:

1. `sendToOperator` 호출
2. `RetryRegistry.retry("emailSender")` 조회
3. `Retry.decorateRunnable(...)`로 `send(...)` 실행
4. `MailException` 발생 시 YAML 설정에 따라 재시도

### MetricAnalysisService
`@Retry`, `@CircuitBreaker` AOP 의존을 제거하고, `RetryRegistry`와 `CircuitBreakerRegistry`를 직접 사용하도록 변경했다.

변경 후 흐름:

1. `RetryRegistry.retry("llmAnalysis")` 조회
2. `CircuitBreakerRegistry.circuitBreaker("llmAnalysis")` 조회
3. Retry 바깥, CircuitBreaker 안쪽 구조로 LLM 호출 실행
4. `TransientAiException`은 재시도 대상
5. `AiAnalysisException`은 ignore 대상
6. 응답 검증 및 제어문자 제거 로직은 유지

### ViolationStateStore
기존처럼 `RetryRegistry` Functional API를 사용한다. 이번 수정으로 `RetryRegistry` Bean을 직접 등록하므로 `ViolationStateStore` 생성자 해석 단계에서 Boot 3 starter 자동 설정에 의존하지 않는다.

## 검증
다음 명령으로 타깃 테스트와 전체 테스트를 실행했다.

```powershell
.\gradlew.bat test --tests com.example.pythia.alert.state.ViolationStateStoreTest --tests com.example.pythia.email.EmailServiceRetryTest --tests com.example.pythia.ai.service.MetricAnalysisServiceTest --tests com.example.pythia.ai.service.MetricAnalysisServiceResponseValidationTest
```

결과:

- `BUILD SUCCESSFUL`

전체 테스트도 실행했다.

```powershell
.\gradlew.bat test
```

결과:

- `BUILD SUCCESSFUL`

## 주의 사항
이번 변경은 Spring Boot 4에서 Resilience4j Boot 3 starter 자동 설정을 회피하기 위한 조치다. 따라서 Resilience4j Actuator metric 자동 노출 등 starter가 제공하던 부가 기능은 현재 범위에서 직접 구성하지 않았다.

운영 관측 지표가 필요하면 별도 작업으로 Micrometer 연동 범위를 정의해야 한다.

## 유지된 사항
- `application.yml`의 Resilience4j 설정 네이밍은 유지했다.
- `ViolationStateStore`의 public API는 유지했다.
- `EmailService.sendToOperator`의 호출부 계약은 유지했다.
- `MetricAnalysisService.analyze`의 호출부 계약은 유지했다.
- `LockAcquisitionRetryException` 기반 lock retry 정책은 유지했다.

