# 025. Argus 가상 스레드 활성화 및 병렬 수집 경로 적용

## Target
argus

## 1. 목표 (Goal)
- Argus에 Java 21 가상 스레드를 활성화하여 Prometheus 메트릭 수집의 동시 처리 효율을 높인다
- 기존 `WebClient.block()` 기반 호출 구조를 유지하면서도, 메트릭 조회 병렬화 시 플랫폼 스레드 고갈 위험을 줄인다
- 앞선 안전장치 Task에서 정의한 동시성 제한 및 재시도 정책을 유지한 상태로 가상 스레드 기반 실행 경로를 적용한다

## 2. 입력 (Input)
- Argus는 Spring Boot 4.0.6, Java 21 환경에서 동작한다
- `PrometheusClient`는 `WebClient`를 사용하되 `.block()`으로 동기 호출한다
- `MetricSnapshotScheduler`는 JVM, HTTP, Hikari 스냅샷 수집 및 Kafka publish를 주기적으로 실행한다
- `024` Task에서 Prometheus/Kafka 경로의 동시성 제한 및 짧은 재시도 안전장치가 준비되어 있다
- `023` Task에서 Redis 버퍼 연동이 정의되어 있거나 후속으로 연계될 수 있다

## 3. 제약 (Constraints)
* 이번 Task는 `WebClient`를 `RestClient`로 교체하지 않는다
* 기존 PromQL, DTO 구조, Kafka 메시지 포맷, 스케줄 주기는 변경하지 않는다
* Scheduler -> Service -> Client/Messaging 계층 구조를 유지한다
* 가상 스레드 적용은 설정 추가만이 아니라 실제 병렬 실행 경로에 반영되어야 한다
* 가상 스레드 적용 후에도 기존 동시성 제한, timeout, 재시도 정책은 그대로 유효해야 한다
* `spring.main.keep-alive` 등 가상 스레드 환경에서 스케줄러가 안전하게 유지되도록 필요한 설정을 포함한다
* pinned virtual thread를 유발할 수 있는 장시간 synchronized/lock 보유 구조를 새로 도입하지 않는다
* 성능 개선이 없거나 역효과가 있는 경우 원인 분석이 가능하도록 관측 가능한 로그 또는 측정 근거를 남길 수 있어야 한다

## 4. 작업 내용 (Implementation Scope)
- Argus 설정에 가상 스레드 활성화 설정을 추가한다
  - `spring.threads.virtual.enabled`
  - 필요 시 `spring.main.keep-alive`
- 가상 스레드 환경에서 사용하는 실행 정책을 점검하고 필요한 executor 또는 실행 전략을 정의한다
- 메트릭 수집 병렬화 적용 범위를 결정하고 구현한다
  - JVM 메트릭 내부 조회 병렬화
  - HTTP 메트릭 내부 조회 병렬화
  - Hikari 메트릭 내부 조회 병렬화
  - 필요 시 스냅샷 유형 간 병렬화 여부도 명시
- 병렬 실행 구현은 가상 스레드와 호환되는 방식으로 작성한다
  - 예: `CompletableFuture` + virtual-thread executor
  - 또는 동등한 구조
- 기존 Prometheus 조회 동시성 제한 및 Kafka publish 제한이 병렬 실행 환경에서도 일관되게 적용되도록 보장한다
- 스케줄러 실행 흐름에서 가상 스레드 활성화 후 종료/유휴 상태 문제가 없도록 검증한다
- 가상 스레드 적용 후 성능 비교 또는 회귀 검증에 필요한 테스트 또는 측정 절차를 추가한다
  - 기존 대비 한 주기 처리 시간 비교
  - 병렬 호출 시 실패율 및 timeout 변화 확인
  - 동시 호출 수 증가 시 안정성 확인
- 가상 스레드 적용과 관련된 운영 주의사항을 문서 또는 설정 근거로 남긴다

## 5. 성공 기준 (Acceptance Criteria)
- Argus에서 가상 스레드가 활성화된다
- 메트릭 수집 병렬 실행 경로가 가상 스레드 기반으로 동작한다
- 기존 `WebClient.block()` 호출 구조를 유지하면서도 병렬 수집 시 플랫폼 스레드 고갈 없이 동작한다
- `024`에서 정의한 동시성 제한 및 재시도 정책이 가상 스레드 적용 후에도 정상 동작한다
- 스케줄러는 가상 스레드 환경에서도 안정적으로 계속 동작하며, 애플리케이션이 조기 종료되지 않는다
- 기존 테스트가 깨지지 않고, 가상 스레드 적용 경로를 검증하는 테스트 또는 성능 비교 근거가 추가된다

## 6. 제외 범위 (Out of Scope)
* `WebClient`를 `RestClient`로 교체하는 리팩터링
* Reactor 기반 전면 재작성 (`Mono.zip` 등)
* Kafka broker, Prometheus 서버, Redis 인프라 설정 변경
* Kafka 메시지 스키마 변경
* Redis 버퍼 정책 자체의 재설계
