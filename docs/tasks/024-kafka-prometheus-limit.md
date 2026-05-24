# 024. 가상 스레드 도입 전 Argus Prometheus/Kafka 부하 제한 안전장치 추가

## Target
argus

## 1. 목표 (Goal)
- Argus에 가상 스레드를 도입하기 전에 Prometheus 조회와 Kafka 전송에 대한 동시 실행 상한을 두는 안전장치를 추가한다
- 메트릭 스냅샷 수집 작업이 병렬화되더라도 Prometheus 서버와 Kafka 전송 경로에 과도한 순간 부하를 주지 않도록 제어한다
- 이후 Task에서 가상 스레드 기반 병렬 조회를 도입하더라도 운영 리스크를 낮출 수 있는 최소 보호장치를 먼저 마련한다

## 2. 입력 (Input)
- Argus는 `MetricSnapshotScheduler`에서 주기적으로 JVM, HTTP, Hikari 메트릭 스냅샷 수집 및 Kafka publish를 수행한다
- 현재 `PrometheusClient`는 `WebClient`를 사용하지만 `.block()`으로 동기 호출하고 있다
- 현재 assembler 내부 Prometheus 조회는 대부분 순차 실행이며, 이후 가상 스레드 기반 병렬화가 검토되고 있다
- Kafka publish는 `CompletableFuture` 기반으로 비동기 전송되지만 상위에서 동시성 제한이나 backpressure 제어는 없다

## 3. 제약 (Constraints)
* 이번 Task는 가상 스레드 자체를 도입하지 않는다
* 이번 Task는 Prometheus 조회 및 Kafka publish의 동시성/진입량 제한 안전장치 추가에만 집중한다
* 기존 메트릭 산출 규칙, PromQL, DTO 구조, Kafka 메시지 포맷은 변경하지 않는다
* Scheduler -> Service -> Client/Messaging 계층 구조를 유지한다
* 제한값은 코드 상수 하드코딩 대신 설정값으로 외부화하여 환경별 조정이 가능해야 한다
* 제한 초과 시 무한 대기하지 않도록 timeout 또는 제한된 재시도 정책을 명확히 정의한다
* 실패는 조용히 무시하지 않고 기존 예외 처리/로그 정책에 맞게 드러나야 한다
* 1차 단계에서는 짧은 대기 후 재시도를 적용하되, 별도 durable 버퍼는 두지 않는다

## 4. 작업 내용 (Implementation Scope)
- Argus 설정에 Prometheus 조회 동시 실행 제한값과 Kafka publish 동시 실행 제한값을 추가한다
- 제한값을 바인딩할 `ConfigurationProperties` 또는 동등한 설정 객체를 추가한다
- Prometheus 조회 경로에 대해 동시 실행 개수를 제한하는 안전장치를 도입한다
- Kafka publish 경로에 대해 동시 실행 개수를 제한하는 안전장치를 도입한다
- 제한 초과 시 동작 정책을 정의한다
  - 짧은 대기 후 permit 또는 실행 기회 재시도
  - 제한 횟수 또는 제한 시간 내 재시도 실패 시 명시적 실패 처리
  - 실패 시 로그와 운영 추적이 가능하도록 정보 기록
- `MetricSnapshotScheduler` 단일 실행 주기 안에서 JVM/HTTP/Hikari 작업이 동시에 몰릴 때도 제한이 일관되게 적용되도록 한다
- 향후 가상 스레드 병렬화가 들어와도 동일한 제한 장치를 재사용할 수 있도록 구현 위치와 책임을 설계한다
- 제한 동작을 검증하는 테스트를 추가한다
  - permit 획득 성공 시 정상 수행
  - permit 획득 실패 또는 timeout 후 재시도 시나리오
  - 재시도 소진 후 예외/실패 처리
  - release 누락 없이 정리되는지 검증
  - 기존 정상 publish/query 시나리오가 유지되는지 검증

## 5. 성공 기준 (Acceptance Criteria)
- Argus에 Prometheus 조회 동시성 제한과 Kafka publish 동시성 제한 설정이 추가된다
- 제한값 내에서는 기존 메트릭 수집 및 publish 흐름이 정상 동작한다
- 제한 초과 상황에서 무한 대기, 스레드 누수, permit 누수 없이 제한된 재시도 후 명시적인 실패가 발생한다
- Scheduler 한 주기 동안 여러 메트릭 수집 작업이 겹쳐도 정의된 상한을 넘지 않는다
- 제한 장치 추가 후에도 기존 메트릭 DTO 조립 및 Kafka 메시지 포맷은 변경되지 않는다
- 관련 단위 테스트 또는 서비스 호출 검증 테스트가 추가되어 제한 장치의 정상/실패 경로를 확인할 수 있다

## 6. 제외 범위 (Out of Scope)
* 가상 스레드 활성화 (`spring.threads.virtual.enabled`) 자체 적용
* `WebClient`를 `RestClient`로 교체하는 리팩터링
* Prometheus 조회 로직의 Reactor 기반 전면 재작성 (`Mono.zip` 등)
* 메트릭 종류 추가, PromQL 변경, DTO 필드 변경
* Kafka broker, Kafka Connect, Prometheus 서버의 인프라 설정 변경
* Redis 기반 durable 버퍼 추가
