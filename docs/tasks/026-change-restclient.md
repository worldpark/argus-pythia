# 026. Argus PrometheusClient의 WebClient를 RestClient로 전환

## Target
argus

## 1. 목표 (Goal)
- Argus의 Prometheus 외부 호출 클라이언트를 `WebClient.block()` 기반 구조에서 `RestClient` 기반 구조로 전환한다
- 가상 스레드 적용 이후에도 외부 HTTP 호출 모델을 명시적인 동기식 클라이언트로 단순화하여 코드 가독성과 유지보수성을 높인다
- 기존 Prometheus 조회 결과, 예외 처리, timeout, 동시성 제한 정책은 유지한 채 클라이언트 구현만 교체한다

## 2. 입력 (Input)
- 현재 Argus의 `PrometheusClient`는 `WebClient`를 사용하고 `.block()`으로 응답을 대기한다
- Prometheus 호출에는 base URL, connect timeout, response timeout 설정이 존재한다
- `PrometheusClient`에는 동시성 제한 안전장치가 이미 적용되어 있다
- `025` Task에서 가상 스레드 활성화 및 병렬 수집 경로가 적용되었거나 적용을 전제로 한다

## 3. 제약 (Constraints)
* 이번 Task는 Prometheus 외부 호출 클라이언트 전환에만 집중한다
* 기존 PromQL, DTO 구조, metric 조립 로직, Kafka 메시지 포맷은 변경하지 않는다
* Scheduler -> Service -> Client 계층 구조를 유지한다
* 기존 connect timeout, response timeout 수준은 유지하거나 동등하게 보장해야 한다
* 기존 동시성 제한기(`ConcurrencyLimiter`) 적용 위치와 동작 의미를 유지해야 한다
* 기존 예외 처리 의미를 유지해야 한다
  - HTTP 오류는 Prometheus 전용 예외로 변환
  - timeout 또는 네트워크 오류도 기존과 동일한 계열의 예외로 드러나야 한다
* `spring-boot-starter-webflux` 제거 여부는 실제 코드 의존성을 확인한 뒤 판단하되, 이번 Task 범위에서 제거가 가능하면 함께 정리할 수 있다

## 4. 작업 내용 (Implementation Scope)
- `WebClient` 기반 Prometheus 설정을 `RestClient` 기반 설정으로 전환한다
- Prometheus base URL, connect timeout, response timeout을 `RestClient`에서도 동일하게 적용한다
- `PrometheusClient` 구현을 `RestClient` 사용 방식으로 변경한다
- 기존 `.block()` 의존을 제거한다
- `ConcurrencyLimiter`가 RestClient 호출 경로에서도 동일하게 적용되도록 유지한다
- HTTP 상태 코드 오류, 직렬화 오류, timeout, 연결 실패에 대한 예외 변환 로직을 점검하고 필요 시 보완한다
- 기존 테스트를 `RestClient` 기준으로 갱신한다
  - 성공 응답 매핑
  - query parameter 인코딩
  - HTTP 5xx 오류
  - invalid JSON
  - 서버 미가용 또는 연결 실패
- `WebClient` 제거 후 불필요해진 설정 또는 의존성이 있다면 정리한다
  - 예: `WebClientConfig`
  - 예: `spring-boot-starter-webflux` 제거 가능 여부 검토

## 5. 성공 기준 (Acceptance Criteria)
- Argus의 Prometheus 호출이 `RestClient` 기반으로 동작한다
- 기존과 동일한 Prometheus 응답 매핑 결과를 유지한다
- 기존 timeout, base URL, query parameter 인코딩 동작이 유지된다
- 기존 동시성 제한 정책이 RestClient 호출 경로에서도 그대로 적용된다
- HTTP 오류, timeout, 연결 실패가 기존과 동일한 성격의 예외로 변환된다
- 관련 테스트가 통과하며, `PrometheusClient`의 정상/오류 시나리오가 검증된다
- `WebClient` 제거 후 불필요한 설정 또는 의존성이 정리된다면 그 변경이 함께 반영된다

## 6. 제외 범위 (Out of Scope)
* Prometheus 조회 병렬화 구조의 재설계
* 가상 스레드 정책 자체 변경
* Kafka publish 흐름 변경
* Redis 버퍼 정책 변경
* PromQL 변경 또는 메트릭 스키마 변경
