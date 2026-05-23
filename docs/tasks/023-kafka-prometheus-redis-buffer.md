# 023. Argus Prometheus/Kafka 완충용 Redis 버퍼 연동 추가

## Target
argus

## 1. 목표 (Goal)
- Argus의 Prometheus 메트릭 수집 및 Kafka publish 경로의 **fallback 경로**로 Redis 기반 완충 버퍼를 도입한다
- 정상 경로(Kafka 직접 publish)는 그대로 유지하여 장애 감지/알람 latency 를 보존한다
- Kafka publish 가 실패한 메트릭 스냅샷을 즉시 유실하지 않고 단기적으로 Redis 에 보존하여 후속 재시도로 배출한다
- 이후 가상 스레드 기반 병렬 수집이 도입되더라도 실패 케이스를 흡수할 수 있는 보조 버퍼 계층을 마련한다

## 2. 입력 (Input)
- Argus는 `MetricSnapshotScheduler`에서 JVM, HTTP, Hikari 메트릭 스냅샷을 수집한 뒤 Kafka로 publish 한다
- 현재 Kafka publish 실패 시 상위에서 durable 한 완충 저장소 없이 해당 주기 실패로 끝날 수 있다
- 현재 pythia 프로젝트에는 Redis 연동 경험과 설정 패턴이 존재하지만, Argus에는 Redis 버퍼 계층이 아직 없다
- 이후 Task에서 Prometheus 조회 병렬화 및 가상 스레드 도입이 검토되고 있다

## 3. 제약 (Constraints)
* 이번 Task는 Redis를 영구 저장소가 아니라 단기 완충 버퍼로 사용한다
* 기존 메트릭 계산 규칙, PromQL, DTO 필드 구조, Kafka 메시지 포맷은 변경하지 않는다
* Scheduler -> Service -> Messaging 계층 구조를 유지한다
* Redis 관련 설정은 외부 설정값으로 관리하며 하드코딩하지 않는다
* Redis 장애 시 동작 정책과 Redis 자체 데이터 유실 정책을 명시해야 한다
* 중복 전송 가능성 여부를 명확히 정의해야 하며, 필요 시 at-least-once를 기본 가정으로 둔다
* 무제한 적재는 허용하지 않으며 TTL 및 큐 길이 상한을 둔다

## 4. 작업 내용 (Implementation Scope)
- Argus에 Redis 연동 의존성과 기본 설정을 추가한다
- Kafka publish 가 실패한 메트릭 스냅샷을 Redis 버퍼에 적재할 수 있는 저장 구조를 정의한다 (Fallback-Only-On-Failure)
- JVM, HTTP, Hikari 스냅샷을 버퍼에 저장할 DTO 직렬화/역직렬화 방식을 정의한다
- Redis 버퍼 저장소를 담당하는 계층을 추가한다
- Redis 버퍼에서 Kafka로 재배출하는 서비스 또는 워커 흐름을 설계하고 구현한다
- 재배출 사이클에서 Kafka publish 성공 시 해당 버퍼 항목을 제거하는 상태 전이 규칙을 정의한다
- 재배출 사이클에서 Kafka publish 실패 시 버퍼 항목을 유지하여 후속 재시도가 가능하도록 한다
- Redis 버퍼 정책을 설정값으로 추가한다
  - TTL
  - 최대 적재 개수
  - overflow 정책
  - 배출 간격 또는 배출 트리거 방식
- Redis 자체 데이터 유실 정책을 문서화하고 구현에 반영한다
  - Redis 저장 실패 시 처리 정책
  - Redis TTL 만료 시 처리 정책
  - Redis 장애 또는 재시작 시 허용 가능한 유실 범위
  - 중복 배출 허용 여부와 downstream 가정
- Redis 버퍼 경로의 정상/실패 동작을 검증하는 테스트를 추가한다

## 5. 성공 기준 (Acceptance Criteria)
- 정상 경로(Kafka 직접 publish)에 추가 latency 가 발생하지 않고 정상 ack 시 Redis 호출이 없다
- Argus에서 Kafka publish 실패 시 메트릭 스냅샷을 Redis 버퍼에 저장할 수 있다
- Kafka 일시 실패 상황에서 Redis 버퍼에 남은 스냅샷을 후속 재배출 사이클에서 다시 배출할 수 있다
- Redis 버퍼는 TTL 및 최대 적재 개수 제한을 가진다
- Redis 장애, 저장 실패, TTL 만료에 대한 정책이 문서와 코드에 반영된다
- 재배출 사이클의 Kafka publish 성공 시 버퍼 항목이 정리된다
- 관련 테스트를 통해 정상 ack 무적재, fallback 적재, 재배출, 실패 보존, 정책 기반 삭제 또는 거부 시나리오가 검증된다

## 6. 제외 범위 (Out of Scope)
* 가상 스레드 활성화 자체 적용
* Prometheus 조회 병렬화 자체 구현
* `WebClient`를 `RestClient`로 교체하는 리팩터링
* Kafka broker, Kafka Connect, Redis HA/Cluster/Sentinel 인프라 구성
* 장기 영속 저장소 수준의 무손실 보장
