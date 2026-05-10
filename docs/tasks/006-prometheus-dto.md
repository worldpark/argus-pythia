# Task: 006. Prometheus 결과 DTO 설계 및 Snapshot 구성

## 1. 목표 (Goal)

* Prometheus PromQL 응답(`PrometheusResponse`)을 서비스 내부 DTO로 변환한다.
* JVM 메트릭을 하나의 스냅샷(`JvmMetricSnapshotDto`)으로 구성한다.
* 일부 메트릭 수집 실패 상황에서도 안정적으로 DTO를 생성한다.
* 최종 결과를 Kafka로 전송 가능한 구조로 설계한다.

---

## 2. 현재 상태 (Current State)

* Prometheus 서버: `localhost:9090`
* Spring Boot 애플리케이션: Actuator + Micrometer 메트릭 노출
* `PrometheusClient`를 통해 PromQL 실행 가능
* `PrometheusResponse` DTO 존재 (수정 금지)

---

## 3. 입력 (Input)

* `MetricType` (CPU, Memory, GC, Thread 등)
* `PrometheusResponse`

  * `data.result[].metric` (labels)
  * `data.result[].value` ([timestamp, value])

---

## 4. 출력 (Output)

### 4.1 공통 DTO

* `MetricPointDto`

  * application
  * instance
  * labels (Map<String, String>)
  * timestamp (Instant)
  * value (BigDecimal)

---

### 4.2 하위 메트릭 DTO

#### CpuUsageDto

* usagePercent
* measuredAt
* status
* missingReason

#### MemoryUsageDto

* heapUsagePercent
* measuredAt
* status
* missingReason

#### GcMetricDto

* avgDurationSeconds
* count
* measuredAt
* status
* missingReason

#### ThreadMetricDto

* activeCount
* measuredAt
* status
* missingReason

---

### 4.3 스냅샷 DTO

#### JvmMetricSnapshotDto

* application
* instance
* collectedAt
* cpu (CpuUsageDto)
* memory (MemoryUsageDto)
* gc (GcMetricDto)
* thread (ThreadMetricDto)
* status (SnapshotStatus)

---

### 4.4 상태 정의

#### MetricStatus

* SUCCESS
* EMPTY_RESULT
* QUERY_FAILED
* PARSE_FAILED

#### SnapshotStatus

* COMPLETE
* PARTIAL
* FAILED

---

## 5. 데이터 흐름 (Flow)

1. MetricType 기반 PromQL 실행
2. PrometheusResponse 수신
3. PrometheusResponse → MetricPointDto 변환
4. MetricPointDto → 하위 DTO 변환
5. 하위 DTO → JvmMetricSnapshotDto 조립
6. SnapshotStatus 계산
7. Kafka 전송

---

## 6. 매핑 규칙 (Mapping Rules)

### 6.1 Label → 필드 매핑

| Label Key   | DTO 필드          |
| ----------- | --------------- |
| application | application     |
| instance    | instance        |
| uri         | endpoint (HTTP) |
| method      | method          |
| status      | httpStatus      |

---

### 6.2 Value 변환

| Prometheus         | Java       |
| ------------------ | ---------- |
| timestamp (double) | Instant    |
| value (string)     | BigDecimal |

---

### 6.3 쿼리 기준

* Instant query 기준 (`/api/v1/query`)
* range query 사용 금지

---

## 7. 부분 실패 처리 (중요)

* 일부 메트릭 실패 시에도 Snapshot 생성
* 각 DTO는 개별 상태를 가짐
* Snapshot 전체 상태 정의:

| 조건        | 상태       |
| --------- | -------- |
| 모든 메트릭 성공 | COMPLETE |
| 일부 실패     | PARTIAL  |
| 전부 실패     | FAILED   |

---

## 8. Kafka 전송 정책

* COMPLETE → 전송
* PARTIAL → 전송
* FAILED → 전송 여부 선택 (기본: 전송 안 함)

---

## 9. 제약 (Constraints)

* `PrometheusResponse` 수정 금지
* PromQL 수정 금지
* Controller/API 구현 제외
* DB 저장 제외
* LLM 분석 제외
* DTO 설계 및 변환 구조만 정의

---

## 10. 성공 기준 (Acceptance Criteria)

* 모든 메트릭이 DTO로 변환 가능
* 부분 실패 상황에서도 Snapshot 생성 가능
* MetricStatus / SnapshotStatus 정상 동작
* Kafka 전송 가능한 구조 정의 완료
* 이후 구현 Task에서 바로 사용 가능

---

## 11. 제외 범위 (Out of Scope)

* Grafana Dashboard
* Prometheus 설정 변경
* Micrometer metric name 변경
* Alert/알람 시스템
* 데이터 분석/집계
* 멀티 노드 Aggregation
* range query 처리

---
