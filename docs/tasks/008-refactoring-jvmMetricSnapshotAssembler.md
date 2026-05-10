# Task: 008. JvmMetricSnapshotAssembler refactoring

## 1. 목표 (Goal)
- `JvmMetricSnapshotAssembler` 의 내용을 좀더 간결하게 리팩토링 한다.
- `CpuUsageDto`, `MemoryUsageDto`, `ThreadMetricDto`, `GcMetricDto`에 같은 부모 DTO를 생성한다.
- 팩토리 메서드 패턴을 사용한다.

## 2. 제약 (Constraints)
* `PrometheusResponse` 수정 금지
* PromQL 수정 금지

## 3. 성공 기준 (Acceptance Criteria)
- 이후 구현 Task에서 바로 사용 가능
- kafka로 정상적으로 message가 producing 됨

## 4. 제외 범위 (Out of Scope)
* Grafana Dashboard
* Prometheus 설정 변경
* Micrometer metric name 변경
* Alert/알람 시스템
* 데이터 분석/집계
* 멀티 노드 Aggregation
* range query 처리