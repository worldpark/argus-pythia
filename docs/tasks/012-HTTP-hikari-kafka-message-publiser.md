# Task: 012. HTTP, Hikari 전용 Kafka Message Publiser 작성

## 1. 목표 (Goal)
- `com.example.argus.service.metric.snapshot.JvmMetricSnapshotPublisher` 와 유사한 HTTP, Hikari Publisher 작성

## 2. 입력 (Input)
- 메시지의 data는 `HttpMetricSnapshotDto`, `HikariMetricSnapshotDto` 이며, topic 명은 각각 `http.metrics.raw`, `hikari.metrics.raw` 이다.
- 
## 3. 제약 (Constraints)
* PromQL 수정 금지
* Controller/API 구현 제외
* DB 저장 제외
* LLM 분석 제외
* DTO 설계 및 변환 구조만 정의

## 4. 성공 기준 (Acceptance Criteria)
- 이후 구현 Task에서 바로 사용 가능
- kafka로 정상적으로 message가 producing 됨

## 5. 제외 범위 (Out of Scope)
* Grafana Dashboard
* Prometheus 설정 변경
* Micrometer metric name 변경
* Alert/알람 시스템
* 데이터 분석/집계
* 멀티 노드 Aggregation
* range query 처리