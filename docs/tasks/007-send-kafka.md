# Task: 007. Prometheus PromQL의 결과 데이터를 Kafka 메시지로 producer

## 1. 목표 (Goal)
- PromQL 결과를 kafka로 메시지를 producing

## 2. 입력 (Input)
- 메시지의 data는 `JvmMetricSnapshotDto` 이며, topic 명은 `jvm.metrics.raw` 이다.


## 3. 제약 (Constraints)
- 메시지 발행시 `MetricEventProducer` 의 send 함수를 사용하라.
- 필요하다면 `MetricEventProducer` 의 send 함수의 내용을 수정하라
- `MetricEventProducer`의 send 함수 수정시 연관된 test 함수들도 수정해야한다.
* `PrometheusResponse` 수정 금지
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