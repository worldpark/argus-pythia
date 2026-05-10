# Task: 009. Schedule 작성

## 1. 목표 (Goal)
- Spring Schedule이 적용된 통합된 함수 작성
- 함수 실행 주기는 60초, 함수는 `JvmMetricSnapshotPublisher.publish()` 를 호출해야함

## 2. 제약 (Constraints)
- Schedule이 적용된곳은 한곳임

## 3. 성공 기준 (Acceptance Criteria)
- 60초 마다 JvmMetricSnapshotPublisher.publish() 함수가 실행됨
- Promtheus 로 promQL 을 보낸 뒤 결과를 받아 Kafka `jvm.metrics.raw` topic 으로 메시지를 생성해야함

## 4. 제외 범위 (Out of Scope)
* Grafana Dashboard
* Prometheus 설정 변경
* Micrometer metric name 변경
* Alert/알람 시스템
* 데이터 분석/집계
* 멀티 노드 Aggregation
* range query 처리