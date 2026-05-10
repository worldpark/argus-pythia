# Task: 011. HTTP, Hikari DTO 작성

## 1. 목표 (Goal)
- `argus` 프로젝트의 `com.example.argus.service.metric.MetricType` 의 HTTP_P99_RESPONSE_TIME, HTTP_RPS, HIKARI_ACTIVE_CONNECTIONS, HIKARI_PENDING_CONNECTIONS 의 PromQL 결과 DTO를 작성한다.
- DTO 구조는 `com.example.argus.dto.metric.JvmMetricSnapshotDto` 를 참고하여 유사하게 작성하라

## 2. 제약 (Constraints)
* `PrometheusResponse` 수정 금지
* PromQL 수정 금지
* Controller/API 구현 제외
* DB 저장 제외
* LLM 분석 제외
* DTO 설계 및 변환 구조만 정의

## 3. 성공 기준 (Acceptance Criteria)
* 모든 메트릭이 DTO로 변환 가능
* 부분 실패 상황에서도 Snapshot 생성 가능
* Kafka 전송 가능한 구조 정의 완료
* 이후 구현 Task에서 바로 사용 가능

## 5. 제외 범위 (Out of Scope)
* Grafana Dashboard
* Prometheus 설정 변경
* Micrometer metric name 변경
* Alert/알람 시스템
* 데이터 분석/집계
* 멀티 노드 Aggregation
* range query 처리