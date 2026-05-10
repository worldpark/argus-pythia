# 017. 메트릭 값 RDB에 저장

## Target
pyshia

## 1. 목표 (Goal)
- pyshia 프로젝트에 Kafka `jvm.metrics.raw`, `http.metrics.raw`, `hikari.metrics.raw` topic으로 부터 받은 메시지 내용을 RDB에 저장
- RDB는 PostgreSQL 사용

## 2. 입력 (Input)
- 메시지의 data는 `com.example.pyshia.kafka.dto.jvm.JvmMetricSnapshotDto`, `com.example.pyshia.kafka.dto.http.HttpMetricSnapshotDto`, `com.example.pyshia.kafka.dto.hikari.HikariMetricSnapshotDto` 이며, topic 명은 각각 `jvm.metrics.raw` `http.metrics.raw`, `hikari.metrics.raw` 이다.
  
## 3. 제약 (Constraints)
* Controller/API 구현 제외
* LLM 분석 제외

## 4. 성공 기준 (Acceptance Criteria)
- 이후 구현 Task에서 바로 사용 가능
- RDB에 정상적으로 테이블 생성

## 5. 제외 범위 (Out of Scope)
* Micrometer metric name 변경
* Alert/알람 시스템
* 데이터 분석/집계
* 멀티 노드 Aggregation
* range query 처리