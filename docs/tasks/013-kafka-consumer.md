# 013. Pyshia Kafka Consumer 환경 구성

## Target
pyshia

---

## Goal
- pyshia 프로젝트에 Kafka Consumer 환경을 구성

---

## Context
- Kafka URL: http://localhost:9092

## Requirements
- `jvm.metrics.raw` topic은 `JvmMetricSnapshotDto` DTO로 response 받음
- `http.metrics.raw` topic은 `HttpMetricSnapshotDto` DTO로 response 받음
- `hikari.metrics.raw` topic은 `HikariMetricSnapshotDto` DTO로 response 받음

## Constraints
- spring-boot-starter-kafka 사용
- 별도의 consumer 패키지 생성해서 작성

## Acceptance Criteria
- Kafka의 `jvm.metrics.raw`, `http.metrics.raw`, `hikari.metrics.raw` topic에서 정상적으로 message를 consume

## Test
- Kafka consumer response 성공 테스트
- 빈 결과 처리 테스트