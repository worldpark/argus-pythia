# 002. Docker Compose Kafka 환경 구성

## Target
root

## Goal
Argus와 Pythia가 Kafka를 사용해 통신할 수 있도록 로컬 개발용 Kafka 환경을 Docker Compose로 구성한다.

## Requirements
- docker-compose.yml 작성
- Kafka 브로커 실행 가능
- Topic 생성 또는 자동 생성 설정
- Argus에서 접근 가능한 bootstrap server 제공
- 로컬 개발 환경 기준으로 구성

## Kafka Topic
- jvm.metrics.raw
- jvm.alert

## Acceptance Criteria
- docker compose up -d 실행 시 Kafka가 정상 실행된다
- Kafka bootstrap server로 localhost:9092 접근 가능하다
- jvm.metrics.raw 토픽을 생성할 수 있다