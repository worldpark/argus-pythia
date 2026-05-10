# Task: 010. Grafana docker compose 작성

## Target
root

## Goal
현 Prometheus 와 연동되는 grafana docker-compose.yml 작성

## Requirements
- docker/prometheus/docker-compose.yml 에 서비스를 추가하여 작성
- 로컬 개발 환경 기준으로 구성
- 포트포워딩은 3100:3000 으로 설정

## Acceptance Criteria
- docker compose up -d 실행 시 Kafka가 정상 실행된다