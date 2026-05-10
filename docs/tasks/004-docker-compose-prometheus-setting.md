# 004. Docker Compose Prometheus 환경 구성

## Target
root

## Goal
Argus가 Prometheus HTTP API와 통신할 수 있도록 로컬 개발용 Prometheus 환경을 Docker Compose로 구성한다.

## Requirements
- Prometheus 컨테이너를 Docker Compose로 구성한다
- Prometheus는 localhost:9090으로 접근 가능해야 한다
- Prometheus 설정 파일과 scrape target 파일을 분리한다
- 로컬 개발 환경 기준으로 구성한다
- Prometheus는 file_sd_configs를 사용하여 scrape target을 동적으로 관리한다
- targets.yml 변경 시 별도 재시작 없이 대상이 갱신되어야 한다

## Files
- docker/prometheus/docker-compose.yml
- docker/prometheus/prometheus.yml
- docker/prometheus/targets.yml

## Constraints
- Prometheus 설정은 컨테이너 내부에 마운트한다
- 설정 파일은 docker/prometheus 경로에 둔다
- Target Service는 file_sd_configs 방식으로 분리 관리한다
- Kafka 설정과 섞지 않는다

## Acceptance Criteria
- `docker compose -f docker/prometheus/docker-compose.yml up -d` 실행 시 Prometheus가 정상 실행된다
- `http://localhost:9090` 접근 가능하다
- Prometheus UI에서 target 상태를 확인할 수 있다
- `targets.yml`에 추가한 Spring Boot 서비스가 scrape target으로 인식된다