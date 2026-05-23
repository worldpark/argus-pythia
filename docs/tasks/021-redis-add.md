# 021. pythia 프로젝트에 Redis 모듈을 추가한다

## Target
pythia

## 1. 목표 (Goal)
- pythia 프로젝트에서 Redis를 사용할 수 있도록 애플리케이션 모듈과 실행 환경을 추가한다
- Spring Boot 기반 Redis 연동 설정을 추가하고, 이후 Task에서 바로 사용할 수 있는 기반을 마련한다
- 로컬 개발 환경에서 Redis를 Docker Compose로 기동할 수 있도록 구성한다

## 2. 입력 (Input)
- 기존 pythia 프로젝트는 PostgreSQL, Kafka, Spring AI 기반으로 동작한다
- Redis는 아직 프로젝트에 연동되어 있지 않다
- Docker 관련 파일은 반드시 `/docker` 하위에 생성해야 한다

## 3. 제약 (Constraints)
* 이번 Task에서는 Redis 연동 기반만 추가한다
* Controller/API는 구현하지 않는다
* Redis를 사용하는 비즈니스 기능은 구현하지 않는다
* Redis 설정은 pythia 애플리케이션 기준으로 추가한다
* Redis 실행용 compose 파일은 루트가 아닌 `/docker` 하위에 둔다

## 4. 작업 내용 (Implementation Scope)
- `pythia/build.gradle`에 Redis 연동에 필요한 Spring Boot 의존성을 추가한다
- `pythia/src/main/resources/application.yml`에 Redis 연결 설정 항목을 추가한다
- Redis 연결에 사용하는 설정값은 환경변수 또는 profile 설정으로 분리 가능하게 구성한다
- Redis 연결 확인 또는 최소 동작 검증이 가능한 구성 클래스를 추가한다
- 로컬 실행을 위한 Redis Docker Compose 파일을 `/docker` 하위에 추가한다
- Redis 추가 후 애플리케이션이 기존 PostgreSQL, Kafka 설정과 충돌하지 않도록 유지한다

## 5. 성공 기준 (Acceptance Criteria)
- pythia 프로젝트가 Redis 의존성을 포함한 상태로 빌드된다
- 로컬에서 Docker Compose로 Redis를 기동할 수 있다
- pythia 애플리케이션에 Redis host, port 등 기본 연결 설정이 존재한다
- 이후 Task에서 RedisTemplate, Cache, Pub/Sub 등 Redis 기능을 바로 확장할 수 있는 상태다
- Redis 추가로 기존 테스트와 기본 애플리케이션 기동이 깨지지 않는다

## 6. 제외 범위 (Out of Scope)
* Redis 기반 캐시 비즈니스 로직 구현
* Redis Pub/Sub, Stream, Distributed Lock 구현
* 세션 저장소 전환
* PostgreSQL 데이터를 Redis로 이관하는 작업
* 운영 환경 배포 파이프라인 변경
