# 016. Pythia 임계값 설정 및 임계값 도달시 이메일 알림 기능 구현

## Target
pythia

## Goal
- pythia 프로젝트에 Kafka `jvm.metrics.raw`, `http.metrics.raw`, `hikari.metrics.raw` topic으로 부터 받은 메시지 내용에 적용할 임계값 설정
- 임계값 도달시 설정된 이메일로 알림 기능 구현

## Requirements
- `docs/metrics/promqls.md` 파일을 참고 하여 작성

## Acceptance Criteria
- 임계값은 application.yml의 설정값으로 설정됨
- 이메일 알림은 `com.example.pythia.email.EmailService` 을 사용

## Test
- 임계치 도달 성공 테스트
- 이메일 알림 성공 테스트