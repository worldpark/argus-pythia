# 프로젝트 컨텍스트

## 기술 스택
- Backend: Java 21, Spring Boot 4.0.6, Spring Data JPA, Spring Security, Spring AI 2.0.0-M6
- DB: PostgreSQL (개발: H2)
- 빌드: Gradle
- 테스트: JUnit 5 + Mockito (백엔드)

## 아키텍처 규칙
- REST 진입점은 `Controller -> ServiceResponse -> Service -> Repository` 레이어를 따른다
- 이벤트 진입점은 `Consumer/Handler -> Service -> Repository` 레이어를 따른다
- 스케줄러 진입점은 `Scheduler -> Service -> Repository` 레이어를 따른다
- `ServiceResponse`는 REST 응답 조합 전용이며 Kafka Consumer, Scheduler에서는 사용하지 않는다
- DTO/Entity 분리 필수
- REST API는 `/api/v1/**` 패턴을 사용한다
- 모든 예외는 `RuntimeException` 상속 `CustomException` 또는 도메인별 커스텀 예외로 변환한다
- 에러 응답은 REST API에서만 공통 포맷을 사용한다
- 외부 API 오류는 별도 Exception으로 변환한다

## Docker 파일 규칙
- 모든 docker 관련 파일은 `/docker` 경로에 생성한다
- `docker-compose.yml`, `docker-compose.*.yml` 등은 루트에 생성하지 않는다
- 환경별 compose 파일은 `/docker` 하위에서 분리한다

## 코딩 컨벤션
- 백엔드: Google Java Style Guide
- 커밋: `feat/`, `fix/`, `test/` 접두어 사용

## 테스트 규칙
- 서비스 레이어: 단위 테스트 필수 (Mockito)
- REST 컨트롤러가 있을 경우: MockMvc 통합 테스트 작성
- Kafka Consumer/Handler가 있을 경우: 메시지 진입 처리 테스트 또는 서비스 호출 검증 테스트 작성

## 설계 문서 위치
- 전체 아키텍처: `docs/architecture.md`
- Task 문서: `docs/tasks/*.md`
- Plan 문서: `docs/plans/*.md`
- Plan Fix 문서: `docs/plans/fix-logs/*.md`
- Full Review Plan 문서: `docs/plans/full-review/*.md`
- Full Review Plan Fix 문서: `docs/plans/full-review/fix-logs/*.md`

## 작업 규칙
- 설계와 다르게 구현할 경우 이유를 주석 또는 작업 문서에 명시
- 하나의 Task는 하나의 기능만 구현
- 수정 범위를 명확히 제한
- 기존 코드 스타일 유지
- 테스트 없이 기능 추가 금지

## 금지 규칙
- Controller에서 비즈니스 로직 작성 금지
- Consumer, Handler, Scheduler에서 Repository 직접 호출 금지
- Controller와 ServiceResponse에서는 Repository 직접 호출 금지
- Entity를 API 응답으로 직접 반환 금지

## 외부 연동 규칙
- Prometheus 호출은 Client 계층에서만 수행
- Kafka Producer/Consumer는 별도 패키지로 분리
- DB 접근은 Repository만 수행

## 패키지 구조
- controller
- service
- repository
- domain (entity)
- dto
- client (외부 API)
- messaging (Kafka Producer)
- kafka.consumer 또는 messaging.consumer (Kafka Consumer)
- scheduler

## AI 작업 규칙
- agents 규칙 문서: `docs/agent-rules/*.md`

## Kafka 규칙
- Kafka 메시지 직렬화는 `JsonSerializer` 대신 `JacksonJsonSerializer`를 사용한다
- Spring Boot 4 기준 `JsonSerializer`는 deprecated 상태이므로 사용 금지

## Redis / Redisson Notes
- Spring Boot `4.0.6` 환경에서는 `org.redisson:redisson-spring-boot-starter`를 사용하지 않는다. 구형 `org.springframework.boot.autoconfigure.data.redis.RedisProperties` 클래스를 참조하여 애플리케이션 시작 시 실패할 수 있다
- 이 프로젝트에서는 `org.redisson:redisson`을 사용하고 `RedissonClient`는 설정 클래스에서 명시적으로 등록한다
- Redisson 명시적 설정에서도 `spring.data.redis.*` 프로퍼티를 재사용하여 Redis 접속 설정을 한 곳에서 관리한다
- 애플리케이션 서비스가 시작 시점에 `RedissonClient`를 필요로 하는 경우 `@ConditionalOnBean(RedisConnectionFactory.class)`로 빈 등록을 막지 않는다
- `RedissonClient` 빈이 없다는 오류로 애플리케이션 시작이 실패하면 서비스 생성자보다 명시적 Redisson 설정을 먼저 확인한다
