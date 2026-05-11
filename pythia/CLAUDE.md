# 프로젝트 컨텍스트

## 기술 스택
- Backend: Java 21, Spring Boot 4.0.6, Spring Data JPA, Spring Security, Spring AI 2.0.0-M6
- DB: PostgreSQL (개발: H2)
- 빌드: Gradle
- 테스트: JUnit 5 + Mockito (백엔드)

## 아키텍처 규칙
- 레이어: Controller → ServiceResponse → Service → Repository
- DTO/Entity 분리 필수
- REST API: /api/v1/** 패턴
- 모든 예외는 RuntimeException 상속 CustomException 사용
- 에러 응답은 공통 포맷 사용
- 외부 API 오류는 별도 Exception으로 변환

## Docker 파일 규칙
- 모든 docker 관련 파일은 `/docker` 경로에 생성한다
- docker-compose.yml, docker-compose.*.yml 등은 루트에 생성하지 않는다
- 환경별 compose 파일은 `/docker` 하위에서 분리한다 (예: docker-compose.kafka.yml)

## 코딩 컨벤션
- 백엔드: Google Java Style Guide
- 커밋: feat/fix/test/ 접두어 사용

## 테스트 규칙
- 서비스 레이어: 단위 테스트 필수 (Mockito)
- 컨트롤러: MockMvc 통합 테스트
  
## 설계 문서 위치
- 전체 아키텍처: docs/architecture.md
- 업무 문서 : docs/tasks/*.md

## 작업 규칙
- 설계와 다르게 구현할 경우 이유를 주석으로 명시
  
## 금지 규칙
- Controller에서 비즈니스 로직 작성 금지
- Controller 와 ServiceResponse에서는 Repository 직접 호출 금지 (Service 통해서만)
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
- messaging (Kafka)

## AI 작업 규칙
- 하나의 Task는 하나의 기능만 구현
- 수정 범위를 명확히 제한
- 기존 코드 스타일 유지
- 테스트 없이 기능 추가 금지

## 에이전트 워크플로우

### 에이전트 구성
| 에이전트 | 파일 | 역할 |
|----------|------|------|
| plan | `.claude/agents/plan.md` | 오케스트레이터. 요구사항 분석 및 전체 흐름 관리 |
| implementor | `.claude/agents/implementor.md` | 코드 구현 (신규 + 기존 파일) |
| reviewer | `.claude/agents/reviewer.md` | plan 기준 코드 스타일/보안/버그 리뷰 |
| fixer | `.claude/agents/fixer.md` | 리뷰 피드백 기반 코드 수정 (신규 + 기존 파일) |

### 실행 순서 (반드시 준수)
```
plan → implementor → reviewer
                         ↓ FAIL (최대 3회)
                       fixer → reviewer
                         ↓ PASS 또는 3회 초과
                       plan (최종 보고)
```

### 에이전트 호출 규칙
- **오케스트레이션은 plan 에이전트만** 수행한다
- implementor / reviewer / fixer 는 다른 에이전트를 직접 호출하지 않는다
- 리뷰-수정 루프는 **최대 3회**로 제한한다
- 3회 초과 시 plan 에이전트가 미해결 항목을 정리하여 사용자에게 수동 개입을 요청한다

### 에이전트별 참조 문서
- plan 에이전트는 작업 전 `docs/tasks/*.md` 에서 관련 업무 문서를 확인한다
- reviewer 에이전트는 `docs/architecture.md` 를 리뷰 기준으로 함께 참조한다
- 모든 에이전트는 이 CLAUDE.md의 **금지 규칙**, **아키텍처 규칙**, **패키지 구조**를 최우선으로 따른다
  
## Kafka 규칙
- Kafka 메시지 직렬화는 JsonSerializer 대신 JacksonJsonSerializer를 사용한다
- Spring Boot 4 기준 JsonSerializer는 deprecated 상태이므로 사용 금지