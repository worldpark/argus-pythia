# Task 015: Pyshia Email Alarm 구현 — 구현 계획

## Context

Pyshia는 Task 013에서 Kafka 메트릭 스냅샷 수신 진입점을, Task 014에서 임계값 정의 문서를 갖췄으나, 임계값 위반 시 운영자에게 알릴 수단이 전혀 없다. `spring-boot-starter-mail` 의존도 없고 `email`/`alarm` 패키지도 존재하지 않으며, CLAUDE.md가 규정한 공통 `CustomException` 부모 클래스 역시 아직 미구현 상태다.

본 task는 후속 알림 트리거 task(임계값 평가 결과 → 알림 발송)가 호출할 수 있는 **이메일 발송 능력 자체**를 신규 도입한다. 트리거 / Handler hook 통합은 본 task 범위 외 (별 task). 사용자 결정에 따라 (a) CustomException 공통 부모도 함께 본 task에서 도입하고, (b) 운영자 수신자 주소를 application.yml로 관리하며 EmailService가 직접 읽어 발송하는 `sendToOperator()` 헬퍼를 함께 제공한다.

---

## 1. 설계 방식 및 이유

### 방향: `email` 단일 신규 패키지 + `JavaMailSender` 위임 + `@ConfigurationProperties` 기반 수신자 관리

| 결정 | 선택 | 이유 |
|------|------|------|
| 의존 | **`spring-boot-starter-mail`** | Spring Boot 4 표준. 자동 구성으로 `JavaMailSender` 빈을 `spring.mail.*` 키 기반으로 만들어 줌 — 직접 `JavaMailSenderImpl` 빈 정의 불필요 |
| 패키지 위치 | **`com.example.pyshia.email`** (top-level) | Constraint("별도 email 패키지 생성") 명시. CLAUDE.md 패키지 구조에서 `kafka/`도 top-level로 다뤄지는 것과 동일한 결 |
| 발송 추상화 | **`EmailService` 단일 클래스 (인터페이스 분리 X)** | YAGNI. 대체 구현(SES 등) 도입 시점에 인터페이스 분리. 현재는 단일 구현으로 충분. 테스트는 `JavaMailSender` mocking으로 가능 |
| 발송 시그니처 | **`void send(EmailRequest)` + `void sendToOperator(String subject, String body)`** | generic send + 운영자 헬퍼 두 메서드. sendToOperator는 yml의 operator-recipients를 채워 send에 위임 (thin wrapper) |
| 수신자/발신자 관리 | **`@ConfigurationProperties("pyshia.email")` → `EmailProperties` record** | yml 키 변경 시 컴파일 타임 검출 가능. `@Value` 산발 주입 회피. `List<String>` 다중 수신자 자연스럽게 처리 |
| DTO | **`EmailRequest` record (to, subject, body)** | CLAUDE.md "DTO 없이 Map 사용 금지". `to`는 `List<String>`으로 다중 수신자 허용 |
| 메일 페이로드 | **plain text** (`SimpleMailMessage`) | 알람용 충분. HTML은 후속에서 도입 (template 엔진 없이 시작) |
| 비밀번호 / 사용자명 | **환경 변수 placeholder (`${MAIL_PASSWORD}`, `${MAIL_USERNAME}`)** | Gmail 앱 비밀번호를 yml에 평문 금지. 부팅 시 미설정이면 자동 구성 단계에서 fail-fast |
| 예외 부모 | **`CustomException`(abstract, RuntimeException 상속)을 본 task에서 신규 도입** + `EmailSendException extends CustomException` | 사용자 결정. CLAUDE.md 규칙 충족 + 향후 도메인 예외들이 동일 부모 사용 가능 |
| 예외 정보 | **에러 코드(enum) + message** | 공통 부모에 `ErrorCode` 인터페이스 슬롯만 신설. EmailErrorCode enum 1개로 시작 — 다른 도메인이 자기 enum을 만들 수 있는 패턴만 깔아 둠 |
| 동기/비동기 | **동기 발송 (Spring 기본 `JavaMailSender#send`)** | 후속 트리거 task에서 호출 컨텍스트(스케줄러/이벤트)가 결정되면 그 layer에서 비동기/큐잉 도입. 본 task에서 `@Async` 도입은 over-engineering |
| 테스트 작성 | **본 task에서는 작성하지 않음** (Task 015 명세 "Test: 임계값 처리와 이메일 설정이 끝나면 작성") | Task 명세가 명시적으로 deferred. CLAUDE.md "테스트 없이 기능 추가 금지"와 충돌하나 task별 명세가 우선. 후속 통합 테스트 task에서 일괄 작성 |

### 비대상 (의식적 제외)
- 임계값 평가 트리거 / Handler hook 통합
- 메일 템플릿 엔진(Thymeleaf 등), HTML 본문, 첨부파일
- 재시도 / DLQ / outbox
- 비동기 발송, rate limiting, dedup/cooldown
- 다른 도메인 예외(EmailSendException 외)의 CustomException 마이그레이션

---

## 2. 구성 요소

### 신규 파일

**`pyshia/build.gradle` (수정)**
- 추가: `implementation 'org.springframework.boot:spring-boot-starter-mail'`

**`pyshia/src/main/resources/application.yml` (수정)**
```yaml
spring:
  mail:
    host: smtp.gmail.com
    port: 587
    username: ${MAIL_USERNAME}
    password: ${MAIL_PASSWORD}
    properties:
      mail:
        smtp:
          auth: true
          starttls:
            enable: true

pyshia:
  email:
    from: ${MAIL_USERNAME}
    operator-recipients:
      - ops@example.com   # 실제 운영 주소로 교체 (또는 환경별 프로파일에서 오버라이드)
```

**`com.example.pyshia.common.exception.CustomException`** — abstract class, RuntimeException 상속
- 필드: `ErrorCode errorCode`
- 생성자: `(ErrorCode, String message)`, `(ErrorCode, String message, Throwable cause)`
- getter: `getErrorCode()`

**`com.example.pyshia.common.exception.ErrorCode`** — interface
- `String code()`
- `String defaultMessage()`
- 다른 도메인이 enum으로 구현. 본 task에서는 EmailErrorCode 1개 도입

**`com.example.pyshia.email.EmailService`** — `@Service`
- 의존: `JavaMailSender`, `EmailProperties` (생성자 주입)
- public method:
  - `void send(EmailRequest request)` — request.to/subject/body 검증 후 SimpleMailMessage 빌드 → `mailSender.send(...)` 호출
  - `void sendToOperator(String subject, String body)` — `properties.operatorRecipients()`로 `EmailRequest` 구성 → `send` 위임
- 모든 발송 실패는 `EmailSendException`으로 변환 throw

**`com.example.pyshia.email.dto.EmailRequest`** — record
- `List<String> to`, `String subject`, `String body`
- 정합성 검증(빈 to/subject/body 체크)은 EmailService 진입 시점

**`com.example.pyshia.email.config.EmailProperties`** — `@ConfigurationProperties(prefix = "pyshia.email")`, record 또는 `@Validated` 클래스
- `String from`
- `List<String> operatorRecipients`

**`com.example.pyshia.email.exception.EmailSendException`** — `extends CustomException`
- 생성자: `(EmailErrorCode, String message, Throwable cause)`

**`com.example.pyshia.email.exception.EmailErrorCode`** — enum, `implements ErrorCode`
- 후보 값:
  - `INVALID_RECIPIENT` — 수신자 누락/포맷 오류
  - `INVALID_PAYLOAD` — subject/body 누락
  - `SMTP_FAILURE` — `MailException` 계열 (인증/연결/전송 실패 통합)

### 수정하지 않는 파일 (제약/안전)
- 기존 `kafka/**` (Consumer/Handler/Config) — 본 task는 발송 능력만 추가
- 기존 DTO들 — 무관
- `PyshiaApplication.java` — `@ConfigurationPropertiesScan` 추가가 필요할 수 있음(EmailProperties 활성화). 자동 구성으로 안 잡히면 **부팅 클래스에 `@ConfigurationPropertiesScan` 한 줄만 추가** — 다른 변경 없음

---

## 3. 데이터 흐름

본 task는 발송 능력만 제공. 트리거는 후속 task.

```
(후속) ThresholdEvaluator (임계값 위반 감지)
   │
   ▼
EmailService.sendToOperator(subject, body)
   │
   ├─ EmailProperties.operatorRecipients()  →  List<String> to
   ├─ EmailProperties.from()                →  from
   │
   └─▶ EmailService.send(EmailRequest(to, subject, body))
         │
         ├─ 검증: to/subject/body 비어있지 않음 (실패 시 EmailSendException(INVALID_*))
         │
         ├─ SimpleMailMessage 빌드 (from, to[], subject, text)
         │
         └─▶ JavaMailSender.send(message)        ← Spring Boot 자동 구성
                │ TLS handshake → SMTP AUTH → DATA
                ▼
              smtp.gmail.com:587
                │
                ├─ 성공: void return
                └─ 실패: MailException 계열 → EmailService가 catch → EmailSendException(SMTP_FAILURE) 변환 throw
```

호출자 컨텍스트(예: 후속 트리거 task의 알림 라우터)가 동기/비동기 결정. EmailService 자체는 동기.

---

## 4. 예외 처리 전략

| 단계 | 상황 | 처리 |
|------|------|------|
| 부팅 | `${MAIL_USERNAME}`/`${MAIL_PASSWORD}` 미설정 | Spring 환경 변수 해석 실패 → 부팅 fail-fast (의도) |
| 부팅 | `pyshia.email.operator-recipients` 비어있음 | EmailProperties에 `@NotEmpty` 검증 또는 EmailService 첫 호출 시 IllegalState. 결정: 부팅 시 `@Validated` + `@NotEmpty`로 fail-fast (운영 안전) |
| 호출 | `request.to`가 null/empty | EmailSendException(INVALID_RECIPIENT) throw — 호출자에게 즉시 통지 |
| 호출 | `request.subject`/`body` 비어있음 | EmailSendException(INVALID_PAYLOAD) throw |
| 발송 | `MailAuthenticationException` (Gmail 앱 비밀번호 오류 등) | catch → EmailSendException(SMTP_FAILURE, cause=원본) throw + log.error |
| 발송 | `MailSendException` (네트워크/타임아웃 등) | 동일 — SMTP_FAILURE로 통합 변환 |
| 발송 | 기타 `MailException` 하위 | 동일 — SMTP_FAILURE |
| 발송 | 비-Mail RuntimeException (드물게 ClassCast 등) | catch하지 않고 그대로 전파 — 상위에서 처리 |

핵심 원칙:
- **실패는 swallow 금지**. 항상 EmailSendException으로 변환해 호출자에게 throw — 후속 트리거 task가 dedup/재시도/대체 채널 정책을 결정 가능하게 함.
- 본 task에서는 재시도 X. JavaMailSender의 connectionTimeout/readTimeout 등 timeout 속성도 spring.mail.properties.mail.smtp.* 기본값 그대로 사용 (튜닝은 후속).

---

## 5. 검증 방법

### 본 task 자체의 검증 (테스트 작성 X — Task 명세상 deferred)
- `./gradlew :pyshia:build` BUILD SUCCESSFUL — 의존 추가/패키지 신설로 컴파일/컨텍스트 로딩 깨지지 않는지
- 기존 `PyshiaApplicationTests` 컨텍스트 로딩 통과 (단, `MAIL_USERNAME/MAIL_PASSWORD` 환경 변수가 없으면 ApplicationContext 로딩 실패 가능 → 검증용 더미 값 환경에 설정하거나 test profile에서 `spring.mail.*` 오버라이드)

### 수동 통합 검증 (선택)
- 실제 Gmail 계정 + 앱 비밀번호로 환경 변수 설정 → Pyshia 부팅 → Spring Boot Actuator 또는 한 회성 Runner로 `EmailService.sendToOperator("test", "hello")` 호출 → 운영자 메일 수신 확인
- 또는 GreenMail 같은 임베디드 SMTP 띄워 round-trip 확인 (의존 추가가 따라오므로 후속 통합 테스트 task에서 검토)

### 후속 테스트 task에서 작성할 항목 (참고용 — 본 task 산출물 아님)
- `EmailServiceTest`: JavaMailSender mock, (1) 정상 send 시 SimpleMailMessage가 올바른 from/to/subject/body로 호출되는지 ArgumentCaptor 검증, (2) MailException 발생 시 EmailSendException(SMTP_FAILURE)로 변환되는지, (3) 빈 to/subject/body에서 INVALID_* 예외, (4) sendToOperator 호출 시 properties의 operator-recipients가 to로 들어가는지
- `EmailPropertiesTest`: yml 바인딩 round-trip, `@NotEmpty` 검증 동작
- `CustomExceptionTest`: ErrorCode 보존 / cause 체인 보존

---

## 6. 트레이드오프

1. **CustomException 공통 부모를 본 task에 도입**: 사용자 결정. 장점은 CLAUDE.md 규칙 즉시 충족 + 후속 도메인 재사용 가능. 단점은 본 task scope가 다소 확장되어 검토 면적이 커짐. 완화책: ErrorCode 인터페이스 + EmailErrorCode 1개로 최소 패턴만 도입, 다른 도메인 예외 마이그레이션은 별 task로 분리.
2. **EmailService 인터페이스 분리 X**: SES 등 대체 구현이 필요해지면 그 시점에 분리 비용 작음 (호출자 의존이 EmailService 클래스 한 곳). 지금 분리하면 빈 1개 + 인터페이스 1개로 간접화 비용만 증가.
3. **plain text vs HTML**: 알람은 plain text로 충분. HTML/template 도입은 사용자 보낼 메시지 수가 많아질 때.
4. **동기 발송**: 임계값 위반 알림은 본질적으로 burst 가능 → 호출 스레드(스케줄러/Kafka listener)가 SMTP 지연으로 막힐 수 있음. 본 task는 동기로 시작, 후속 트리거 task에서 호출 측이 `@Async` / 큐잉 결정. 이유: 비동기 결정은 "어디서 호출할지" 가 정해진 다음 결정해야 안전.
5. **`@ConfigurationProperties` vs `@Value`**: `@Value`가 1줄 짧지만, operator-recipients가 List이고 여러 키가 묶여 있어 `@ConfigurationProperties`가 가독성/타입 안전성/검증 모두 우수. 부팅 클래스에 `@ConfigurationPropertiesScan` 한 줄 추가 필요.
6. **재시도 미적용**: SMTP 일시 장애 시 1회 실패로 알림 누락 가능. 그러나 재시도는 dedup 정책과 함께 가야 하고(중복 발송 방지) → 본 task에서 분리. 대신 EmailSendException으로 throw하므로 후속 layer가 결정 가능.
7. **테스트 미포함 (CLAUDE.md 충돌)**: Task 명세가 명시적으로 deferred. 위험은 후속 통합 테스트 task가 누락되는 것 — plan에 후속 테스트 항목을 미리 명시해 두어 누락 방지.
8. **수신자 List를 yml에 평문**: 운영자 이메일은 일반적으로 secret이 아니나, 환경별로 다를 수 있어 `${OPS_RECIPIENTS}` 환경변수 placeholder 도입 여지 있음. 본 task는 yml 직접 기재로 시작, 환경별 분리는 프로파일(application-{env}.yml)로 후속.

---

## 핵심 파일 경로

신규/수정 (절대 경로):
- `C:\side_project\pyshia\build.gradle` — spring-boot-starter-mail 의존 추가
- `C:\side_project\pyshia\src\main\resources\application.yml` — `spring.mail.*` + `pyshia.email.*` 키 추가
- `C:\side_project\pyshia\src\main\java\com\example\pyshia\PyshiaApplication.java` — `@ConfigurationPropertiesScan` 추가 (한 줄)
- `C:\side_project\pyshia\src\main\java\com\example\pyshia\common\exception\CustomException.java` — 신규 (abstract, RuntimeException 상속)
- `C:\side_project\pyshia\src\main\java\com\example\pyshia\common\exception\ErrorCode.java` — 신규 (interface)
- `C:\side_project\pyshia\src\main\java\com\example\pyshia\email\EmailService.java` — 신규 (`@Service`)
- `C:\side_project\pyshia\src\main\java\com\example\pyshia\email\dto\EmailRequest.java` — 신규 (record)
- `C:\side_project\pyshia\src\main\java\com\example\pyshia\email\config\EmailProperties.java` — 신규 (`@ConfigurationProperties`)
- `C:\side_project\pyshia\src\main\java\com\example\pyshia\email\exception\EmailSendException.java` — 신규
- `C:\side_project\pyshia\src\main\java\com\example\pyshia\email\exception\EmailErrorCode.java` — 신규 (enum)

참조 (수정 없음):
- `C:\side_project\pyshia\CLAUDE.md` — 패키지/예외/Kafka 규약 정합성 확인
- `C:\side_project\pyshia\src\main\java\com\example\pyshia\kafka\consumer\*Handler.java` — 후속 트리거 task의 호출 지점 후보 (본 task 수정 X)
- `C:\side_project\docs\metrics\thresholds.md` — 후속 트리거 task의 평가 기준

---

## 후속 작업 (본 task 범위 외)
- ThresholdEvaluator 도입 + Handler hook에서 EmailService.sendToOperator 호출
- 통합 테스트 task: EmailServiceTest, EmailPropertiesTest, CustomExceptionTest 일괄 작성 (Task 015 Test 섹션 명세)
- 알림 dedup/cooldown, 재시도/큐잉, 비동기 발송 정책
- 메일 템플릿/HTML 본문, 첨부파일
- 환경별 프로파일(application-{dev,staging,prod}.yml)에서 운영자 수신자 분리
- 다른 도메인 예외들의 CustomException 마이그레이션 (Kafka 측 등)
