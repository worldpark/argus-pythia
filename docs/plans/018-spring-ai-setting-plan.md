# Task 018: Pyshia Spring AI 환경 세팅 — 구현 계획

## Context

Pyshia는 Task 013~017을 거치며 Kafka 메트릭 수신 / 임계값 평가 / 이메일 알림 / 영속화 능력을 갖췄으나, **LLM 기반 복합 패턴 분석** (architecture.md의 "LLM Analyzer" 모듈)이 비어 있다. 단순 임계값으로는 잡히지 않는 복합 패턴(예: GC 급증 + P99 동시 튐) 분석을 위해 후속 Task에서 LLM 호출이 필요하다.

본 task는 그 호출 가능 능력 자체 — 즉 (a) Spring AI **2.0.0-M6** 자동 구성으로 OpenAI **gpt-4.1-mini** 모델에 연결되는 `ChatClient`, (b) 분석 입력을 표현하는 Request DTO, (c) DTO → Prompt 문자열 변환기, (d) DTO를 받아 LLM 응답 문자열을 돌려주는 Service 진입점 — 까지를 신규 도입한다. 후속 task(임계값 evaluator의 복합 패턴 분기 → 호출 / 응답 파싱 / RAG 컨텍스트 주입)는 본 task 범위 외다.

확장성 요구(향후 Anthropic 추가 가능성)는 **`ChatClient` 인터페이스에만 의존**하여 자연스럽게 만족시키며, 모델 종속 코드(OpenAI 옵션 등)는 설정 한 곳에 격리한다.

> **버전 변경 이력 (2026-05-10)**: Task 명세 초안의 "Spring AI 1.1.6"은 Spring Boot 4.0.6과 런타임 비호환(`HttpHeaders.addAll(MultiValueMap)` `NoSuchMethodError` — Spring Framework 6 vs 7 충돌)이 사용자 결정에 따라 **Spring AI 2.0.0-M6**(Spring Boot 4 공식 지원, 2026-05-08 릴리즈)로 변경. M3부터 Spring Boot 4 호환. starter 좌표(`spring-ai-starter-model-openai`)와 `spring.ai.openai.*` 설정 키는 1.1.x와 동일하므로 코드 변경 없음. 차이는 (1) BOM 버전, (2) Spring milestone repository 추가, (3) 테스트 컨텍스트의 OpenAI 자동구성 exclude **제거** 가능.

---

## 1. 설계 방식 및 이유

### 방향: Spring AI `ChatClient` 단일 진입점 + DTO → PromptTemplate 변환 + 모델 설정 외부화

| 결정 | 선택 | 이유 |
|------|------|------|
| Spring AI 사용 방식 | **`ChatClient` (high-level) 의존, `ChatModel` 직접 호출 X** | `ChatClient`는 1.1.x에서 모든 provider(OpenAI/Anthropic/Azure 등) 공통 추상화. Provider 교체 시 호출부 0줄 수정. `ChatModel`은 raw 인터페이스라 옵션 처리/메시지 빌딩이 노출됨 — 호출부 결합도↑ |
| 의존 도입 | **`spring-ai-bom:2.0.0-M6` + `spring-ai-starter-model-openai`** + `https://repo.spring.io/milestone` repo | BOM으로 sub-module 버전 정렬. starter 한 줄로 자동 구성(OpenAiChatModel, ChatClient.Builder) 활성. Anthropic 전환 시 starter 좌표만 교체(`spring-ai-starter-model-anthropic`)하면 됨 — 코드 무변경. 2.0.0-M6는 milestone이라 Maven Central에 없으므로 Spring milestone repo 명시 필요 |
| `ChatClient` 빈 등록 | **`ChatClientConfig`에서 `ChatClient.Builder` → `ChatClient` 빈 1개 명시 등록** | 자동 구성은 Builder만 제공. `ChatClient` 자체는 앱이 default system prompt / 옵션을 묶어 빌드하는 게 권장 패턴. 명시 등록으로 모델 옵션(temperature 등) 변경 지점 단일화 |
| 모델 옵션 위치 | **`application.yml` → `spring.ai.openai.chat.options.*`** | Spring AI 표준 키. 코드 비종속. 환경별 프로파일 오버라이드 용이. API key는 `${OPENAI_API_KEY}` placeholder |
| 모델명 | **`gpt-4.1-mini`** | Task 명세 그대로. 옵션은 `spring.ai.openai.chat.options.model`로 지정 |
| Request DTO | **`MetricAnalysisRequest` record 1개 + 하위 record들** (`AnalysisTarget`, `MetricSummary`, `TimeSeriesPoint`) | 명세의 4-블록 입력 구조(분석대상/요약/시계열/요청)를 1:1 매핑. record로 불변성/equals/toString 무료. `kafka/dto`와 패키지 분리(`ai/dto`) — 도메인 경계 유지 |
| 분석 요청 사항(이상 징후 판단/원인/추가 지표/조치 4항) | **`MetricAnalysisRequest`에 boolean flag 또는 enum List 두지 **않음**, Prompt 템플릿에 4항 고정 문장으로 박음** | Task 명세의 4항은 현 단계에서 고정 — 가변화는 후속 요구가 생긴 뒤. flag로 만들면 Prompt 템플릿이 분기 갈래에 의해 깨지기 쉬움 |
| DTO → Prompt 변환 | **`MetricAnalysisPromptFactory` 단일 클래스**, Spring AI `PromptTemplate` 사용 | "Request data는 dto로 먼저 만든뒤에 prompt 변환" 명세 직접 구현. PromptTemplate은 Spring AI의 `{var}` placeholder 치환 지원 — 명세의 `{application}`, `{instance}`, `{timeSeriesTable}` 키와 직결 |
| `{timeSeriesTable}` 렌더링 | **TSV(탭 구분) 표 형태**, 헤더 1행 + 데이터 N행 | 가독성과 토큰 효율의 균형. JSON은 필드명 반복으로 토큰↑, CSV는 콤마 충돌 위험. TSV는 LLM이 표로 인식 잘 함. PromptFactory 내부 `renderTimeSeriesTable(List<TimeSeriesPoint>)` private 메서드 |
| 프롬프트 템플릿 위치 | **`src/main/resources/prompts/metric-analysis.st` (Spring AI Template — `.st` 확장)** | 코드와 분리. PromptTemplate가 classpath resource 로딩 지원. 운영 중 프롬프트 튜닝 시 코드 빌드/배포 없이 수정 가능 (다만 본 task는 jar 내장 — 운영 외부화는 후속) |
| LLM 호출 진입점 | **`MetricAnalysisService` (`@Service`)** — `String analyze(MetricAnalysisRequest)` | 후속 task가 의존할 단일 진입점. ChatClient 호출/응답 추출(`.content()`)을 한 곳에 격리. 응답 파싱은 본 task 범위 외 — String 그대로 반환 |
| Service 시그니처 동기/비동기 | **동기** | EmailService와 동일 패턴. 비동기/타임아웃 정책은 호출 컨텍스트(스케줄러/Kafka listener)에서 결정 — 후속 |
| 예외 부모 | **기존 `CustomException` 재사용** + `AiAnalysisException extends CustomException` | Task 015에서 도입한 공통 부모. 도메인 예외 추가만 |
| Error Code | **`AiErrorCode` enum (implements ErrorCode)** | EmailErrorCode 패턴 그대로. 시작값 3개: `INVALID_REQUEST`(DTO null/필수 누락), `LLM_CALL_FAILURE`(network/API 4xx-5xx), `EMPTY_RESPONSE`(응답 content 비어있음) |
| 테스트 작성 범위 | **PromptFactory 단위 테스트만 작성 + Service는 ChatClient mock 단위 테스트** | CLAUDE.md "테스트 없이 기능 추가 금지". 실제 OpenAI 호출 통합 테스트는 본 task 범위 외(API key/비용/네트워크 의존). Provider 추상화 검증은 mock으로 충분 |

### 비대상 (의식적 제외)

- **응답 파싱 / Structured Output**: 본 task는 raw `String` 반환. JSON 응답 강제 / record 매핑은 후속 task (응답 스키마 결정 후)
- **RAG / VectorStore**: PGVector·과거 장애 패턴 컨텍스트 주입은 별 task
- **Streaming 응답**: 후속(UX 결정 후)
- **재시도 / Circuit Breaker / Rate Limit**: spring-retry/resilience4j 도입은 후속
- **Controller/API**: Task 명세 명시 — Service까지만
- **Anthropic 동시 지원**: starter 좌표만 미리 정렬, 실제 빈/조건부 활성화는 그때
- **Range query 처리 / 멀티 노드 Aggregation**: Task 명세 Out of Scope

---

## 2. 구성 요소

### 신규 파일

**`pyshia/build.gradle` (수정)**
- `dependencyManagement` 블록 신설: Spring AI BOM
- `dependencies`에 추가: `implementation 'org.springframework.ai:spring-ai-starter-model-openai'`
- `repositories` 블록에 Spring milestone repo 추가 (M6는 Maven Central에 미배포)

```groovy
ext {
    springAiVersion = '2.0.0-M6'
}
repositories {
    mavenCentral()
    maven { url 'https://repo.spring.io/milestone' }
}
dependencyManagement {
    imports {
        mavenBom "org.springframework.ai:spring-ai-bom:${springAiVersion}"
    }
}
dependencies {
    // ... 기존 의존
    implementation 'org.springframework.ai:spring-ai-starter-model-openai'
}
```

> Spring AI 2.0은 내부적으로 Jackson 3(`tools.jackson` 패키지)를 사용한다. 본 프로젝트의 다른 직렬화 경로(Spring Boot 자동 ObjectMapper, Kafka JacksonJsonSerializer)는 Spring Boot 4의 Jackson 2를 그대로 사용하며 격리되므로 영향 없음. 우리 DTO record는 Spring AI 직렬화 경로를 거치지 않는다 (PromptFactory가 PromptTemplate placeholder로 직접 렌더링).

**`pyshia/src/main/resources/application.yml` (수정)**
```yaml
spring:
  ai:
    openai:
      api-key: ${OPENAI_API_KEY}
      chat:
        options:
          model: gpt-4.1-mini
          temperature: 0.2
```
> `temperature` 0.2: 분석 일관성 우선 (값은 후속 튜닝 시 yml만 변경)

**`com.example.pyshia.ai.config.ChatClientConfig`** — `@Configuration`
- `ChatClient.Builder` 주입
- `@Bean ChatClient chatClient(ChatClient.Builder builder)` 등록 — 현 단계에선 default system prompt 미설정 (PromptFactory가 user 메시지 일체 구성). 향후 system prompt 추가 시 이 클래스에서만 변경

**`com.example.pyshia.ai.dto.MetricAnalysisRequest`** — record
```java
public record MetricAnalysisRequest(
    AnalysisTarget target,
    List<MetricSummary> summaries,
    List<TimeSeriesPoint> timeSeries) {}
```

**`com.example.pyshia.ai.dto.AnalysisTarget`** — record
```java
public record AnalysisTarget(
    String application,
    String instance,
    Duration range  // 명세 "최근 15분" 표현. 기본 Duration.ofMinutes(15) 호출자 결정
) {}
```

**`com.example.pyshia.ai.dto.MetricSummary`** — record
```java
public record MetricSummary(
    String metricName,        // ex: "process_cpu_usage"
    SummaryAggregation aggregation,  // AVG | MAX
    BigDecimal value,
    String unit                // 선택: "%", "s", "count" 등 — null 허용
) {}
```

**`com.example.pyshia.ai.dto.SummaryAggregation`** — enum: `AVG`, `MAX`

**`com.example.pyshia.ai.dto.TimeSeriesPoint`** — record
```java
public record TimeSeriesPoint(
    OffsetDateTime timestamp,
    String metricName,
    BigDecimal value
) {}
```
> 시계열을 metric별 분리 record로 만들지 않고 (metricName, value) flat 구조로 둠 — 다양한 메트릭이 같은 시계열에 섞여도 단일 표로 렌더링 가능. PromptFactory가 정렬·그룹핑 담당.

**`com.example.pyshia.ai.prompt.MetricAnalysisPromptFactory`** — `@Component`
- `Prompt build(MetricAnalysisRequest request)` (Spring AI `Prompt` 반환)
- 내부:
  - 검증: target/summaries/timeSeries 필수 (null/empty → `AiAnalysisException(INVALID_REQUEST)`)
  - `PromptTemplate` 로딩 (classpath: `prompts/metric-analysis.st`) — 1회 로딩 후 인스턴스 보관
  - `Map<String, Object>` 변수: `application`, `instance`, `range` (사람이 읽는 문자열, 예 "최근 15분"), `metricSummary` (요약 블록 텍스트), `timeSeriesTable` (TSV 텍스트)
  - `template.create(variables)` → `Prompt`
- private:
  - `String renderSummaryBlock(List<MetricSummary>)` → `- process_cpu_usage (avg): 0.72\n- ...`
  - `String renderTimeSeriesTable(List<TimeSeriesPoint>)` → 헤더 `timestamp\tmetric\tvalue` + N행
  - `String renderRange(Duration)` → "최근 15분" 형태 (특수 케이스: 15분이면 명세 문구 그대로, 그 외 `Duration.toString()` 변형)

**`pyshia/src/main/resources/prompts/metric-analysis.st`** — Spring AI 템플릿
```text
# 분석 대상
- application: {application}
- instance: {instance}
- range: {range}

# 메트릭 요약
{metricSummary}

# 시계열 데이터
{timeSeriesTable}

# 분석 요청
- 이상 징후를 판단하라
- 원인 후보를 우선순위로 제시하라
- 추가 확인할 지표를 제안하라
- 조치 방안을 제시하라
```
> 명세 입력 프롬프트 그대로. "각 메트릭별 평균값 혹은 최대값"은 `{metricSummary}` 치환 결과로 대체.

**`com.example.pyshia.ai.service.MetricAnalysisService`** — `@Service`
- 의존: `ChatClient`, `MetricAnalysisPromptFactory` (생성자 주입)
- public: `String analyze(MetricAnalysisRequest request)`
  - PromptFactory.build → Prompt
  - `chatClient.prompt(prompt).call().content()`
  - 응답 null/blank → `AiAnalysisException(EMPTY_RESPONSE)`
  - Spring AI runtime 예외(`org.springframework.ai.retry.NonTransientAiException` 등 super: `RuntimeException`) → catch → `AiAnalysisException(LLM_CALL_FAILURE, cause)` 변환

**`com.example.pyshia.ai.exception.AiAnalysisException`** — `extends CustomException`
- 생성자: `(AiErrorCode, String message)`, `(AiErrorCode, String message, Throwable cause)`

**`com.example.pyshia.ai.exception.AiErrorCode`** — enum, `implements ErrorCode`
- `INVALID_REQUEST("AI_001", "Analysis request payload is invalid")`
- `LLM_CALL_FAILURE("AI_002", "LLM call failed")`
- `EMPTY_RESPONSE("AI_003", "LLM returned empty response")`

### 신규 테스트 파일

**`pyshia/src/test/java/com/example/pyshia/ai/prompt/MetricAnalysisPromptFactoryTest.java`** (단위)
- 정상 입력 → Prompt의 user message 텍스트가 명세 4-블록 구조 + 변수 치환 정확
- summaries empty / timeSeries empty / target null → AiAnalysisException(INVALID_REQUEST)
- timeSeriesTable: 헤더 1행 + 데이터 행 수 일치, 정렬(timestamp 오름차순) 검증

**`pyshia/src/test/java/com/example/pyshia/ai/service/MetricAnalysisServiceTest.java`** (단위)
- `ChatClient`, `MetricAnalysisPromptFactory` mock
- 정상: `chatClient.prompt(any).call().content()` stub → 반환값 확인
- mock fluent chain은 Mockito `RETURNS_DEEP_STUBS` 또는 명시 stub
- 응답 null/blank → AiAnalysisException(EMPTY_RESPONSE)
- ChatClient call 단계에서 RuntimeException → AiAnalysisException(LLM_CALL_FAILURE) + cause 보존

> `ChatClientConfig`는 빈 등록만 하므로 별도 테스트 X. 컨텍스트 로딩은 `PyshiaApplicationTests`로 자연 검증.

### 수정하지 않는 파일 (제약/안전)

- `kafka/**` — 본 task는 분석 능력만 추가. consumer hook 통합은 후속
- `email/**` — 무관
- `metric/**`, `alert/**` — 무관
- `PyshiaApplication.java` — `@ConfigurationPropertiesScan` 추가 불필요 (별도 ConfigurationProperties 신설 없음)
- 기존 `application.yml` 키 — `spring.ai.*`만 추가, 다른 키 변경 없음

---

## 3. 데이터 흐름

본 task는 **분석 능력만 제공**. 트리거는 후속 task.

```
(후속) ThresholdEvaluator (복합 패턴 감지)
   │
   ├─ MetricAnalysisRequest 조립
   │     ├─ AnalysisTarget(application, instance, Duration.ofMinutes(15))
   │     ├─ List<MetricSummary>  (각 메트릭별 AVG/MAX 1개씩)
   │     └─ List<TimeSeriesPoint> (수집된 raw 시계열)
   │
   ▼
MetricAnalysisService.analyze(request)
   │
   ├─ MetricAnalysisPromptFactory.build(request)
   │     ├─ 검증: target/summaries/timeSeries 필수
   │     │     실패 → AiAnalysisException(INVALID_REQUEST)
   │     ├─ renderSummaryBlock(summaries) → "- name (agg): value\n..."
   │     ├─ renderTimeSeriesTable(timeSeries) → TSV 헤더 + 행
   │     ├─ renderRange(Duration) → "최근 15분"
   │     └─ PromptTemplate("prompts/metric-analysis.st").create({
   │            application, instance, range, metricSummary, timeSeriesTable
   │        }) → Prompt
   │
   ├─ chatClient.prompt(prompt).call().content()  ← Spring AI ChatClient
   │     │ HTTP POST https://api.openai.com/v1/chat/completions
   │     │ model=gpt-4.1-mini, messages=[{role:user, content: rendered prompt}]
   │     ▼
   │   OpenAI API
   │     ├─ 성공: completion content (분석 결과 텍스트)
   │     ├─ 4xx/5xx: NonTransientAiException 또는 TransientAiException
   │     └─ 네트워크 장애: RestClient 예외 (RuntimeException 계열)
   │
   ├─ 응답 null/blank → AiAnalysisException(EMPTY_RESPONSE)
   ├─ 정상 → return String (본문)
   └─ 예외 → catch (RuntimeException) → AiAnalysisException(LLM_CALL_FAILURE, cause)
```

---

## 4. 예외 처리 전략

| 단계 | 상황 | 처리 |
|------|------|------|
| 부팅 | `${OPENAI_API_KEY}` 미설정 | Spring 환경 변수 해석 실패 — 부팅 fail-fast (의도) |
| 부팅 | spring-ai BOM/starter 없음 | Gradle 단계 검출 — 빌드 실패 |
| 호출 | request null | `AiAnalysisException(INVALID_REQUEST)` (PromptFactory 진입 직전 명시 throw) |
| 호출 | target/summaries/timeSeries 중 하나라도 null/empty | `AiAnalysisException(INVALID_REQUEST)` — 메시지에 누락 필드명 포함 |
| 호출 | application/instance blank | `AiAnalysisException(INVALID_REQUEST)` |
| Prompt 빌드 | 템플릿 리소스 누락 / Spring AI 내부 예외 | `AiAnalysisException(INVALID_REQUEST)` 또는 그대로 전파 (리소스 누락은 빌드 시 검출되므로 사실상 발생 X) |
| LLM 호출 | OpenAI 4xx (auth/quota) | Spring AI `NonTransientAiException` (RuntimeException) → catch → `AiAnalysisException(LLM_CALL_FAILURE, cause)` |
| LLM 호출 | OpenAI 5xx / 네트워크 timeout | Spring AI `TransientAiException` 등 → 동일하게 LLM_CALL_FAILURE 변환. **재시도는 본 task 미적용** — 호출자 결정 |
| LLM 호출 | 응답 본문 null/blank | `AiAnalysisException(EMPTY_RESPONSE)` |
| LLM 호출 | 비-Spring AI RuntimeException | `LLM_CALL_FAILURE`로 통합 변환. 원인 식별 필요 시 cause 체인 보존 |

핵심 원칙:
- **Swallow 금지**. 항상 `AiAnalysisException`으로 변환해 호출자에게 throw — 후속 트리거 task가 dedup/대체 채널/재시도 정책 결정 가능하게 함
- 재시도/Circuit Breaker/타임아웃은 호출 컨텍스트 결정 후 도입 — 본 task는 일관된 변환만

---

## 5. 검증 방법

### 본 task 자체의 검증

**자동:**
- `./gradlew :pyshia:test` BUILD SUCCESSFUL
  - `MetricAnalysisPromptFactoryTest` 케이스 전부 통과
  - `MetricAnalysisServiceTest` 케이스 전부 통과
- `./gradlew :pyshia:build` BUILD SUCCESSFUL — Spring AI 의존 추가로 컴파일/컨텍스트 로딩 깨지지 않는지 검증
- 기존 `PyshiaApplicationTests` 컨텍스트 로딩 통과
  - 단, 테스트 환경에 `OPENAI_API_KEY` placeholder가 빠지면 ApplicationContext 로딩 실패 — 검증 시 `OPENAI_API_KEY=test-dummy` 환경 변수 또는 `src/test/resources/application.yml`에 `spring.ai.openai.api-key: test-dummy` 설정 (실제 호출 안 일어나므로 dummy 값 OK)

**프롬프트 형태 검증 (PromptFactoryTest 내부):**
- `Prompt#getInstructions()`의 user message text가
  - "# 분석 대상", "# 메트릭 요약", "# 시계열 데이터", "# 분석 요청" 4 헤더 모두 포함
  - 변수 치환된 값(application, instance, range) 정확
  - timeSeriesTable이 TSV 헤더로 시작
  - 4-항목 분석 요청 문장 그대로 포함

### 수동 통합 검증 (선택, 본 task 범위 내 권장 — 성공 기준 "정상적으로 LLM과 연동 가능")
- 실제 OpenAI API key를 환경 변수로 설정 → Pyshia 부팅 → 일회성 Runner / `@PostConstruct` 임시 호출로 sample MetricAnalysisRequest 작성 → `MetricAnalysisService.analyze` 호출 → 콘솔에 LLM 응답 출력 확인
- 또는 IntegrationTest 한 개를 `@Tag("integration")` 으로 격리하고 `OPENAI_API_KEY` 있을 때만 실행 (`@EnabledIfEnvironmentVariable(named = "OPENAI_API_KEY", matches = ".+")`)

> 통합 테스트의 자동화 정착(별도 mock LLM 서버 / WireMock + OpenAI HTTP fixture)은 후속 task. 본 task는 mock 단위 테스트 + 선택적 수동 검증으로 갈음.

### 후속 task에서 작성/확장할 항목 (참고용)
- `ThresholdEvaluator`의 복합 패턴 분기 → `MetricAnalysisService.analyze` 호출 + 응답 → 알림 라우팅
- 응답 파싱 (Structured Output / JSON) → record 매핑
- RAG: PGVector 검색 → ChatClient `system` 메시지에 컨텍스트 주입
- 재시도 / Circuit Breaker / 타임아웃 / 비동기 발송

---

## 6. 트레이드오프

1. **`ChatClient` 의존 vs `ChatModel` 의존**: ChatClient는 옵션/메시지 빌딩을 캡슐화하는 만큼 **provider 중립성↑** (요구사항 직결) 단 fluent API 학습 필요. ChatModel은 raw하지만 옵션이 호출부에 노출 → provider 교체 시 호출부 변경 필요. ChatClient 채택.
2. **Prompt 템플릿 외부 파일(.st) vs 코드 내장**: 외부 파일은 **코드 빌드 없이 프롬프트 튜닝 가능**(다만 본 task는 jar 내장). 코드 내장은 한 곳에서 보임. 외부 파일 채택 — 운영 중 프롬프트 수정 빈도가 높을 것이라는 일반적 경험.
3. **시계열 표 형식 TSV vs CSV vs JSON**: TSV는 LLM이 표로 인식 잘 하고 토큰 효율 양호. CSV는 콤마 충돌 위험. JSON은 필드명 반복 토큰 비용. TSV 채택. 단점: 시각적 정렬은 LLM이 알아서 — 사람 디버깅 시 다소 빡빡.
4. **시계열 단일 flat record (`metricName` 포함) vs metric별 분리 record**: flat이 다양한 메트릭을 단일 표로 표현 가능 + DTO 수↓. 분리는 타입 안전성↑ but DTO 폭발. flat 채택. 후속 응답 파싱이 metric별 결과를 요구하면 그 시점에 분리 검토.
5. **Service 동기 vs 비동기**: Email과 동일 — 호출 컨텍스트 결정 시점에 비동기 도입. 본 task에서 `@Async` 도입 시 thread pool/타임아웃 정책이 외부 트리거 없이 결정되어 재작업 위험.
6. **재시도 미적용**: OpenAI는 transient 5xx 빈도 낮지 않음. 본 task에서 `Retryable` 한 줄로 도입할 수도 있으나 dedup/idempotency 정책과 함께 가야 안전 → 후속.
7. **Anthropic 동시 활성화 X**: `@Conditional` + 두 starter 동시 도입은 빈 충돌(자동 구성된 ChatModel 2개) 회피 로직 필요 → 본 task 범위 초과. 좌표 분리 + 단일 활성화 (config-only switch 후속) 정책 채택.
8. **Structured Output 미적용**: 응답 record 매핑은 응답 스키마 결정이 선행되어야 함. 임계값 평가 결과 → 알림 메시지 흐름이 굳어진 뒤 도입 (현 시점에 강제하면 후속 변경 비용↑).
9. **테스트 범위(mock 단위만)**: 실제 OpenAI 호출 통합 테스트는 비용/네트워크 의존. WireMock 기반 OpenAI fixture는 가치 있으나 별 task. mock 단위로 PromptFactory의 명세 4-블록 충실도 + Service의 예외 변환을 충분히 보장.
10. **`temperature` 0.2 고정**: 분석 일관성 우선. 후속 튜닝(예: 창의적 원인 가설 요구 시 0.5+)은 yml만 수정 — 코드 무영향.

---

## 핵심 파일 경로

신규/수정 (절대 경로):
- `C:\side_project\pyshia\build.gradle` — Spring AI BOM(**2.0.0-M6**) + OpenAI starter 의존 + Spring milestone repository 추가
- `C:\side_project\pyshia\src\main\resources\application.yml` — `spring.ai.openai.*` 키 추가
- `C:\side_project\pyshia\src\main\resources\prompts\metric-analysis.st` — 신규 (프롬프트 템플릿)
- `C:\side_project\pyshia\src\main\java\com\example\pyshia\ai\config\ChatClientConfig.java` — 신규 (`@Configuration`, ChatClient 빈)
- `C:\side_project\pyshia\src\main\java\com\example\pyshia\ai\dto\MetricAnalysisRequest.java` — 신규 (record)
- `C:\side_project\pyshia\src\main\java\com\example\pyshia\ai\dto\AnalysisTarget.java` — 신규 (record)
- `C:\side_project\pyshia\src\main\java\com\example\pyshia\ai\dto\MetricSummary.java` — 신규 (record)
- `C:\side_project\pyshia\src\main\java\com\example\pyshia\ai\dto\SummaryAggregation.java` — 신규 (enum)
- `C:\side_project\pyshia\src\main\java\com\example\pyshia\ai\dto\TimeSeriesPoint.java` — 신규 (record)
- `C:\side_project\pyshia\src\main\java\com\example\pyshia\ai\prompt\MetricAnalysisPromptFactory.java` — 신규 (`@Component`)
- `C:\side_project\pyshia\src\main\java\com\example\pyshia\ai\service\MetricAnalysisService.java` — 신규 (`@Service`)
- `C:\side_project\pyshia\src\main\java\com\example\pyshia\ai\exception\AiAnalysisException.java` — 신규 (`extends CustomException`)
- `C:\side_project\pyshia\src\main\java\com\example\pyshia\ai\exception\AiErrorCode.java` — 신규 (enum)
- `C:\side_project\pyshia\src\test\java\com\example\pyshia\ai\prompt\MetricAnalysisPromptFactoryTest.java` — 신규
- `C:\side_project\pyshia\src\test\java\com\example\pyshia\ai\service\MetricAnalysisServiceTest.java` — 신규
- `C:\side_project\pyshia\src\test\resources\application-test.yml` — 테스트 컨텍스트용 `spring.ai.openai.api-key: test-dummy` 오버라이드 (Round 1에서 Spring AI 1.1.6 호환 회피용으로 추가했던 OpenAI 자동구성 exclude는 **제거** — 2.0.0-M6에서 정상 부팅됨)

참조 (수정 없음):
- `C:\side_project\pyshia\CLAUDE.md` — 패키지/예외/테스트 규약
- `C:\side_project\docs\architecture.md` — Pythia LLM Analyzer 모듈 위치
- `C:\side_project\pyshia\src\main\java\com\example\pyshia\common\exception\CustomException.java` — 예외 부모(재사용)
- `C:\side_project\pyshia\src\main\java\com\example\pyshia\common\exception\ErrorCode.java` — 인터페이스(재사용)
- `C:\side_project\pyshia\src\main\java\com\example\pyshia\email\EmailService.java` — 동기 Service + 예외 변환 패턴 원본

---

## 후속 작업 (본 task 범위 외)

- 임계값 evaluator 복합 패턴 분기 → `MetricAnalysisService.analyze` 호출 → 응답 → 알림 라우팅
- 응답 파싱 (Structured Output / JSON record 매핑)
- PGVector 기반 RAG 컨텍스트 주입 (system 메시지)
- Anthropic 모델 동시 지원 + provider switch (yml config 기반)
- 재시도 / Circuit Breaker / 타임아웃 / 비동기 발송
- LLM 호출 통합 테스트 자동화 (WireMock OpenAI fixture)
- 호출 비용/토큰 사용량 메트릭화 (Micrometer)
- range query / 멀티 노드 aggregation (별 task — Task 018 Out of Scope)
