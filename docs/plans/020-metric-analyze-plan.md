# Task 020 — 임계값 위반 시 LLM 분석 결과를 이메일에 첨부 계획

## Context
현재 임계값 위반 흐름.

```
ThresholdEvaluator.evaluateOne
  → store.shouldSend(...) == true
  → AlertNotifier.notify(kind, severity, key, value, threshold, consecutive)
      → AlertMessageFormatter.subject(...) / body(...)
      → EmailService.sendToOperator(subject, body)
```

본 Task는 `AlertNotifier.notify` 내부에서 **DB에 1분 주기로 적재된 최근 10분 분량 스냅샷**을 조회하여 `MetricAnalysisRequest`로 변환한 뒤, `MetricAnalysisService.analyze(request)`로 LLM 분석을 받아 이메일 본문 말미에 첨부한다.

조회 대상 Entity:
- `JvmMetricSnapshotEntity` — CPU/Heap/OldGen/GC/Thread 컬럼이 한 레코드에 평탄화
- `HttpMetricSnapshotEntity` (+ `HttpEndpointMetricPointEntity`) — `HttpMetricKind ∈ {P99, RPS, ERROR_RATE}` × endpoint 별 point
- `HikariMetricSnapshotEntity` (+ `HikariPoolMetricPointEntity`) — `HikariMetricKind ∈ {ACTIVE, PENDING, USAGE_RATIO}` × pool 별 point

`MetricAnalysisService`/`MetricAnalysisRequest` 등 Task 018 산출물은 변경하지 않는다(Task 제약).

## 1. 설계 방식 및 이유

### 1.1 단일 조립기(Assembler) + Notifier 통합
- **신규**: `MetricAnalysisRequestAssembler` — `MetricKind`, `ViolationKey`를 입력받아 (1) 카테고리 디스패치, (2) 10분 분량 Repository 조회, (3) `MetricAnalysisRequest` 변환을 책임지는 단일 진입점.
- **수정**: `AlertNotifier` — `MetricAnalysisService` + `MetricAnalysisRequestAssembler` 주입. 본문 조립 직전 분석 호출, 결과를 `AlertMessageFormatter.body(...)`에 추가 인자로 전달.
- **수정**: `AlertMessageFormatter.body(...)` — 옵셔널 `String analysis` 인자 추가, 값이 있을 경우 "## LLM 분석" 섹션 부착.
- **수정**: `JvmMetricSnapshotRepository`, `HttpMetricSnapshotRepository`, `HikariMetricSnapshotRepository` — 각각 "app/instance/시간 범위" 조회 메서드 추가.

이유:
- Assembler는 alert 컨텍스트에 한정된 변환 책임(다른 진입점에서 재사용 가능성도 열어둠).
- 기존 `AlertNotifier`가 흐름의 단일 진입점이므로 호출 지점을 한 곳에 모음.
- `MetricAnalysisService`/Request DTO는 무수정.

### 1.2 동기 호출 (이미 @Async 컨텍스트)
- `AlertNotifier.notify`는 이미 `@Async`. 그 내부에서 Repository 조회와 `analyze` 호출을 **동기**로 수행한다.
- 결과를 본문에 포함해야 하므로 호출 결과를 기다려야 함. 추가 비동기화는 불필요.

### 1.3 분석 실패 시 fallback (이메일은 반드시 발송)
- Repository 조회 실패, 데이터 0건, `AiAnalysisException`, 그 외 `RuntimeException` 모두 흡수.
- 위 어느 경우에도 분석 섹션 없이 기존 본문으로 이메일 발송. 운영자 알림 우선.

### 1.4 카테고리 디스패치 규칙

| `MetricKind` | 조회 Entity | 추출 컬럼/포인트 필터 | unit |
|---|---|---|---|
| `JVM_CPU` | `JvmMetricSnapshotEntity` | `cpuUsagePercent` | `%` |
| `JVM_HEAP` | 〃 | `heapUsagePercent` | `%` |
| `JVM_HEAP_OLD_GEN` | 〃 | `oldGenUsagePercent` | `%` |
| `JVM_GC_PAUSE` | 〃 | `gcAvgDurationSeconds` | `초` |
| `JVM_GC_COUNT` | 〃 | `gcCount` | `회` |
| `JVM_THREAD_ACTIVE` | 〃 | `threadActiveCount` (Integer→BigDecimal) | `개` |
| `JVM_THREAD_PEAK` | 〃 | `threadPeakCount` | `개` |
| `JVM_THREAD_DAEMON` | 〃 | `threadDaemonCount` | `개` |
| `HTTP_P99` | `HttpMetricSnapshotEntity.points` where `kind=P99 AND endpoint=key.sub()` | `value` | `초` |
| `HTTP_ERROR_RATE` | 〃 where `kind=ERROR_RATE AND endpoint=key.sub()` | `value` | `%` |
| `HIKARI_ACTIVE` | `HikariMetricSnapshotEntity.points` where `kind=ACTIVE AND pool=key.sub()` | `value` | `개` |
| `HIKARI_PENDING` | 〃 where `kind=PENDING AND pool=key.sub()` | `value` | `개` |
| `HIKARI_USAGE_RATIO` | 〃 where `kind=USAGE_RATIO AND pool=key.sub()` | `value` | `%` |

- `metricName` 출력값: `kind.name()` (안정적 영문 식별자).
- `unit`: `kind.getUnit()` (이미 enum에 존재).
- HTTP/Hikari의 `sub`(endpoint/pool) 매칭은 동일 인스턴스의 동일 식별자에 한해 시계열 추출.
- 타임스탬프: JVM은 `collectedAt`, HTTP/Hikari point는 `measuredAt` (null이면 부모 `collectedAt`로 fallback).

### 1.5 10분 조회 윈도우
- 기준: `now = OffsetDateTime.now()` → `from = now.minusMinutes(10)` → `collectedAt >= from AND collectedAt <= now`, `ORDER BY collectedAt ASC`.
- 상수: `private static final Duration WINDOW = Duration.ofMinutes(10);` (Assembler 내부 private 상수, 외부 노출 없음).
- 1분 주기 적재 가정 → 최대 10건. validator(`MetricAnalysisPromptFactory`)는 비어있지 않으면 통과.

### 1.6 `MetricAnalysisRequest` 매핑 규칙
| 필드 | 값 |
|---|---|
| `target.application` | `key.application()` |
| `target.instance` | `key.instance()` |
| `target.range` | `Duration.ofMinutes(10)` (조회 윈도우와 일치) |
| `summaries[0]` | `MetricSummary(kind.name(), AVG, avg(values), kind.getUnit())` |
| `summaries[1]` | `MetricSummary(kind.name(), MAX, max(values), kind.getUnit())` |
| `timeSeries[*]` | 10개 이하 `TimeSeriesPoint(timestamp, kind.name(), value)`, asc 정렬 |

- `avg` 계산: `RoundingMode.HALF_UP`, scale=6 (`MetricStoreServiceImpl`의 precision=19/scale=6과 일치).
- `null` 값(예: cpuUsagePercent가 null인 행)은 집계/시계열에서 제외. 모든 값이 null이면 데이터 0건과 동일 처리.

### 1.7 본문 분석 섹션 포맷
```
임계값 위반이 감지되었습니다.

메트릭: JVM CPU 사용률
심각도: CRITICAL
...
연속 위반: 3회

## LLM 분석
<analyze 호출 결과 문자열 그대로 부착>
```
- 별도 마크업 없이 LLM 출력 원문 부착(Task 제약: `MetricAnalysisService`의 반환값을 그대로 사용).

## 2. 구성 요소

### 2.1 신규 파일
| 경로 | 역할 |
|---|---|
| `pythia/src/main/java/com/example/pythia/alert/service/MetricAnalysisRequestAssembler.java` | 위반 메트릭 → 10분 윈도우 Repository 조회 → `MetricAnalysisRequest` 변환 |
| `pythia/src/test/java/com/example/pythia/alert/service/MetricAnalysisRequestAssemblerTest.java` | Assembler 단위 테스트 (Repository는 모킹, 카테고리별/엣지 케이스) |

### 2.2 수정 파일
| 경로 | 변경 요약 |
|---|---|
| `pythia/src/main/java/com/example/pythia/alert/service/AlertNotifier.java` | `MetricAnalysisService`, `MetricAnalysisRequestAssembler` 주입; `notify` 본문 조립 전 분석 호출 + fallback 처리 |
| `pythia/src/main/java/com/example/pythia/alert/service/AlertMessageFormatter.java` | `body(...)`에 `String analysis` 인자 추가 (단일 시그니처) |
| `pythia/src/main/java/com/example/pythia/metric/repository/JvmMetricSnapshotRepository.java` | `findByApplicationAndInstanceAndCollectedAtBetweenOrderByCollectedAtAsc(String, String, OffsetDateTime, OffsetDateTime)` |
| `pythia/src/main/java/com/example/pythia/metric/repository/HttpMetricSnapshotRepository.java` | 동일 시그니처 + `@EntityGraph(attributePaths = "points")` (LAZY OneToMany 해소) |
| `pythia/src/main/java/com/example/pythia/metric/repository/HikariMetricSnapshotRepository.java` | 동일 시그니처 + `@EntityGraph(attributePaths = "points")` |
| `pythia/src/test/java/com/example/pythia/alert/service/AlertNotifierTest.java` | `MetricAnalysisService`/`MetricAnalysisRequestAssembler` 모킹 추가, 분석 성공/실패 케이스 추가 |
| `pythia/src/test/java/com/example/pythia/alert/service/AlertMessageFormatterTest.java` | `analysis` 인자 분기 케이스 추가 (없으면 신규) |

### 2.3 `MetricAnalysisRequestAssembler` 핵심 시그니처
```java
@Service
public class MetricAnalysisRequestAssembler {

  private static final Duration WINDOW = Duration.ofMinutes(10);

  // Repository 3개 주입. 시각은 OffsetDateTime.now() 내부 사용 (단순)

  @Transactional(readOnly = true)
  public MetricAnalysisRequest assemble(MetricKind kind, ViolationKey key);
}
```
- `@Transactional(readOnly = true)` — HTTP/Hikari의 LAZY 컬렉션을 `@EntityGraph`로 해소하므로 트랜잭션 경계는 안전 측면. AlertNotifier는 트랜잭션 무관.
- `currentValue`/`threshold`는 시계열 AVG/MAX로 LLM이 추세 파악 가능하므로 시그니처에서 제외(YAGNI).

### 2.4 `AlertMessageFormatter.body(...)` 변경안
```java
public String body(MetricKind kind, Severity severity, ViolationKey key,
    BigDecimal value, BigDecimal threshold, int consecutive, String analysis) {
  // 기존 본문 구성 ...
  if (analysis != null && !analysis.isBlank()) {
    sb.append('\n').append("## LLM 분석").append('\n').append(analysis).append('\n');
  }
  return sb.toString();
}
```
- 호출자가 `AlertNotifier` 단 한 곳이므로 기존 6-인자 시그니처를 제거하고 7-인자 단일 시그니처로 변경(죽은 코드 회피).

### 2.5 `AlertNotifier.notify` 변경안 (의사 코드)
```java
@Async
public void notify(MetricKind kind, Severity severity, ViolationKey key,
    BigDecimal value, BigDecimal threshold, int consecutive) {
  String analysis = null;
  try {
    MetricAnalysisRequest req = assembler.assemble(kind, key);
    analysis = analysisService.analyze(req);
  } catch (RuntimeException e) {
    log.warn("LLM analysis skipped: kind={} key={} error={}", kind, key, e.getMessage());
  }
  try {
    String subject = formatter.subject(kind, severity, key);
    String body = formatter.body(kind, severity, key, value, threshold, consecutive, analysis);
    emailService.sendToOperator(subject, body);
  } catch (EmailSendException e) {
    log.error("Failed to send alert: ...", e);
  }
}
```

### 2.6 Repository 메서드 (Spring Data 명명 규칙 기반)
- JVM:
```java
List<JvmMetricSnapshotEntity> findByApplicationAndInstanceAndCollectedAtBetweenOrderByCollectedAtAsc(
    String application, String instance, OffsetDateTime from, OffsetDateTime to);
```
- HTTP:
```java
@EntityGraph(attributePaths = "points")
List<HttpMetricSnapshotEntity> findByApplicationAndInstanceAndCollectedAtBetweenOrderByCollectedAtAsc(
    String application, String instance, OffsetDateTime from, OffsetDateTime to);
```
- Hikari: HTTP와 동일 패턴 (`@EntityGraph(attributePaths = "points")`).

## 3. 데이터 흐름

```
ThresholdEvaluator.evaluateOne
  ├─ store.shouldSend(...) == true
  └─ notifier.notify(kind, severity, key, value, threshold, consecutive)
        │ [AlertNotifier — @Async 워커 스레드]
        ├─ assembler.assemble(kind, key)   ← @Transactional(readOnly=true)
        │     ├─ to = OffsetDateTime.now()
        │     ├─ from = to.minus(Duration.ofMinutes(10))
        │     ├─ kind 카테고리 분기:
        │     │   JVM    → jvmRepo.findBy...Between...(app, instance, from, to)
        │     │             → 각 row에서 kind에 해당하는 컬럼 값 추출
        │     │   HTTP   → httpRepo.findBy...Between...(...) → points where kind/endpoint 매칭
        │     │   HIKARI → hikariRepo.findBy...Between...(...) → points where kind/pool 매칭
        │     ├─ null 값 제외 후 (timestamp, value) 리스트화
        │     └─ MetricAnalysisRequest(target, [AVG/MAX summary], timeSeries) 반환
        ├─ try: analysisService.analyze(request)
        │       → analysis: String
        │   catch RuntimeException:
        │       analysis = null  (warn 로그)
        ├─ formatter.subject(...) / formatter.body(..., analysis)
        └─ emailService.sendToOperator(subject, body)
              → SMTP 전송 (운영자 수신함)
```

## 4. 예외 처리 전략

| 케이스 | 처리 |
|---|---|
| Repository 조회 결과 0건 | Assembler에서 `AiAnalysisException(INVALID_REQUEST, "no metric data")` 던짐 → AlertNotifier에서 흡수, 분석 없이 발송 |
| JVM 행 전부 해당 컬럼 null | 동일 처리 (0건과 등가) |
| HTTP/Hikari point 매칭 0건 (sub 미존재) | 동일 처리 |
| 사용자 sub가 null (예: JVM 메트릭) | JVM 카테고리는 sub 사용 안 함 → 무관 |
| `DataAccessException` (DB 장애) | AlertNotifier가 `RuntimeException`으로 흡수, warn 로그, 분석 없이 발송 |
| `MetricAnalysisService.analyze` → `AiAnalysisException(*)` | 흡수, 분석 없이 발송 |
| `EmailService.sendToOperator` → `EmailSendException` | 기존 동일하게 error 로그 (전파 안 함) |
| 매우 긴 LLM 응답 → SMTP 실패 | `EmailSendException` 경로로 처리. 본 Task에서 절단/요약은 미도입 |
| LLM 호출 지연 | `@Async` 워커 스레드 점유. 본 Task에서 타임아웃 미도입 (과도한 설계 금지). 후속 Task에서 ChatClient 타임아웃 설정 가능 |
| @EntityGraph 미적용 시 LazyInitializationException | 위 Repository 변경으로 사전 방지. 별도 처리 불필요 |

## 5. 검증 방법

### 5.1 단위 테스트
- **`MetricAnalysisRequestAssemblerTest`** (신규, Mockito로 3개 Repository 모킹)
  - JVM_CPU: 5개 JVM 스냅샷 mocking → summaries에 AVG/MAX, timeSeries 5건, range=10분
  - JVM_HEAP: heapUsagePercent 컬럼 추출 검증
  - JVM_THREAD_ACTIVE: Integer → BigDecimal 변환 검증
  - HTTP_P99: 동일 인스턴스의 P99 + 다른 endpoint 혼재 → sub(endpoint) 일치 항목만 추출
  - HIKARI_USAGE_RATIO: pool 일치 항목만 추출
  - 데이터 0건 → `AiAnalysisException(INVALID_REQUEST)` 발생
  - 일부 row의 해당 컬럼이 null → null만 제외하고 나머지로 정상 생성
  - 결과가 `MetricAnalysisPromptFactory.validate`를 통과해야 함을 통합적으로 검증(실제 PromptFactory 1회 호출, 예외 없음)

- **`AlertMessageFormatterTest`** (신규 또는 확장)
  - `analysis = null` → 본문에 "## LLM 분석" 미포함
  - `analysis = " "` (blank) → 미포함
  - `analysis = "분석결과"` → 본문에 "## LLM 분석"과 "분석결과" 포함

- **`AlertNotifierTest`** (수정)
  - 정상: assembler/analysisService mocking → `analysis`가 `formatter.body`에 전달됨, `sendToOperator` 호출
  - assembler가 `AiAnalysisException`을 던질 때 → 본문에 "## LLM 분석" 미포함, `sendToOperator`는 정상 호출
  - analysisService가 일반 `RuntimeException`을 던질 때 → 동일 동작
  - 기존 `EmailSendException` 흡수 케이스 유지(분석 성공 가정으로 mocking)

### 5.2 빌드/테스트 명령
```powershell
.\gradlew.bat :pythia:test --tests "com.example.pythia.alert.service.*"
```

### 5.3 수동 검증 (선택)
- 본 Task는 통합/엔드투엔드 검증을 요구하지 않음("이후 구현 Task에서 바로 사용 가능"). 단위 테스트 통과로 수용 기준 충족.

## 6. 트레이드오프

| 결정 | 채택 | 대안 | 사유 |
|---|---|---|---|
| 데이터 소스 | DB에서 10분 윈도우 조회 | 위반 시점 단일 스냅샷만 사용 | LLM이 추세/변동성 기반 분석을 할 수 있도록 시계열 제공 (사용자 요구사항) |
| 윈도우 길이 | 10분 (요구사항) | 5분/30분 등 가변 | 요구사항에 명시. 가변 설정은 본 Task 범위 외 |
| 호출 위치 | `AlertNotifier.notify` 내부 | `ThresholdEvaluator.evaluateOne` 내부 | Notifier가 "알림 콘텐츠 조립" 단일 진입점 |
| 비동기화 | 미적용 (기존 @Async 위에서 동기) | 별도 Future로 병렬 | 본문 조립이 분석 결과에 의존. 병렬화 이점 없음 |
| 분석 실패 시 | 분석 생략 후 이메일 발송 | 이메일 자체 생략 | 운영자 알림 최우선 |
| Repository 조회 메서드 | Spring Data 메서드 네이밍 | 별도 `@Query` JPQL | 단순 조건. 네이밍 규칙으로 충분 |
| HTTP/Hikari 컬렉션 로딩 | `@EntityGraph(attributePaths = "points")` | JPQL fetch join + DISTINCT | EntityGraph가 간결. 결과 중복 위험은 부모 PK 기준 자연 정리 |
| 트랜잭션 경계 | Assembler에 `@Transactional(readOnly=true)` | AlertNotifier에 `@Transactional` | Assembler가 DB 접근 책임. 격리 |
| 카테고리 분기 위치 | Assembler 내부 switch(MetricKind) | MetricKind enum에 메서드 추가 | enum은 도메인 식별자 책임에 한정. Repository 의존성을 enum에 주입하지 않음 |
| timeSeries 메트릭 종류 | 위반 kind 1종만 | 동일 카테고리 전체 메트릭 함께 | Task 범위/프롬프트 단순화. 동일 kind만으로도 추세 분석 가능 |
| summaries 구성 | AVG + MAX (`SummaryAggregation` 재사용) | enum에 `MIN`/`THRESHOLD` 등 추가 | Task 018 산출물 무수정. 기존 enum으로 충분 |
| currentValue/threshold 전달 | Assembler 시그니처 제외 (YAGNI) | 포함하여 summaries에 추가 | 시계열 AVG/MAX로 LLM이 추세 파악 가능. 단일 시점값 중복 불필요 |
| 윈도우 상수 | Assembler 내부 private 상수 | `@ConfigurationProperties` 노출 | 본 Task 범위 외. 필요 시 후속 Task |
| LLM 호출 타임아웃 | 미설정 | ChatClient 측 설정 | 본 Task 범위 외 |
| LLM 응답 가공 | 원문 부착 | 길이 제한/요약 | Task 제약: `analyze` 반환값 그대로 사용 |

## 7. 작업 순서
1. 3개 Repository에 `findBy...CollectedAtBetween...` 메서드 추가 (HTTP/Hikari는 `@EntityGraph`)
2. `MetricAnalysisRequestAssembler` 신규 작성 + 단위 테스트 (Repository 모킹)
3. `AlertMessageFormatter.body(...)` 시그니처에 `analysis` 추가 + 단위 테스트
4. `AlertNotifier` 의존성/호출 흐름 수정 + 기존 테스트 갱신
5. `:pythia:test` 실행으로 전체 회귀 확인
6. plan → implementor → reviewer → (FAIL 시) fixer → reviewer 루프 (최대 3회) → plan 최종 보고
