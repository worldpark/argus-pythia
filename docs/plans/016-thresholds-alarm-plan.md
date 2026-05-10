# Task 016: Pyshia 임계값 평가 및 이메일 알림 트리거 — 구현 계획

## Context

Task 013에서 Pyshia가 `jvm.metrics.raw` / `http.metrics.raw` / `hikari.metrics.raw` Kafka 토픽 수신 능력을, Task 014에서 `docs/metrics/thresholds.md`로 임계값 정의를, Task 015에서 `EmailService.sendToOperator(subject, body)` 발송 능력을 각각 갖추었다. 그러나 세 Handler(`JvmMetricSnapshotHandler` / `HttpMetricSnapshotHandler` / `HikariMetricSnapshotHandler`)는 모두 `// hook: 후속 Task에서 임계값 평가 및 알림 처리 추가` 주석만 남긴 상태이고, 임계값 평가 로직과 EmailService 호출 트리거가 부재하다.

본 task는 (a) thresholds.md에 정의된 임계값 8종(JVM 5 / HTTP 1 / HIKARI 2)을 application.yml에 외부화하고, (b) 수신 스냅샷마다 임계값을 평가하며, (c) 윈도우(연속 N회)를 만족하는 시점에 `EmailService.sendToOperator`로 알림을 발송하는 능력을 도입한다. 운영자가 임계값 위반을 직접 인지하지 못하던 사각지대를 메우는 것이 목적.

사용자 결정사항 (확정):
- **알림 dedup 정책**: 윈도우(연속 N회) 만족 시 1회 발송 + 정상 복귀 시 리셋. severity 전이(warning ↔ critical) 시 재발송.
- **비동기 발송**: `@Async` 도입. Kafka listener 스레드가 SMTP 지연으로 막히지 않도록.
- **scope 확장 (옵션 B 채택)**: thresholds.md §7 "후속 권장"에 분류되어 있던 5종(`HEAP_OLD_GEN_USAGE`, `HTTP_ERROR_RATE`, `JVM_THREAD_PEAK`, `JVM_THREAD_DAEMON`, `HIKARI_USAGE_RATIO`)을 본 task에 추가. DTO 측 필드(`MemoryUsageDto.oldGenUsagePercent`, `HttpMetricSnapshotDto.errorRate`, `ThreadMetricDto.peakCount/daemonCount`, `HikariActiveDto.usageRatio`)는 모두 이미 존재하므로 DTO 변경 없음. thresholds.md §7 → §3-§5 본 정의로 승격하는 문서 보강이 본 task에 포함됨. 평가 메트릭 총 8종 → **13종**으로 확대.

---

## 1. 설계 방식 및 이유

### 방향: `alert` 단일 신규 패키지 + 메트릭별 평가 메서드 + in-memory 상태 + `@Async` 발송

| 결정 | 선택 | 이유 |
|------|------|------|
| 패키지 | **`com.example.pyshia.alert`** (top-level) | `email`, `kafka`, `common`과 같은 결. 임계값 평가 + 알림 라우팅을 한 패키지에 묶어 응집도 ↑ |
| 평가 진입점 | **각 Handler `handle()` 끝에서 `ThresholdEvaluator.evaluate(snapshot)` 직접 호출** | Handler에 hook 주석이 이미 있고, 이벤트 발행은 본 task scope 대비 과한 간접화. Handler→Evaluator 의존 1단계만 추가 |
| 평가 추상화 | **`ThresholdEvaluator` 단일 클래스 + 메트릭별 메서드(`evaluateJvm/Http/Hikari`)** | YAGNI. Strategy 패턴은 메트릭이 더 늘어날 때 도입. 현재 3종은 메서드 분리로 충분 |
| 임계값 외부화 | **`@ConfigurationProperties("pyshia.threshold")` → `ThresholdProperties` (record 트리)** | yml 키 변경 시 컴파일 타임 검출. 메트릭별 record 분리로 타입 안전성. `@Validated` + `@NotNull` |
| 윈도우 카운팅 | **in-memory `ConcurrentMap<ViolationKey, ViolationState>`** (`ViolationStateStore` 빈) | 본 task scope 내에서 가장 단순. 인스턴스 재시작 시 리셋 허용(첫 윈도우만 다시 채우면 됨). 분산/지속성은 후속 |
| dedup | **윈도우 만족 첫 스냅샷에서 1회 발송, 같은 severity가 유지되는 동안 silent. 정상 복귀 시 카운터+lastSent 리셋. severity 전이 시 재발송** | 사용자 결정. 매 위반 발송은 폭주, cooldown은 키/타이머 추가로 scope 확장 |
| 알림 발송 | **`AlertNotifier` 빈 + `@Async` 메서드 → `EmailService.sendToOperator` 위임** | `@EnableAsync` 1줄 + `@Async` 1줄. 사용자 결정. Handler/Evaluator는 fire-and-forget |
| 비교 연산자 | **메트릭별 enum/코드로 하드코딩** (JVM/HTTP는 `>`, Hikari는 `>=`) | thresholds.md에 고정. yml로 빼면 단조성 검증 복잡도만 증가. 본 task scope에 불필요 |
| 평가 단위 키 | **`ViolationKey(MetricKind, application, instance, sub)`** — sub는 endpoint(HTTP) / pool(Hikari) / null(JVM) | thresholds.md "평가 단위" 컬럼 그대로. 키 동등성으로 카운팅 그룹화 |
| 결측/실패 처리 | **Handler 단계의 기존 조기 return 유지** + `MetricStatus.MISSING`인 단일 메트릭은 Evaluator에서 skip | thresholds.md §2.4 그대로. 결측 자체는 위반/정상 어느 쪽도 카운트하지 않음 |
| 단조성 self-check | **부팅 시 `ThresholdProperties` 검증 — `warning < critical` 위반 시 `IllegalStateException` fail-fast** | thresholds.md §2.6 명시. 운영 안전. `EmailProperties`의 `@Validated` 패턴 재사용 |
| 알림 본문 | **plain text, 템플릿 헬퍼 1개** (`AlertMessageFormatter`) | "[CRITICAL] JVM CPU 사용률 위반: app=argus instance=node-1 value=92% threshold=85% (3회 연속)" 같은 한 줄 형식. 템플릿 엔진 도입 X |
| 예외 부모 | **`CustomException` 재사용** + `ThresholdConfigErrorCode` enum 1개(부팅 단조성 검증 실패용) | Task 015에서 이미 도입한 패턴 그대로. 평가 로직 자체는 예외 throw 안 함(silent skip + log) |
| 테스트 | **본 task에서 작성 필수** | Task 명세 "임계치 도달 성공 테스트" + "이메일 알림 성공 테스트" 명시. CLAUDE.md "테스트 없이 기능 추가 금지" 일치 |

### 비대상 (의식적 제외)
- thresholds.md §7 잔여 권장 항목(HTTP RPS 급락 baseline 비교 등) — DTO 또는 baseline 학습 인프라 선행 필요
- endpoint별 임계값 오버라이드 (모든 endpoint 동일 임계값 적용)
- cooldown(시간 기반) dedup
- 알림 재시도 / DLQ / outbox / dedup 영속화
- HTML/템플릿 엔진 본문, 첨부파일
- 다른 채널(Slack, PagerDuty 등)
- 기존 `EmailService`의 `JavaMailSender` 직접 의존을 Client 계층으로 분리 — 별도 task

---

## 2. 구성 요소

### 신규 파일

**`pyshia/src/main/resources/application.yml`** (수정)
- `pyshia.threshold.*` 트리 추가. 메트릭별 키 (총 13종):
  ```yaml
  pyshia:
    threshold:
      jvm:
        cpu-usage-percent:           { warning: 70,  critical: 85,  consecutive: 3 }
        heap-usage-percent:          { warning: 75,  critical: 90,  consecutive: 2 }
        old-gen-usage-percent:       { warning: 80,  critical: 90,  consecutive: 2 }   # 신규 (§7→정식)
        gc-avg-pause-seconds:        { warning: 0.2, critical: 0.5, consecutive: 1 }
        gc-count:                    { warning: 10,  critical: 30,  consecutive: 1 }
        thread-active-count:         { warning: 200, critical: 500, consecutive: 2 }
        thread-peak-count:           { warning: 300, critical: 800, consecutive: 1 }   # 신규 (§7→정식)
        thread-daemon-count:         { warning: 200, critical: 500, consecutive: 2 }   # 신규 (§7→정식)
      http:
        p99-response-seconds:        { warning: 1.0, critical: 3.0, consecutive: 2 }
        error-rate-percent:          { warning: 1.0, critical: 5.0, consecutive: 2 }   # 신규 (§7→정식, % 단위 가정)
      hikari:
        active-connections:          { warning: 8,   critical: 10,  consecutive: 2 }
        pending-connections:         { warning: 1,   critical: 5,   consecutive: 1 }
        usage-ratio-percent:         { warning: 80,  critical: 95,  consecutive: 2 }   # 신규 (§7→정식, % 단위 가정)
  ```
- 5종 신규 임계값은 **권장값**이며 thresholds.md 보강 시 근거와 함께 확정. 운영 데이터로 조정 가능.

**`com.example.pyshia.PyshiaApplication`** (수정)
- `@EnableAsync` 어노테이션 추가 (한 줄). 기존 `@ConfigurationPropertiesScan` 유지

**`com.example.pyshia.alert.config.ThresholdProperties`** — `@ConfigurationProperties("pyshia.threshold")`, `@Validated` record
- `JvmThresholds jvm`, `HttpThresholds http`, `HikariThresholds hikari` (각 record `@NotNull` 필드)
- `JvmThresholds`: `cpuUsagePercent`, `heapUsagePercent`, `oldGenUsagePercent`, `gcAvgPauseSeconds`, `gcCount`, `threadActiveCount`, `threadPeakCount`, `threadDaemonCount` (8 필드)
- `HttpThresholds`: `p99ResponseSeconds`, `errorRatePercent` (2 필드)
- `HikariThresholds`: `activeConnections`, `pendingConnections`, `usageRatioPercent` (3 필드)
- 각 필드는 `Limit` record
- `Limit(BigDecimal warning, BigDecimal critical, int consecutive)` — 부팅 시 `warning.compareTo(critical) < 0` 검증 (모든 신규 메트릭도 단조 증가). record 컴팩트 생성자에서 단조성 self-check, 위반 시 `ThresholdConfigException(ThresholdConfigErrorCode.NON_MONOTONIC)` throw

**`com.example.pyshia.alert.domain.MetricKind`** — enum (총 13종)
- 기존 8종: `JVM_CPU`, `JVM_HEAP`, `JVM_GC_PAUSE`, `JVM_GC_COUNT`, `JVM_THREAD_ACTIVE`, `HTTP_P99`, `HIKARI_ACTIVE`, `HIKARI_PENDING`
- 신규 5종: `JVM_HEAP_OLD_GEN`, `JVM_THREAD_PEAK`, `JVM_THREAD_DAEMON`, `HTTP_ERROR_RATE`, `HIKARI_USAGE_RATIO`
- 각 enum에 표시명(한글/영문) + 단위(%, 초, 개 등) 보유 — 알림 본문에 사용
- 비교 연산자(GT/GTE)도 enum 필드로 보유 권장: 기존 8종 그대로, 신규 5종 모두 GT

**`com.example.pyshia.alert.domain.Severity`** — enum
- `WARNING`, `CRITICAL`

**`com.example.pyshia.alert.domain.ViolationKey`** — record
- `MetricKind kind`, `String application`, `String instance`, `String sub` (endpoint/pool, JVM은 null)
- equals/hashCode는 record 자동 생성 사용

**`com.example.pyshia.alert.state.ViolationState`** — class (mutable, store 내부에서만 변형)
- `int warningCount`, `int criticalCount`, `Severity lastSentSeverity` (nullable)
- 상태 전이 메서드: `recordViolation(Severity)`, `reset()`, `markSent(Severity)`

**`com.example.pyshia.alert.state.ViolationStateStore`** — `@Component`
- `ConcurrentMap<ViolationKey, ViolationState>` 보유
- public API:
  - `boolean shouldSend(ViolationKey, Severity, int window)` — 카운트++ → window 만족 && lastSent != current면 true 반환 + `markSent` 처리
  - `void clear(ViolationKey)` — 정상 복귀 시 카운터/lastSent 리셋
- 키별 락 또는 `compute` 사용으로 동시성 안전

**`com.example.pyshia.alert.service.ThresholdEvaluator`** — `@Service`
- 의존: `ThresholdProperties`, `ViolationStateStore`, `AlertNotifier` (생성자 주입)
- public API:
  - `void evaluateJvm(JvmMetricSnapshotDto snapshot)` — cpu/heap/oldGen/gc-pause/gc-count/threadActive/threadPeak/threadDaemon **8개** 평가
  - `void evaluateHttp(HttpMetricSnapshotDto snapshot)` — p99 endpoint별 + errorRate endpoint별 평가 (`p99.points()`, `errorRate.points()` 순회)
  - `void evaluateHikari(HikariMetricSnapshotDto snapshot)` — active pool별 + pending pool별 + usageRatio pool별 평가 (`active.points()`, `pending.points()`, `active.usageRatio()` 순회)
- 각 메트릭 평가 흐름 (private 헬퍼 `evaluateOne(...)`로 공통화):
  1. 값 추출. `MetricStatus.MISSING` / null이면 skip return
  2. critical 임계 비교(연산자는 `MetricKind.operator()`에서 결정 — 기존 8종 그대로, 신규 5종 GT) → 위반 시 `Severity.CRITICAL`, 아니면 warning 임계 비교 → 위반 시 `WARNING`, 둘 다 아니면 정상
  3. 위반: `ViolationStateStore.shouldSend(key, severity, window)`가 true면 `AlertNotifier.notify(...)`
  4. 정상: `ViolationStateStore.clear(key)`
- HTTP errorRate / Hikari usageRatio는 단위 가정에 의존 (§6 트레이드오프 참조). yml 키 이름 (`error-rate-percent`, `usage-ratio-percent`)에 단위를 박아 가정 명시. 실제 단위 불일치 시 첫 운영에서 false positive/negative로 발견되면 yml 임계값 조정 또는 assembler 단위 변환 보강.

**`com.example.pyshia.alert.service.AlertNotifier`** — `@Service`
- 의존: `EmailService`, `AlertMessageFormatter`
- `@Async` 메서드 `void notify(MetricKind, Severity, ViolationKey, BigDecimal value, BigDecimal threshold, int consecutive)`
- 본문 포매팅 → `emailService.sendToOperator(subject, body)` 호출
- `EmailSendException`은 catch + log.error만 (재throw 안 함 — fire-and-forget)

**`com.example.pyshia.alert.service.AlertMessageFormatter`** — `@Component`
- `String subject(MetricKind, Severity, ViolationKey)` — 예: `"[CRITICAL] JVM CPU 사용률 위반 (argus / node-1)"`
- `String body(MetricKind, Severity, ViolationKey, BigDecimal value, BigDecimal threshold, int consecutive)` — 한 줄 또는 키-값 나열

**`com.example.pyshia.alert.exception.ThresholdConfigErrorCode`** — enum implements `ErrorCode`
- `NON_MONOTONIC("THRESHOLD_001", "warning must be less than critical")`
- (필요 시) `MISSING_THRESHOLD("THRESHOLD_002", "...")` — yml 키 누락은 `@NotNull`이 우선 잡으므로 보조

**`com.example.pyshia.alert.exception.ThresholdConfigException`** — extends `CustomException`

### 수정 대상 파일

**`docs/metrics/thresholds.md`** (보강)
- §3 JVM 표에 `JVM_HEAP_OLD_GEN`, `JVM_THREAD_PEAK`, `JVM_THREAD_DAEMON` 3행 추가 (DTO 필드: `memory.oldGenUsagePercent`, `thread.peakCount`, `thread.daemonCount`)
- §4 HTTP 표에 `HTTP_ERROR_RATE` 1행 추가 (DTO 필드: `errorRate.points[].value`, 평가 단위: application+instance+endpoint, 단위 가정: %)
- §5 HIKARI 표에 `HIKARI_USAGE_RATIO` 1행 추가 (DTO 필드: `active.usageRatio[].value`, 평가 단위: application+instance+pool, 단위 가정: %)
- §6 매핑 부록에 5행 추가 (PromQL ↔ DTO ↔ 임계값)
- §7 후속 권장 표에서 5종 항목 제거 (또는 "Task 016에서 정식 정의로 승격" 메모로 strikethrough). RPS 급락 항목만 잔여.
- §8 Changelog에 "2026-05-03: §7 5종을 정식 정의로 승격 (Task 016)" 추가
- 각 신규 행의 임계값/근거는 application.yml 권장값과 일치
- §2.5 단위 주의에 errorRate(%)와 usageRatio(%) 단위 가정 추가 — assembler 변환 가정

**`com.example.pyshia.kafka.consumer.JvmMetricSnapshotHandler`**
- `ThresholdEvaluator` 생성자 주입
- hook 주석 자리에 `evaluator.evaluateJvm(snapshot)` 호출 한 줄 추가

**`com.example.pyshia.kafka.consumer.HttpMetricSnapshotHandler`**
- 동일 패턴, `evaluator.evaluateHttp(snapshot)`

**`com.example.pyshia.kafka.consumer.HikariMetricSnapshotHandler`**
- 동일 패턴, `evaluator.evaluateHikari(snapshot)`

### 수정하지 않는 파일 (제약/안전)
- `EmailService` / `EmailProperties` / `EmailRequest` / `EmailSendException` (Task 015 산출물 그대로 사용)
- 모든 `*MetricSnapshotDto` 및 하위 DTO들
- 기존 Consumer 클래스 (Handler만 수정)
- `docs/metrics/thresholds.md` (정의 문서, 본 task 범위 외)

### 신규 테스트 파일 (CLAUDE.md "테스트 없이 기능 추가 금지" 충족)

- `pyshia/src/test/java/.../alert/config/ThresholdPropertiesTest.java`
  - yml 바인딩 round-trip
  - 단조성 검증 fail-fast (warning >= critical 케이스에서 컨텍스트 로딩 실패)
- `.../alert/state/ViolationStateStoreTest.java`
  - count 누적 → window 만족 시 `shouldSend` true 반환
  - 같은 severity 재호출은 false (lastSent 가드)
  - severity 승격(warning→critical) 시 다시 true
  - `clear` 후 재호출 시 카운터 리셋 확인
- `.../alert/service/ThresholdEvaluatorTest.java` (Mockito, `@Mock AlertNotifier`)
  - JVM 8종 / HTTP 2종 / Hikari 3종 각 메트릭 정상값 → notify 호출 안 됨
  - warning 임계 1회 위반 (window=2) → notify 호출 안 됨
  - warning 임계 2회 연속 → notify 1회 호출 (`ArgumentCaptor`로 severity/value/threshold 검증)
  - critical 즉시 위반 (window=1) → notify CRITICAL 호출
  - 위반 후 정상 복귀 → store clear 호출 (verify)
  - `MetricStatus.MISSING` → skip
  - HTTP는 endpoint별 (p99, errorRate 모두), Hikari는 pool별 (active/pending/usageRatio 모두)로 키 분리 동작 (서로 영향 없음)
  - 신규 5종 각각에 대해 정상/warning/critical 1세트씩 최소 케이스 추가 (총 케이스 수 증가)
- `.../alert/service/AlertNotifierTest.java` (Mockito, `@Mock EmailService`)
  - notify 호출 시 `emailService.sendToOperator(subject, body)` 호출되는지 `ArgumentCaptor`로 subject/body 패턴 검증
  - `EmailSendException` 발생 시 throw하지 않고 log만 (`assertThatNoException`)
  - 비동기 동작은 테스트 시 `SyncTaskExecutor` 또는 `@Async` 미적용으로 동기 검증 (별도 `@TestConfiguration`)
- `.../kafka/consumer/JvmMetricSnapshotHandlerTest.java`, `HttpMetricSnapshotHandlerTest.java`, `HikariMetricSnapshotHandlerTest.java` (보강)
  - 기존 ListAppender 검증은 유지
  - `@Mock ThresholdEvaluator` 추가, 정상 스냅샷에서 `evaluator.evaluateXxx(snapshot)` 호출 verify
  - FAILED/null 스냅샷에서는 evaluator 미호출 verify

---

## 3. 데이터 흐름

```
Kafka topic (jvm.metrics.raw)
   │
   ▼
JvmMetricSnapshotConsumer  (@KafkaListener)
   │
   ▼
JvmMetricSnapshotHandler.handle(snapshot)
   │
   ├─ status == FAILED || cpu == null  →  log.warn + return
   │
   └─▶ thresholdEvaluator.evaluateJvm(snapshot)
           │
           ├─ evaluateOne(JVM_CPU, key, snapshot.cpu().usagePercent(), props.jvm().cpuUsagePercent())
           │     │
           │     ├─ value == null || MetricStatus.MISSING  →  skip
           │     ├─ value > critical.warning?              →  severity = CRITICAL or WARNING or null
           │     │
           │     ├─ severity == null (정상)  →  store.clear(key)
           │     │
           │     └─ severity != null (위반)
           │           │
           │           └─ store.shouldSend(key, severity, limit.consecutive())
           │                 │
           │                 ├─ count++ → count >= window && lastSent != severity  →  return true (markSent)
           │                 └─ otherwise                                          →  return false
           │                       │
           │                       └─ true 시 →  alertNotifier.notify(JVM_CPU, severity, key, value, threshold, consecutive)
           │
           ├─ evaluateOne(JVM_HEAP,         snapshot.memory().heapUsagePercent(),    props.jvm().heapUsagePercent())
           ├─ evaluateOne(JVM_HEAP_OLD_GEN, snapshot.memory().oldGenUsagePercent(),  props.jvm().oldGenUsagePercent())   # 신규
           ├─ evaluateOne(JVM_GC_PAUSE,     snapshot.gc().avgDurationSeconds(),      props.jvm().gcAvgPauseSeconds())
           ├─ evaluateOne(JVM_GC_COUNT,     snapshot.gc().count(),                   props.jvm().gcCount())
           ├─ evaluateOne(JVM_THREAD_ACTIVE, snapshot.thread().activeCount(),        props.jvm().threadActiveCount())
           ├─ evaluateOne(JVM_THREAD_PEAK,   snapshot.thread().peakCount(),          props.jvm().threadPeakCount())     # 신규
           └─ evaluateOne(JVM_THREAD_DAEMON, snapshot.thread().daemonCount(),        props.jvm().threadDaemonCount())   # 신규

(HTTP 흐름:
   p99.points()       순회 → endpoint별 ViolationKey(HTTP_P99, app, instance, endpoint)
   errorRate.points() 순회 → endpoint별 ViolationKey(HTTP_ERROR_RATE, app, instance, endpoint)   # 신규)

(Hikari 흐름:
   active.points()      순회 → pool별 ViolationKey(HIKARI_ACTIVE, app, instance, pool)
   pending.points()     순회 → pool별 ViolationKey(HIKARI_PENDING, app, instance, pool)
   active.usageRatio()  순회 → pool별 ViolationKey(HIKARI_USAGE_RATIO, app, instance, pool)      # 신규)


AlertNotifier.notify(...)   ← @Async (별도 스레드 풀에서 실행)
   │
   ├─ subject = formatter.subject(kind, severity, key)
   ├─ body    = formatter.body(kind, severity, key, value, threshold, consecutive)
   │
   └─▶ emailService.sendToOperator(subject, body)
           │
           ├─ 성공: log.debug
           └─ 실패: EmailSendException → AlertNotifier가 catch → log.error (재throw X)
```

---

## 4. 예외 처리 전략

| 단계 | 상황 | 처리 |
|------|------|------|
| 부팅 | `pyshia.threshold.*` 키 누락 | `ThresholdProperties` `@NotNull` → 컨텍스트 로딩 실패 (의도) |
| 부팅 | `warning >= critical` (단조성 위반) | record 컴팩트 생성자에서 검증 → `ThresholdConfigException(NON_MONOTONIC)` throw → 컨텍스트 로딩 실패 (fail-fast) |
| 평가 | snapshot 단일 메트릭 값 null / MISSING | 해당 메트릭만 skip, log.debug |
| 평가 | snapshot.application/instance null | log.warn + 해당 평가만 skip (키 구성 불가) |
| 평가 | unexpected RuntimeException (NPE 등) | catch → log.error + 다음 메트릭 평가 계속 (한 메트릭의 버그가 다른 메트릭 평가를 막지 않도록). 핸들러까지 전파하지 않음 |
| 발송 | `EmailSendException` (SMTP_FAILURE 등) | `AlertNotifier`가 catch → log.error만, 재throw X. 비동기 fire-and-forget이므로 호출자가 처리할 방법 없음 |
| 발송 | `@Async` 스레드 풀 포화 | Spring 기본 동작(rejected) → log.error. 풀 튜닝은 후속 |

핵심 원칙:
- **평가 단계는 Kafka listener를 막지 않는다**: Evaluator 내부 예외는 로컬에서 catch + log, 절대 propagate X. Listener acknowledgment 실패는 메시지 재처리 폭주를 유발.
- **부팅 시점 검증은 fail-fast**: 잘못된 임계값 설정으로 잘못된 알림이 나가는 것보다 부팅 실패가 안전.
- **알림 발송 실패는 silent log**: 알림이 못 가는 것보다 시스템이 멈추는 게 더 큰 사고. 발송 실패 자체에 대한 메타알림은 후속(자기 자신을 알릴 수 없으므로 대안 채널 필요).

---

## 5. 검증 방법

### 빌드 / 단위 테스트
- `./gradlew :pyshia:build` → BUILD SUCCESSFUL
- 위 §2 신규 테스트 파일 5종 + 보강 3종 모두 통과
- 단조성 검증 테스트는 `@SpringBootTest(properties = "pyshia.threshold.jvm.cpu-usage-percent.warning=90")` 같은 잘못된 값으로 컨텍스트 로딩이 실패함을 `assertThrows`로 확인

### 컨텍스트 로딩 검증
- 기존 `PyshiaApplicationTests` 통과 (정상 yml 값으로)
- `ThresholdProperties`가 `@ConfigurationPropertiesScan`을 통해 빈 등록되는지 확인

### 통합 검증 (선택, 본 task 필수 아님)
- 임계값 초과하는 가짜 `JvmMetricSnapshotDto`를 직접 Handler.handle 호출 → `JavaMailSender` mock으로 send 호출 verify
- 실제 SMTP까지의 round-trip은 Task 015의 수동 검증 절차 재사용 (앱 비밀번호 환경변수 + 운영자 메일 수신 확인)

### 정합성 검증
- thresholds.md §3~§5 표의 모든 행(13종)이 application.yml에 있고, 값/연산자가 일치하는지 코드 리뷰 시 1:1 대조
- thresholds.md §2.4 결측 처리 정책이 `evaluateOne`에 반영되었는지 테스트로 검증
- thresholds.md §7에서 5종이 제거되고 §3-§5로 승격되었는지 확인. Changelog 갱신 확인.

### 단위 가정 검증 (신규)
- HTTP errorRate / Hikari usageRatio가 실제로 % 단위(0~100)로 도달하는지, 또는 비율(0~1)인지 확인 — Argus assembler 측 PromQL 처리 코드 확인 또는 실제 메시지 샘플로 검증.
- 단위 가정이 어긋나면 ThresholdEvaluatorTest에서 권장 임계값으로 정상값/위반값을 만들 때 케이스가 비현실적으로 되므로 자연스럽게 발견됨. 이를 검증할 케이스 1건 필수.

---

## 6. 트레이드오프

1. **in-memory 상태 (분산/지속성 없음)**: 인스턴스 재시작 시 카운터/lastSent 모두 리셋되어 첫 윈도우(연속 N회)를 다시 채워야 알림이 나간다. 즉 재시작 직후 1~2건의 위반이 묻힐 수 있음. 그러나 본 task scope에서 Redis/DB 도입은 과함. Pyshia가 단일 인스턴스로 운영되는 한 문제 없고, 다중 인스턴스 시 같은 키가 중복 발송될 수 있다 — 후속에서 영속화 또는 단일 leader 도입.

2. **dedup이 "severity 전이"로만 동작**: warning 상태로 며칠 유지되면 사람이 잊을 수 있음. cooldown(주기적 재알림) 미도입으로 "한 번 알림 후 묵음" 위험. 그러나 사용자 결정이 폭주 방지 우선이고, 주기적 재알림은 cooldown 키/타이머 + 별도 스케줄러가 필요해 scope 확장. 후속에서 추가.

3. **endpoint별 임계값 모두 동일**: thresholds.md §4.1 그대로. 헬스체크 endpoint와 결제 API에 같은 1초 임계값을 적용. false positive 위험은 endpoint별 오버라이드 후속 task에서 해결.

4. **Hikari 절대값 임계의 풀 크기 의존성**: thresholds.md §5.1에 명시된 한계. `maximumPoolSize=10` 가정. 풀 크기를 다르게 설정한 대상 프로젝트는 false positive/negative 가능. §7 후속 권장(active/max 비율) 도입 시 우선 갱신.

5. **`@Async` 스레드 풀 미튜닝**: Spring 기본 `SimpleAsyncTaskExecutor`는 무제한 스레드 생성 → SMTP 폭주 시 OOM 위험. `TaskExecutor` 빈 명시적 정의(예: `ThreadPoolTaskExecutor` corePool=2, maxPool=4, queue=50)가 안전. 본 plan에서는 명시 정의를 §2에 포함하지 않았으나 실구현 시 추가 권장. 사용자 합의 후 plan에 명시 가능.

6. **단일 `ThresholdEvaluator` 클래스 (메트릭 8종을 한 클래스에)**: 메서드 3개로 분리되어 있으나 테스트 클래스가 비대해짐. JvmEvaluator/HttpEvaluator/HikariEvaluator로 분리하면 테스트 단위가 명확하나 빈 3개 + Notifier 의존 중복. 현재 메트릭 수에서는 단일 클래스가 합리적, 메트릭이 더 늘면 분리.

7. **`AlertMessageFormatter`를 별도 클래스로 분리 vs `AlertNotifier` 내부 메서드**: 분리하면 포매팅 단위 테스트가 깔끔, 인라인이면 클래스 1개 절약. 알림 본문 형식이 향후 변경 빈도가 높을 수 있어 분리 선호.

8. **EmailService가 여전히 `JavaMailSender`를 직접 호출 (Client 계층 부재)**: 직전 회차 코드 리뷰에서 지적된 구조 문제. 본 task scope("임계값 평가 + 알림 트리거")를 벗어나므로 별도 task로 분리. 본 task에서는 EmailService를 그대로 사용.

9. **신규 5종 임계값은 권장값**: thresholds.md §3-§5의 기존 8종은 Task 014에서 근거와 함께 확정된 값이나, 신규 5종은 본 task plan에서 일반 가이드라인 기반으로 제안한 값(OldGen 80/90, ThreadPeak 300/800, ThreadDaemon 200/500, ErrorRate 1%/5%, UsageRatio 80%/95%). 운영 데이터 누적 후 조정 필요. thresholds.md 보강 시 근거 컬럼에 "초기 권장값, 운영 데이터로 조정"을 명시.

10. **HTTP errorRate / Hikari usageRatio 단위 가정**: PromQL 원시 값은 0~1 비율인 경우가 많으나, thresholds.md §2.5의 CPU 처리 패턴(assembler에서 ×100 변환)을 errorRate / usageRatio에도 동일하게 가정. yml 키 이름에 `-percent`를 박아 가정 명시. 가정이 틀리면 알림이 실제로 작동하지 않거나(0~1 값에 % 임계값 적용 시 영원히 미달) 폭주(반대 방향)할 수 있음 → §5 검증에 단위 검증 테스트 1건 필수. assembler 측 단위가 다른 것으로 확인되면 yml 임계값 또는 코드에서 단위 변환 보강.

11. **`JVM_THREAD_PEAK`의 누적성 한계**: peakCount는 JVM 시작 이후 최대 활성 스레드 수의 누적값. 한 번 burst 발생 후 영구히 높음(JVM 재시작 전까지 감소 안 함) → 정상 복귀가 자연스럽게 일어나지 않아 한 번 알림 후 영구 silent. 단일 윈도우(consecutive=1)로 시작 burst를 즉시 알리는 것에 의의를 두고, redundancy(active와 중복) 위험은 인정. 의미 있는 사용을 원하면 후속에서 `delta(peak)` 같은 PromQL/assembler 측 처리 필요.

12. **`JVM_THREAD_DAEMON` 임계값의 환경 의존성**: daemon 스레드 수는 사용 라이브러리(Tomcat, Netty, Kafka client 등)에 크게 의존. 일반 Spring Boot 50~100 가정이나 대상 프로젝트 구성 차이로 false positive 가능. 운영 baseline 학습 후 조정 가능성 큼.

13. **scope 확장으로 본 task 검토 면적 증가**: 메트릭 8 → 13으로 늘어나면서 yml 키 5개, MetricKind 5개, ThresholdProperties 필드 5개, ThresholdEvaluator 평가 5세트, 테스트 케이스 다수 증가. thresholds.md 문서 보강도 함께. plan §1 "하나의 Task는 하나의 기능만 구현" 규칙과의 긴장은 있으나 "임계값 평가" 라는 단일 기능의 적용 메트릭이 늘어난 것으로 해석 가능.

---

## 핵심 파일 경로

신규 (절대 경로):
- `C:\side_project\pyshia\src\main\java\com\example\pyshia\alert\config\ThresholdProperties.java`
- `C:\side_project\pyshia\src\main\java\com\example\pyshia\alert\domain\MetricKind.java`
- `C:\side_project\pyshia\src\main\java\com\example\pyshia\alert\domain\Severity.java`
- `C:\side_project\pyshia\src\main\java\com\example\pyshia\alert\domain\ViolationKey.java`
- `C:\side_project\pyshia\src\main\java\com\example\pyshia\alert\state\ViolationState.java`
- `C:\side_project\pyshia\src\main\java\com\example\pyshia\alert\state\ViolationStateStore.java`
- `C:\side_project\pyshia\src\main\java\com\example\pyshia\alert\service\ThresholdEvaluator.java`
- `C:\side_project\pyshia\src\main\java\com\example\pyshia\alert\service\AlertNotifier.java`
- `C:\side_project\pyshia\src\main\java\com\example\pyshia\alert\service\AlertMessageFormatter.java`
- `C:\side_project\pyshia\src\main\java\com\example\pyshia\alert\exception\ThresholdConfigErrorCode.java`
- `C:\side_project\pyshia\src\main\java\com\example\pyshia\alert\exception\ThresholdConfigException.java`
- `C:\side_project\pyshia\src\test\java\com\example\pyshia\alert\config\ThresholdPropertiesTest.java`
- `C:\side_project\pyshia\src\test\java\com\example\pyshia\alert\state\ViolationStateStoreTest.java`
- `C:\side_project\pyshia\src\test\java\com\example\pyshia\alert\service\ThresholdEvaluatorTest.java`
- `C:\side_project\pyshia\src\test\java\com\example\pyshia\alert\service\AlertNotifierTest.java`

수정 (절대 경로):
- `C:\side_project\docs\metrics\thresholds.md` — §3/§4/§5에 신규 5종 행 추가, §6 매핑 부록 5행 추가, §7에서 5종 제거(strikethrough), §8 Changelog 갱신, §2.5에 errorRate/usageRatio 단위 가정 추가
- `C:\side_project\pyshia\src\main\java\com\example\pyshia\PyshiaApplication.java` — `@EnableAsync` 한 줄 추가
- `C:\side_project\pyshia\src\main\resources\application.yml` — `pyshia.threshold.*` 트리 추가 (13종)
- `C:\side_project\pyshia\src\main\java\com\example\pyshia\kafka\consumer\JvmMetricSnapshotHandler.java` — Evaluator 주입 + 호출 한 줄
- `C:\side_project\pyshia\src\main\java\com\example\pyshia\kafka\consumer\HttpMetricSnapshotHandler.java` — 동일
- `C:\side_project\pyshia\src\main\java\com\example\pyshia\kafka\consumer\HikariMetricSnapshotHandler.java` — 동일
- `C:\side_project\pyshia\src\test\java\com\example\pyshia\kafka\consumer\JvmMetricSnapshotHandlerTest.java` — Evaluator mock + verify 보강
- `C:\side_project\pyshia\src\test\java\com\example\pyshia\kafka\consumer\HttpMetricSnapshotHandlerTest.java` — 동일
- `C:\side_project\pyshia\src\test\java\com\example\pyshia\kafka\consumer\HikariMetricSnapshotHandlerTest.java` — 동일

참조 (수정 없음):
- `C:\side_project\docs\metrics\thresholds.md` — 임계값 정의 출처
- `C:\side_project\docs\metrics\promqls.md` — DTO 필드 ↔ PromQL 매핑
- `C:\side_project\docs\tasks\016-thresholds-alarm.md` — 본 task 명세
- `C:\side_project\pyshia\src\main\java\com\example\pyshia\email\EmailService.java` — `sendToOperator(subject, body)` 사용
- `C:\side_project\pyshia\src\main\java\com\example\pyshia\common\exception\CustomException.java`, `ErrorCode.java`
- 모든 `*MetricSnapshotDto` 및 하위 DTO

---

## 후속 작업 (본 task 범위 외)
- `EmailService`의 `JavaMailSender` 직접 의존을 `SmtpClient`로 분리 (직전 회차 코드 리뷰 지적사항)
- 알림 cooldown(주기적 재알림) + dedup 영속화(Redis/DB)
- endpoint별 / pool별 임계값 오버라이드
- thresholds.md §7 잔여 권장 메트릭(HTTP RPS 급락 baseline 비교) — baseline 학습 인프라 선행 필요
- 다른 알림 채널(Slack, PagerDuty)
- `@Async` `TaskExecutor` 빈 명시적 튜닝
- 결측(MISSING) 자체에 대한 알림 정책
- 알림 발송 실패에 대한 대안 채널 메타알림
- 신규 5종 임계값(특히 ThreadPeak/ThreadDaemon, ErrorRate, UsageRatio)을 운영 baseline 데이터 기반으로 재조정
- `JVM_THREAD_PEAK`의 누적성 한계 해결 — `delta(peak)` 기반 재정의 또는 메트릭 자체 제거 검토
