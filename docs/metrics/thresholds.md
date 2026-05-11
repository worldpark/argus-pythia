# 메트릭 임계값 정의 (JVM / HTTP / HIKARI)

본 문서는 Pythia가 수신하는 `*MetricSnapshotDto`의 필드를 기준으로, 대상 프로젝트(Argus 등)의 위험 신호를 판정하기 위한 임계값을 정의한다.

- 산출 task: Task 014
- 적용 task: 후속 ThresholdEvaluator / 알림 라우팅 task
- 본 문서는 **정의**만 포함한다. 평가 엔진 / 알림 채널 / 임계값 외부화는 본 문서 범위 외.

---

## 1. 개요 / 적용 범위

| 항목 | 값 |
|------|---|
| 적용 도메인 | JVM (`JvmMetricSnapshotDto`), HTTP (`HttpMetricSnapshotDto`), HIKARI (`HikariMetricSnapshotDto`) |
| 평가 주체 | Pythia (후속 task의 ThresholdEvaluator) |
| 평가 입력 | Kafka 토픽 `*.metrics.raw` 수신 스냅샷 |
| 평가 단위 | 메트릭별 표의 "평가 단위" 컬럼 참조 (`application+instance` / `+endpoint` / `+pool`) |
| 단계 | `warning`, `critical` 2단계 |

---

## 2. 공통 규약

### 2.1 단계 정의
- **warning** — 즉시 장애는 아니나 추세 추적 / 경계가 필요한 상태
- **critical** — 사용자 영향 발생 가능성이 높음. 즉시 조치 후보

### 2.2 평가 윈도우 표기
- **단일** — 단일 스냅샷 1건이 임계 위반이면 위반으로 판정
- **연속 N회** — 같은 (애플리케이션·인스턴스·필요 시 endpoint/pool) 키로 직전 N개 스냅샷이 모두 위반이면 위반 판정. 노이즈/스파이크 억제용

> 시간 기반 윈도우(예: 5분 평균)는 PromQL 측에서 이미 `avg_over_time(...[1m])` 등으로 처리되어 스냅샷에 도달하므로, 본 layer에서는 스냅샷 카운트 단위만 사용한다.

### 2.3 비교 연산자
- `>`, `>=`, `<`, `<=` 만 사용. "초과"·"이상" 같은 자연어 표기 금지.
- 큰 값이 위험인 메트릭: `warning < critical` (단조 증가)
- 작은 값이 위험인 메트릭: `warning > critical` (단조 감소). 본 1차 정의에는 해당 항목 없음(RPS 급락은 후속 권장).

### 2.4 결측 / 실패 스냅샷 처리
- `SnapshotStatus.FAILED` 스냅샷은 Handler 단계에서 조기 return → 본 임계값 평가 대상 아님.
- 메트릭 `MetricStatus.MISSING` (또는 값이 null)인 단일 메트릭은 **해당 메트릭만 평가 보류(skip)**. 위반/정상 어느 쪽으로도 카운트하지 않음. 결측 자체에 대한 알림은 후속 task의 정책.

### 2.5 단위 주의
- PromQL `process_cpu_usage`는 0~1 비율을 반환한다. DTO 필드 `CpuUsageDto.usagePercent`는 명칭상 0~100 범위를 의미하므로, **Argus assembler에서 ×100 변환을 보장한다는 가정** 위에서 본 표는 0~100 단위로 임계값을 적는다. 후속 평가 코드 도입 시 단위 검증 테스트 1건 필수.
- `HEAP_USAGE` / `HEAP_OLD_GEN_USAGE`는 PromQL이 `... * 100`을 포함해 0~100 % 단위로 반환. 본 표도 % 단위.
- HTTP P99는 PromQL `histogram_quantile(... http_server_requests_seconds_bucket ...)`로 **초(second)** 단위. 본 표도 초 단위로 표기.
- **`HTTP_ERROR_RATE`** (`errorRate.points[].value`)는 PromQL이 `sum(...{status=~"5.."}) / clamp_min(sum(...), 1)` 으로 **0~1 비율 그대로 반환** (×100 변환 없음). 본 표도 비율 단위로 표기 (예: `0.01` = 1%, `0.05` = 5%). yml 외부화 시 키 이름 / 값 모두 비율 단위 일관 유지 필수.
- **`HIKARI_USAGE_RATIO`** (`active.usageRatio[].value`)도 동일하게 **0~1 비율 그대로 반환** (`active / clamp_min(max, 1)`). 본 표도 비율 단위로 표기 (예: `0.8` = 80%).
- Hikari `active`/`pending`은 절대 커넥션 수.
- JVM `thread.activeCount` / `peakCount` / `daemonCount`는 절대 개수.

### 2.6 임계값 단조성 self-check
- 모든 행에서 (단조 증가 메트릭) `warning < critical` 또는 (단조 감소 메트릭) `warning > critical` 성립 여부를 표 작성 시 확인. 후속 평가 코드도 부팅 시 동일 검증을 갖도록 명세.

---

## 3. JVM 임계값

DTO: `JvmMetricSnapshotDto`

| 메트릭 | DTO 필드 | 평가 단위 | 단위 | 연산자 | warning | critical | 윈도우 | 근거 |
|--------|----------|-----------|------|--------|---------|----------|--------|------|
| CPU 사용률 | `cpu.usagePercent` | application + instance | % | `>` | 70 | 85 | 연속 3회 | 일반 Java 앱 헤드룸 권장(70% 경계, 85% 위험). 짧은 spike 억제 위해 연속 3회 |
| Heap 사용률 | `memory.heapUsagePercent` | application + instance | % | `>` | 75 | 90 | 연속 2회 | GC가 회수 가능한 영역 고려. 90% 이상은 OOM/장기 GC 직전 |
| Heap Old Gen 사용률 | `memory.oldGenUsagePercent` | application + instance | % | `>` | 80 | 90 | 연속 2회 | Old Gen은 Young 대비 회수 빈도가 낮아 90% 이상은 Full GC/STW 직전. heap 전체보다 약간 빡빡한 80% 경계 권장 |
| GC 평균 pause | `gc.avgDurationSeconds` | application + instance | 초 | `>` | 0.2 | 0.5 | 단일 | G1 기본 SLA 200ms 권장, 500ms는 사용자 체감 시작 |
| GC 빈도 | `gc.count` | application + instance | count/분 | `>` | 10 | 30 | 단일 | PromQL이 1분 increase로 집계 → 단일 스냅샷이 곧 1분당 빈도. 10회/분부터 메모리 압박 의심 |
| 활성 스레드 수 | `thread.activeCount` | application + instance | 개 | `>` | 200 | 500 | 연속 2회 | 일반 Spring Boot 앱 정상 50~150 범위. 500 초과는 thread leak / blocked 풀 의심 |
| Peak 스레드 수 | `thread.peakCount` | application + instance | 개 | `>` | 300 | 800 | 단일 | JVM 시작 이후 누적 최댓값(단조 증가). 시작 burst 감지용. 한 번 알림 후 JVM 재시작 전까지 영구 silent — 한계는 §3.1 참조 |
| Daemon 스레드 수 | `thread.daemonCount` | application + instance | 개 | `>` | 200 | 500 | 연속 2회 | 일반 Spring Boot 정상 50~150 범위. 사용 라이브러리(Tomcat/Netty/Kafka client)에 따라 baseline 차이 큼 — 운영 데이터로 조정 권장 |

### 3.1 한계
- `Peak 스레드 수`(`thread.peakCount`)는 JVM 시작 이후 누적 최댓값이라 한 번 burst 발생 시 영구히 임계 초과 상태로 유지된다. dedup 정책(severity 전이 시만 재발송)과 결합되면 한 번 알림 후 JVM 재시작 전까지 silent. 시작 burst 감지에는 의미 있으나 지속 모니터링에는 부적합 — 후속에서 `delta(peak)` 또는 `increase(peak)` 기반 PromQL 재정의 검토.
- `Daemon 스레드 수`는 사용 라이브러리(Tomcat/Netty/Kafka client/HikariCP 등) 구성에 크게 의존하여 환경별 baseline 차이가 크다. 본 표 권장값은 일반 Spring Boot Web 가정. 다른 스택 적용 시 false positive/negative 가능 — 운영 baseline 학습 후 조정 필수.

---

## 4. HTTP 임계값

DTO: `HttpMetricSnapshotDto`

| 메트릭 | DTO 필드 | 평가 단위 | 단위 | 연산자 | warning | critical | 윈도우 | 근거 |
|--------|----------|-----------|------|--------|---------|----------|--------|------|
| P99 응답 시간 | `p99.points[].value` | application + instance + endpoint | 초 | `>` | 1.0 | 3.0 | 연속 2회 | 일반 웹 API SLO 1s 경계. 3s는 사용자 이탈 권역. endpoint별 폭주 위험은 §6 참조 |
| HTTP 5xx 비율 | `errorRate.points[].value` | application + instance + endpoint | 비율 (0~1) | `>` | 0.01 | 0.05 | 연속 2회 | 일반 SLO 99% / 95% 기준. PromQL이 비율 그대로 반환(×100 변환 없음) — §2.5 단위 주의 참조. endpoint별 평가는 §4.1 동일 정책 |

> RPS(`rps.points[].value`)는 **본 1차 정의에서 임계값을 두지 않는다**. 절대값 임계는 트래픽 변동에 매우 민감하고, 의미 있는 신호는 "급락" 판정인데 baseline 비교 메커니즘이 부재. 후속 권장 §7로 이동.

### 4.1 endpoint 단위 적용 정책 (1차)
- 본 1차 정의는 **모든 endpoint에 동일 임계값** 적용.
- 현실에서는 endpoint별로 의미 있는 임계값이 다르나(헬스체크 vs 결제 API), URI별 오버라이드는 별도 메커니즘 필요 → 후속 task.
- 알림 폭주를 막기 위해, 후속 평가 task에서 같은 (application, instance, endpoint) 키 기준 dedup/cooldown 정책 도입 권장(본 정의 외).

---

## 5. HIKARI 임계값

DTO: `HikariMetricSnapshotDto`

| 메트릭 | DTO 필드 | 평가 단위 | 단위 | 연산자 | warning | critical | 윈도우 | 근거 |
|--------|----------|-----------|------|--------|---------|----------|--------|------|
| 활성 커넥션 수 | `active.points[].value` | application + instance + pool | 개 | `>=` | 8 | 10 | 연속 2회 | HikariCP 기본 `maximumPoolSize=10` 가정. 풀 max 미노출 상태에서의 잠정값 — 풀 크기 의존성 한계는 §5.1 참조. 풀 사용률 메트릭 도입으로 1차 완화됨 |
| 풀 사용률 | `active.usageRatio[].value` | application + instance + pool | 비율 (0~1) | `>` | 0.8 | 0.95 | 연속 2회 | active/max 비율로 풀 크기 의존성 해소. 80% 경고, 95% 위험은 일반 커넥션 풀 권장값. PromQL이 비율 그대로 반환(×100 변환 없음) — §2.5 단위 주의 참조 |
| 대기 커넥션 수 | `pending.points[].value` | application + instance + pool | 개 | `>=` | 1 | 5 | 단일 | pending > 0 자체가 풀 고갈 신호. 5 이상은 명백한 saturation |

### 5.1 한계
- `active`의 절대값 임계는 풀 크기에 의존적이라 정확하지 않다. 풀 크기를 다르게 설정한 대상 프로젝트에서는 false positive/negative 가능.
- 본 표의 `풀 사용률`(`active/max` 비율) 도입으로 1차 완화. 운영 베이스라인 누적 후 `active` 절대값 메트릭은 deprecate 검토 가능.

---

## 6. DTO ↔ PromQL ↔ 임계값 매핑 부록

| 임계값 항목 | DTO 필드 | 출처 PromQL (`docs/metrics/promqls.md`) |
|-------------|----------|------------------------------------------|
| CPU 사용률 | `JvmMetricSnapshotDto.cpu.usagePercent` | `avg by (application, instance) (avg_over_time(process_cpu_usage[1m]))` |
| Heap 사용률 | `JvmMetricSnapshotDto.memory.heapUsagePercent` | `sum(...jvm_memory_used_bytes{area="heap"}) / sum(...jvm_memory_max_bytes{area="heap"}) * 100` |
| Heap Old Gen 사용률 | `JvmMetricSnapshotDto.memory.oldGenUsagePercent` | `sum(...jvm_memory_used_bytes{area="heap", id=~".*Old Gen\|.*Tenured Gen"}) / sum(...jvm_memory_max_bytes{...}) * 100` |
| GC 평균 pause | `JvmMetricSnapshotDto.gc.avgDurationSeconds` | `sum(...increase(jvm_gc_pause_seconds_sum[1m])) / clamp_min(sum(...increase(jvm_gc_pause_seconds_count[1m])),1)` |
| GC 빈도 | `JvmMetricSnapshotDto.gc.count` | `sum by (application, instance) (increase(jvm_gc_pause_seconds_count[1m]))` |
| 활성 스레드 수 | `JvmMetricSnapshotDto.thread.activeCount` | `avg by (application, instance) (jvm_threads_live_threads)` |
| Peak 스레드 수 | `JvmMetricSnapshotDto.thread.peakCount` | `avg by (application, instance) (jvm_threads_peak_threads)` |
| Daemon 스레드 수 | `JvmMetricSnapshotDto.thread.daemonCount` | `avg by (application, instance) (jvm_threads_daemon_threads)` |
| HTTP P99 | `HttpMetricSnapshotDto.p99.points[].value` | `histogram_quantile(0.99, sum by (application, instance, uri, le) (rate(http_server_requests_seconds_bucket[1m])))` |
| HTTP 5xx 비율 | `HttpMetricSnapshotDto.errorRate.points[].value` | `sum by (application, instance, uri) (rate(http_server_requests_seconds_count{status=~"5.."}[1m])) / clamp_min(sum by (application, instance, uri) (rate(http_server_requests_seconds_count[1m])),1)` |
| Hikari Active | `HikariMetricSnapshotDto.active.points[].value` | `avg_over_time(hikaricp_connections_active[1m])` |
| Hikari Usage Ratio | `HikariMetricSnapshotDto.active.usageRatio[].value` | `hikaricp_connections_active / clamp_min(hikaricp_connections_max, 1)` |
| Hikari Pending | `HikariMetricSnapshotDto.pending.points[].value` | `avg_over_time(hikaricp_connections_pending[1m])` |

> 평가 단위의 라벨은 위 PromQL의 `by(...)` 그룹 라벨과 정합한다(예: HTTP는 `(application, instance, uri, le)` → endpoint 단위 평가 가능, Hikari는 라벨에 `pool`이 포함되는지 PromQL 보강 시 확인 필요).

---

## 7. 후속 권장 (현 PromQL/DTO로 표현 불가 — 본 표에서는 임계값 정의 보류)

각 항목은 새 PromQL 추가 또는 baseline 학습 인프라가 선행되어야 한다. 임계값 자체는 정의하지 않고 항목만 기록한다.

| 항목 | 필요 PromQL (예시) | 필요 DTO 변경 | 추정 가치 |
|------|---------------------|----------------|-----------|
| HTTP RPS 급락 | (현 PromQL 활용) + baseline 학습 메커니즘 | DTO 변경 없음, 평가 로직 신설 | 중간 — false positive 위험 큼, baseline 인프라 필요 |

> 이전 버전(Task 014)에서 §7에 포함되어 있던 `HTTP 5xx 비율`, `Heap Old Gen 사용률`, `Hikari 풀 사용률`, `Thread peak / daemon`은 PromQL과 DTO가 모두 이미 존재함이 Task 016 검토에서 확인되어 §3-§5 정식 정의로 승격됨. §8 Changelog 참조.

---

## 8. Changelog

| 일자 | 변경 | 사유 |
|------|------|------|
| 2026-05-03 | 초기 정의 작성 (JVM 5 / HTTP 1 / HIKARI 2 항목) | Task 014 |
| 2026-05-03 | §7 5종(`HEAP_OLD_GEN_USAGE`, `PEAK_THREADS`, `DAEMON_THREADS`, `HTTP_ERROR_RATE`, `HIKARI_USAGE_RATIO`)을 §3-§5 정식 정의로 승격. PromQL과 DTO 필드(`memory.oldGenUsagePercent`, `thread.peakCount`, `thread.daemonCount`, `errorRate.points[].value`, `active.usageRatio[].value`)가 모두 이미 존재함을 확인. §3.1(JVM 한계) 신설로 Peak/Daemon 한계 명시. §2.5에 `HTTP_ERROR_RATE` / `HIKARI_USAGE_RATIO`가 0~1 비율 단위(×100 변환 없음)임을 명시. §5.1 한계 갱신(풀 사용률 메트릭 도입으로 1차 완화). §6 매핑 부록에 5행 추가. 총 메트릭 8 → **13종**. | Task 016 |
