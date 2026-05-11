# Task 014: JVM / HTTP / HIKARI 임계값 정의 — 구현 계획

## Context

Task 013에서 Pythia가 `jvm.metrics.raw`, `http.metrics.raw`, `hikari.metrics.raw` 세 토픽을 소비하는 진입점(`*MetricSnapshotConsumer` → `*MetricSnapshotHandler`)까지 구축했으나, 각 Handler는 hook 주석으로 비어있다. 후속 Task(임계값 평가 / 알림 / 상태 저장)가 일관된 기준으로 동작하려면 "어떤 메트릭이 어떤 값일 때 위험인지" 가 합의된 정의로 먼저 존재해야 한다. 본 task는 코드 변경 없이 그 정의 문서만 산출한다.

Task 명세상 AC 자체는 없으나(=정의 task), 후속 평가 Task가 곧장 매핑할 수 있도록 (a) DTO 필드와 1:1 연결되는 구조화된 표 + (b) PromQL 정합성을 함께 묶어 둔다. 신규 PromQL이 필요하면 `docs/metrics/promqls.md`를 함께 보강한다.

---

## 1. 설계 방식 및 이유

### 방향: 단일 정의 문서 1개 + (필요 시) PromQL 보강

| 결정 | 선택 | 이유 |
|------|------|------|
| 산출물 형태 | **`docs/metrics/thresholds.md` 1개** | 후속 평가 Task가 한 곳만 보면 됨. YAML/JSON 등 기계 가독 형식은 평가 엔진이 정해진 후에 도입(YAGNI) |
| 정의 단위 | **(domain, metric, level) 3축 표** | 한 메트릭에 warning/critical 두 단계만 우선 정의. 후속 운영 단계에서 info/emergency 추가 가능 |
| 평가 단위 | **메트릭별 명시** (`application` / `instance` / `endpoint` / `pool`) | 같은 임계값이라도 적용 그룹 단위가 다르면 의미가 달라짐. DTO 키와 일치해야 후속 평가가 단순 |
| 비교 연산 | **`>`, `>=`, `<` 명시** | 모호한 "초과" 표현 금지. 후속 코드가 enum으로 그대로 받음 |
| 평가 윈도우 | **"단일 스냅샷" vs "연속 N회"** 두 가지만 | 운영 노이즈 억제를 위한 최소 표현. 시간 기반 윈도우(예: 5분 평균)는 PromQL 측에서 이미 처리 — 본 layer는 스냅샷 단위 카운트로 충분 |
| 단위 표기 | **% / ms / count / 절대값**을 컬럼으로 분리 | DTO 값 단위(BigDecimal seconds vs %, count vs gauge)가 메트릭마다 달라 혼선 방지 |
| 임계값 출처 근거 | **각 행에 한 줄 코멘트** | "왜 70%인가"가 사라지면 후속 튜닝 시 임의 변경됨. JVM 일반 권장치 / Hikari max pool 비율 등 출처 한 줄 |
| PromQL 추가 정책 | **현 DTO로 표현 가능한 임계값 우선** / 신규 PromQL 필요 항목은 "후속 권장" 섹션에 별도 분리 | 본 task에서 DTO/Assembler 구조까지 건드리면 범위 초과. 추가 PromQL은 항목만 제안하고 실제 추가는 별 task |

### 비대상 (의식적 제외)
- 임계값 평가 엔진 / 룰 적용 코드
- 알림 채널 (Slack/메일) 매핑
- 사용자 정의 임계값 오버라이드(애플리케이션별)
- 임계값을 외부 설정으로 분리(yml/DB) — 평가 엔진 도입 시점에 결정

---

## 2. 구성 요소

### 신규 파일

**`docs/metrics/thresholds.md`** — 신규

목차 (예시):
1. 개요 / 적용 범위 / 평가 단위 정의
2. 공통 규약 (warning / critical 단계, 평가 윈도우 표기, 비교 연산자)
3. JVM 임계값 표
4. HTTP 임계값 표
5. HIKARI 임계값 표
6. DTO 필드 매핑 부록
7. 후속 권장(현 DTO로 표현 불가한 항목)

각 표 컬럼:
`도메인 | 메트릭 | DTO 필드 | 평가 단위 | 단위 | 연산자 | warning | critical | 윈도우 | 근거(한 줄)`

### 수정 파일 (조건부)

**`docs/metrics/promqls.md`** — 신규 PromQL을 본 task에서 추가하는 경우에만 수정. 추가 시:
- 항목별로 한 줄 설명 + PromQL 식
- 변경 사유 한 줄 (예: "임계값 `HTTP 5xx 비율` 정의에 필요")
- 기존 항목은 수정/삭제 없음 (Constraint 준수). 만약 부득이하게 변경한다면 변경 전/후 + 이유를 함께 기록

본 task의 1차 정의는 **현 PromQL/DTO 범위 안에서 가능한 임계값으로 한정**한다. 후속 권장 항목(예: HTTP 5xx 비율, Heap Old gen, Hikari pool 사용률 등)은 thresholds.md "후속 권장" 섹션에만 둔다.

### 수정하지 않는 파일
- 모든 `.java` 소스 (정의 task)
- `pythia/src/main/resources/application.yml`
- 기존 PromQL 식 자체 — Constraint 준수

---

## 3. 데이터 흐름

본 task 자체는 흐름 산출물이 없으나, 후속 Task가 본 정의를 어떻게 소비할지를 명시해 둔다.

```
[Argus]                         [Kafka]                  [Pythia]                          [후속 Task: 평가/알림]
*Producer ──▶ *.metrics.raw ──▶ *Consumer ──▶ *Handler ──▶ ThresholdEvaluator
                                                              │ (thresholds.md → 룰 셋 로딩)
                                                              ├─ snapshot 필드 추출 (예: cpu.usagePercent)
                                                              ├─ 평가 단위로 group (application / endpoint / pool)
                                                              ├─ 연산자 + warning/critical 비교
                                                              └─ 위반 시 AlertEvent 생성
```

본 정의 문서가 위 `ThresholdEvaluator`의 입력 명세 역할. DTO 필드 매핑 부록 덕에 "정의 → 코드 룰" 변환이 mechanical하게 이뤄진다.

---

## 4. 예외 처리 전략

문서 정의 task이므로 런타임 예외는 없다. 대신 정의상의 빈틈/모호성에 대한 처리 방침을 명시한다.

| 상황 | 처리 |
|------|------|
| 동일 메트릭에 warning ≥ critical 같은 모순 | 문서 작성 시 self-check 표 한 줄 추가. 후속 평가 코드도 부팅 시 동일 검증을 가질 수 있도록 명세 |
| DTO에 매핑되는 필드가 없는 임계값 | 정의 표에서 제외하고 "후속 권장" 섹션으로 이동. 그대로 두면 후속 코드가 NullPointer 위험 |
| `MetricStatus.MISSING` 등 결측 메트릭 | 평가 보류(skip) — 위반 아님. 결측 자체에 대한 알림은 별 task의 정책 |
| `SnapshotStatus.FAILED` 스냅샷 | Handler 단계에서 이미 조기 return — 본 정의는 적용되지 않음. 정의 문서에 한 줄로 명시 |
| 임계값이 너무 좁거나 노이즈 다발 | 본 task는 초기값 제시. 운영 튜닝은 후속에서 PR 단위로 갱신하고 변경 이력은 thresholds.md 상단 changelog 섹션 |

---

## 5. 검증 방법

코드가 없으므로 "정의의 정합성 검증"이 곧 검증이다.

### 정의 정합성 체크리스트
1. **DTO 매핑 검증** — 표 각 행의 "DTO 필드"가 실제 record의 필드명과 정확히 일치 (예: `JvmMetricSnapshotDto.cpu.usagePercent`). 오타 시 후속 평가 코드 컴파일 단계에서 즉시 드러남
2. **PromQL 매핑 검증** — 표 각 행이 `docs/metrics/promqls.md`의 어떤 라인에서 비롯됐는지 명시. 매핑 불가 항목은 "후속 권장"으로 이동
3. **임계값 단조성 검증** — warning < critical (큰값=위험인 메트릭) / warning > critical (작은값=위험인 메트릭, 예: RPS 급락) 일관성
4. **평가 단위와 PromQL `by(...)` 정합** — 정의의 평가 단위가 PromQL 집계 라벨에 포함되어 있어야 함 (예: HTTP P99는 `(application, instance, uri, le)`이므로 endpoint 단위 평가 가능)
5. **단위 일관성** — `process_cpu_usage`는 0~1, `heap %`는 0~100. DTO/PromQL 양쪽 단위가 표 단위와 같은지 확인 (필요 시 보정 명시)
6. **AC 충족** — Task 명세상 성공 조건 없음. "표가 작성되고 후속 Task가 매핑 가능한 상태" 가 성공.

### Out of scope
- 임계값 평가 코드 작성/테스트 — 별 task
- 실제 메트릭으로 임계값 적정성 시뮬레이션 — 운영 데이터 확보 후 별 task

---

## 6. 트레이드오프

1. **단일 markdown vs 구조화 포맷(YAML/JSON)** — markdown 채택. 평가 엔진이 아직 없어 기계 가독 포맷의 즉시 효용 없음. 엔진 도입 시점에 markdown → YAML 변환은 한 시간 작업.
2. **warning/critical 2단계 vs 다단계(info/warn/error/critical)** — 2단계 채택. 단계가 많을수록 알림 정책 복잡도 ↑. 운영 학습 후 확장.
3. **고정 임계값 vs 동적 baseline(예: SLO 기반)** — 고정 채택. baseline 학습은 시계열 저장/통계가 선결. 본 단계에선 일반 권장치 + 운영 튜닝.
4. **현 PromQL 안에서만 정의 vs 적극적 PromQL 추가** — 보수적으로 현 PromQL 우선. HTTP 5xx 비율 등 가치 큰 항목은 "후속 권장"으로 분리해 별 task로 처리. 이유: PromQL 추가 = Argus assembler/DTO 파급 → 본 task 범위 초과.
5. **메트릭별 평가 윈도우(연속 N회) 표준화 vs 메트릭별 자유** — 메트릭별 자유. CPU spike와 GC pause는 노이즈 특성이 달라 일률 적용이 부적절.
6. **`endpoint` 단위 임계값 폭증 위험** — HTTP는 URI 수에 따라 알림이 폭주할 수 있음. 본 정의에선 "전 URI 공통 임계값" 한 줄로 시작하고, URI별 오버라이드는 후속 task에서 별도 메커니즘 도입.
7. **Hikari 풀 사용률(active/max) 부재** — 현 PromQL은 `active` 절대값만. 풀 max 노출 PromQL(`hikaricp_connections_max`) 추가가 더 의미 있는 임계값을 만들지만, 본 task에서는 후속 권장으로만 명시.

---

## 핵심 파일 경로

신규 (절대 경로):
- `C:\side_project\docs\metrics\thresholds.md` — 신규 (정의 문서)

수정 (조건부):
- `C:\side_project\docs\metrics\promqls.md` — 본 task에서 PromQL을 추가하는 경우에만. 1차 정의는 추가 없이 작성 시도

참조 (수정 없음):
- `C:\side_project\docs\metrics\promqls.md` — 현 PromQL 인벤토리
- `C:\side_project\pythia\src\main\java\com\example\pythia\kafka\dto\jvm\*.java` — JVM DTO 필드명/단위 확인
- `C:\side_project\pythia\src\main\java\com\example\pythia\kafka\dto\http\*.java` — HTTP DTO 필드명/단위 확인
- `C:\side_project\pythia\src\main\java\com\example\pythia\kafka\dto\hikari\*.java` — Hikari DTO 필드명/단위 확인

---

## 1차 정의 후보 (작성 시 표에 채울 항목 — 예시값 아님, 항목 목록만)

### JVM (`JvmMetricSnapshotDto`)
- `cpu.usagePercent` — application+instance 단위, `>` warning/critical
- `memory.heapUsagePercent` — application+instance 단위, `>` warning/critical
- `gc.avgDurationSeconds` — application+instance 단위, `>` warning/critical
- `gc.count` — application+instance 단위, 1분당 빈도 `>` warning/critical
- `thread.activeCount` — application+instance 단위, `>` warning/critical (절대값 / 또는 후속에서 baseline 비교)

### HTTP (`HttpMetricSnapshotDto`)
- `p99.points[].value` — application+instance+endpoint 단위, `>` warning/critical (응답시간 ms)
- `rps.points[].value` — application+instance+endpoint 단위, `<` 기반 "급락" 임계 (선택 — 노이즈 위험으로 후속 권장으로 미룰지 본문에서 결정)

### HIKARI (`HikariMetricSnapshotDto`)
- `active.points[].value` — application+instance+pool 단위, `>` warning/critical (절대값 — 풀 max 부재로 비율 표현 불가)
- `pending.points[].value` — application+instance+pool 단위, `>` warning/critical (>0이면 이미 신호)

### 후속 권장 (현 PromQL/DTO로 표현 불가 — 본 task에서는 임계값 정의 보류)
- HTTP 5xx 비율 — 새 PromQL `rate(...status=~"5..")` + DTO 필드 추가 필요
- Heap Old Gen 사용률 — `jvm_memory_used_bytes{id="..."}` 영역별 분리 필요
- Hikari 풀 사용률(active/max) — `hikaricp_connections_max` 추가 필요
- Thread peak / daemon — 추가 메트릭 + DTO 확장

---

## 후속 작업 (본 task 범위 외)
- Task: ThresholdEvaluator 구현 — thresholds.md 표를 룰 셋으로 로드, Handler에서 호출
- Task: 알림 채널(예: Slack Webhook) Client 도입 + AlertEvent 라우팅
- Task: 후속 권장 PromQL 추가 (Argus assembler/DTO + Pythia DTO 동시 변경)
- Task: 임계값 외부화 (yml/DB)와 운영 핫 리로드
