# Argus-Pythia — JVM 성능 모니터링 플랫폼

> **실시간 JVM 메트릭 수집 + LLM 기반 이상 분석** 을 결합한 범용 모니터링 모듈

> 1인 개발 프로젝트 — Claude Code **에이전트 하네스 엔지니어링**을 활용하여 plan → implementor → reviewer → fixer 워크플로우로 구현

---

## 개요

운영 환경에서 API 응답속도 급저하 등 성능 이슈가 발생했을 때 참고할 지표가 부족했던 경험을 바탕으로 설계한 시스템입니다.

Spring Boot 서비스에 **Actuator + Micrometer** 의존성과 간단한 YAML 설정만 추가하면 즉시 모니터링이 가능합니다. 수집된 지표를 기반으로 단순 임계값 알림부터 LLM 기반 복합 패턴 분석까지 제공합니다.

---

## 서비스 구성

| 서비스 | 역할 | 어원 |
|--------|------|------|
| **Argus** | Metric Collector — Prometheus 지표 수집 및 Kafka 발행 | 그리스 신화의 100개의 눈을 가진 감시자 |
| **Pythia** | LLM Analyzer — 이상 감지 및 알림 발송 | 델포이 신전의 신탁을 내리던 무녀 |

---

## 아키텍처

```
[Target Services] ──스크래핑──▶ [Prometheus] ──PromQL──▶ [Grafana]
Any Spring Boot Apps              수집·저장              시각화

                                       │
                              /api/v1/targets + PromQL
                                       ▼
                                    [Argus]
                               Metric Collector
                                       │
                  Kafka: jvm / http / hikari .metrics.raw
                                       │
                                       ▼
                                    [Pythia]
                       ┌─────────────┴──────────────┐
                       ▼                            ▼
                  MetricStore                 ThresholdEvaluator
                (PostgreSQL 적재)           (임계값 위반 판정)
                       │                            │
                       │ 최근 10분 조회            │
                       └──────┬─────────────────────┘
                              ▼
                  MetricAnalysisRequestAssembler
                              │
                              ▼
              MetricAnalysisService (Spring AI)
                              │
                              ▼
                         [Alert 이메일]
                  본문 + "## LLM 분석" 섹션
```

---

## 기술 스택

| 분류 | 기술 |
|------|------|
| Backend | Java 21, Spring Boot 4.0.6 |
| AI / LLM | Spring AI 2.0.0-M6, OpenAI |
| 메시징 | Apache Kafka |
| 메트릭 수집 | Prometheus, Spring Boot Actuator, Micrometer |
| 시각화 | Grafana |
| 데이터베이스 | PostgreSQL (Alert Rule DB + PGVector) |
| 개발 DB | H2 (테스트 전용) |
| 알림 | Slack, Email |
| 빌드 | Gradle |
| 배포 | Docker Compose, Jenkins CI/CD |

---

## 모듈 구조

```
C:\side_project
├── argus/                       # Metric Collector
│   ├── Dockerfile               # 멀티스테이지 빌드 (JDK21 → JRE21)
│   └── .dockerignore
├── pythia/                      # LLM Analyzer
│   ├── Dockerfile               # 멀티스테이지 빌드 (JDK21 → JRE21)
│   └── .dockerignore
├── docker/                      # 인프라 Compose
│   ├── docker-compose.postgres.yml
│   ├── kafka/
│   │   └── docker-compose.yml
│   ├── prometheus/
│   │   ├── docker-compose.yml   # Prometheus + Grafana
│   │   ├── prometheus.yml
│   │   ├── targets.yml
│   │   └── grafana/
│   │       ├── dashboards/
│   │       └── provisioning/    # datasources, dashboards 자동 등록
│   └── was/                     # Spring Boot WAS (argus + pythia)
│       └── docker-compose.yml
└── docs/
    ├── architecture.md
    ├── tasks/                   # 업무 태스크 문서
    └── plans/                   # 구현 계획 문서
```

---

## 수집 메트릭

Argus는 60초 주기로 아래 메트릭을 수집하여 `jvm.metrics.raw` 토픽으로 발행합니다.

| 분류 | 메트릭 | 목적 |
|------|--------|------|
| CPU | `process_cpu_usage` | CPU 과부하 감지 |
| 메모리 | `jvm_memory_used_bytes` | 힙 사용량 추이 |
| 메모리 | `jvm_memory_max_bytes` | 힙 사용률 계산 기준 |
| GC | `jvm_gc_pause_seconds_sum` | GC 총 소요시간 |
| GC | `jvm_gc_pause_seconds_count` | GC 발생 횟수 |
| 스레드 | `jvm_threads_live_threads` | 스레드풀 포화 감지 |
| HTTP | `http_server_requests_seconds_count` | 요청 처리량 |
| HTTP | `http_server_requests_seconds_bucket` | P99 응답시간 계산 |
| DB | `hikaricp_connections_active` | 커넥션 사용 현황 |
| DB | `hikaricp_connections_pending` | 커넥션 병목 선행 감지 |

---

## Kafka 토픽

| 토픽 | 발행자 | 소비자 | 설명 |
|------|--------|--------|------|
| `jvm.metrics.raw` | Argus | Pythia | JVM 스냅샷 (CPU/Heap/GC/Thread) |
| `http.metrics.raw` | Argus | Pythia | HTTP 스냅샷 (P99/RPS/Error Rate) |
| `hikari.metrics.raw` | Argus | Pythia | HikariCP 스냅샷 (Active/Pending/Usage Ratio) |

파티션 키는 `serviceId`를 사용하여 동일 서비스의 메트릭 순서를 보장합니다.

---

## 이상 감지 흐름

```
1. Pythia가 Kafka 메시지 수신
     └──▶ MetricStoreService 가 PostgreSQL 에 1분 주기 적재
            (JvmMetricSnapshotEntity / HttpMetricSnapshotEntity / HikariMetricSnapshotEntity)

2. ThresholdEvaluator 가 메트릭별 임계값(warning/critical) 위반 판정
     └──▶ 연속 위반(consecutive) 조건 충족 시 AlertNotifier 호출

3. AlertNotifier (@Async)
     ├──▶ MetricAnalysisRequestAssembler
     │       └──▶ 최근 N분 분량 스냅샷을 DB 에서 조회 (N = pythia.alert.analysis-window)
     │             → MetricAnalysisRequest 로 변환 (AVG/MAX summary + 시계열)
     ├──▶ MetricAnalysisService.analyze() — Spring AI ChatClient 호출
     └──▶ EmailService.sendToOperator()
             본문: 메트릭/심각도/현재값/임계값 + "## LLM 분석" 섹션

※ LLM 호출 실패 시(AiAnalysisException, DataAccessException 등) 분석 섹션 없이
  이메일 정상 발송 — 운영자 알림 우선 원칙 (fallback)
```

### Pythia 설정 (application.yml)

| 키 | 타입 | 기본값 | 설명 |
|---|---|---|---|
| `pythia.alert.analysis-window` | Duration | `10m` | LLM 분석에 사용할 메트릭 조회 윈도우. Spring Boot Duration 표기(`10m`, `1h`, `PT30M` 등) 지원. 0 또는 음수는 기동 시 거부 |
| `pythia.threshold.*` | — | application.yml 참조 | 메트릭별 warning/critical/consecutive 임계값 |
| `pythia.email.operator-recipients` | List | `${MAIL_USERNAME}` | 임계값 알림 수신자 목록 |

---

## 모니터링 대상 서비스 연동

모니터링할 Spring Boot 서비스에 아래 설정만 추가하면 자동으로 수집 대상에 포함됩니다.

**1. build.gradle**

```groovy
implementation 'org.springframework.boot:spring-boot-starter-actuator'
implementation 'io.micrometer:micrometer-registry-prometheus'
```

**2. application.yml**

```yaml
management:
  endpoints:
    web:
      exposure:
        include: prometheus
  metrics:
    export:
      prometheus:
        enabled: true
```

**3. docker/prometheus/prometheus.yml 에 타겟 등록**

```yaml
scrape_configs:
  - job_name: 'spring-apps'
    static_configs:
      - targets:
          - 'order-service:8080'
          - 'payment-service:8081'
```

---

## 로컬 실행

### 옵션 A — 호스트에서 직접 실행

```powershell
# 인프라 (Kafka, Prometheus, Grafana, PostgreSQL) 기동
docker compose -f docker/kafka/docker-compose.yml up -d
docker compose -f docker/docker-compose.postgres.yml up -d
docker compose -f docker/prometheus/docker-compose.yml up -d

# Argus 실행
cd argus; ./gradlew bootRun

# Pythia 실행 (별도 터미널)
cd pythia; ./gradlew bootRun
```

### 옵션 B — 전체 컨테이너 실행

```powershell
# 1. 인프라 기동 (옵션 A 1~3번 동일)

# 2. WAS 환경변수 준비 (.env)
Copy-Item docker/was/.env.example docker/was/.env
# .env 의 DB_USERNAME, DB_PASSWORD, MAIL_USERNAME, MAIL_PASSWORD, OPENAI_API_KEY 채우기

# 3. argus + pythia 빌드 및 기동 (외부 네트워크 argus-net / prometheus-net 사전 생성 필요)
docker compose -f docker/was/docker-compose.yml up -d --build

# 포트 매핑: argus=80, pythia=8080
```

---

## 테스트

```bash
# 서비스별 테스트 실행
cd argus && ./gradlew test
cd pythia && ./gradlew test
```

---

## API 응답속도 급저하 원인별 감지 지표

| 원인 | 선행 지표 | 감지 방법 |
|------|-----------|-----------|
| GC Stop-the-World | `jvm_gc_pause_seconds_sum` 급증 | GC pause time 임계값 초과 |
| 스레드풀 포화 | `jvm_threads_live_threads` 상승 | 스레드 수 임계값 초과 |
| DB 커넥션 고갈 | `hikaricp_connections_pending` > 0 | pending 발생 즉시 알림 |
| 힙 메모리 부족 | `jvm_memory_used_bytes / max` > 85% | 사용률 임계값 초과 |

---

## Roadmap

### 완료

- [x] Grafana 대시보드 JSON 프로비저닝 — JVM / HTTP / HikariCP 시각화 패널 구성
- [x] Argus / Pythia 멀티스테이지 Dockerfile 및 `docker/was` compose 구성 *(Task 019)*
- [x] 임계값 위반 시 LLM 분석 결과를 알림 이메일에 첨부 *(Task 020)*
  - DB 적재된 최근 10분 메트릭 → `MetricAnalysisRequest` 변환 → Spring AI 호출 → 본문 첨부
  - 분석 실패 시 fallback 으로 분석 섹션 없이 이메일 정상 발송

### 진행 예정

**LLM 분석 파이프라인 고도화**
- [ ] PGVector 연동 — 과거 장애 패턴 벡터 저장 및 RAG 조회
- [ ] LLM 호출 타임아웃 및 응답 길이 제한 정책

**인프라**
- [ ] Jenkins CI/CD 파이프라인 구성 — Argus / Pythia 자동 빌드 및 배포

---

## 상세 아키텍처

[docs/architecture.md](docs/architecture.md) 참조
