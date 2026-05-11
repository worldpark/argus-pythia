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
                              Kafka: jvm.metrics.raw
                                       │
                    ┌──────────────────┼─────────────────────┐
                    ▼                                         │
                [Pythia]                                      │
          Anomaly Detector                                    │
        단순 임계값 초과 ──────────────────────────▶ [Alert]  │
        복합 패턴 감지 ──▶ LLM Analyzer (Spring AI) ──▶       │
                              RAG (PGVector)                  │
                    └─────────────────────────────────────────┘
                                       │
                                 Slack / Email
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
├── argus/          # Metric Collector
├── pyshia/         # LLM Analyzer
├── docker/         # Docker Compose 및 인프라 설정
│   └── prometheus/
│       ├── prometheus.yml
│       ├── targets.yml
│       └── grafana/
└── docs/
    ├── architecture.md
    ├── tasks/      # 업무 태스크 문서
    └── plans/      # 구현 계획 문서
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
| `jvm.metrics.raw` | Argus | Pythia | 수집된 원시 메트릭 |
| `jvm.alert` | Pythia | Alert | 알림 이벤트 |

파티션 키는 `serviceId`를 사용하여 동일 서비스의 메트릭 순서를 보장합니다.

---

## 이상 감지 흐름

```
단순 임계값 초과 (예: 힙 사용률 > 85%)
    └──▶ LLM 없이 즉시 Alert 발행

복합 패턴 감지 (예: GC 급증 + P99 동시 상승)
    └──▶ LLM Analyzer 분석
           ├── PGVector에서 과거 장애 패턴 RAG 조회
           └──▶ 분석 결과와 함께 Alert 발행
```

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

```bash
# 인프라 (Kafka, Prometheus, Grafana, PostgreSQL) 기동
cd docker
docker compose -f docker-compose.kafka.yml up -d
docker compose -f docker-compose.prometheus.yml up -d

# Argus 실행
cd ../argus
./gradlew bootRun

# Pythia 실행
cd ../pyshia
./gradlew bootRun
```

---

## 테스트

```bash
# 서비스별 테스트 실행
cd argus && ./gradlew test
cd pyshia && ./gradlew test
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

## 상세 아키텍처

[docs/architecture.md](docs/architecture.md) 참조
