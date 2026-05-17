# JVM 모니터 모듈 아키텍처 설계서

## 개요

실무 운영 중 API 응답속도 급저하 등 성능 이슈 발생 시 참고할 수 있는 지표가 부족했던 경험을 바탕으로 설계한 **범용 JVM 성능 모니터링 모듈**입니다.

어느 애플리케이션에도 종속되지 않으며, Spring Boot 서비스에 **Actuator + Micrometer** 의존성과 간단한 설정만 추가하면 즉시 모니터링이 가능합니다. 수집된 지표를 기반으로 단순 임계값 알림부터 LLM 기반 성능 분석까지 제공합니다.

---

## 서비스 구성

| 서비스명 | 역할 | 어원 |
|----------|------|------|
| **Argus** | Metric Collector | 그리스 신화의 100개의 눈을 가진 감시자 |
| **Pythia** | Anomaly Detector + LLM Analyzer | 델포이 신전의 신탁을 내리던 무녀 |

---

## 기술 스택

| 분류 | 기술 |
|------|------|
| Backend | Java 21, Spring Boot, Spring AI |
| 메시징 | Kafka |
| 수집/저장 | Prometheus |
| 시각화 | Grafana |
| LLM | OpenAI |
| 메트릭 저장 | PostgreSQL (로컬) |
| 알림 | Slack / Email |
| 배포 | Docker Compose |

---

## 아키텍처 구조

```
┌─────────────────────────────────────────────────────────────────────┐
│ Docker Compose                                                       │
│                                                                      │
│  [Target Services] ──스크래핑──▶ [Prometheus] ──PromQL──▶ [Grafana] │
│  Any Spring Boot Apps             스크래핑 + 저장         지표 시각화│
│                                        │                             │
│                              /api/v1/targets + PromQL                │
│                                        ▼                             │
│                                    [Argus]                           │
│                                Metric Collector                      │
│                                        │                             │
│                                        ▼                             │
│  ─────────────────────────────────[Kafka]─────────────────────────── │
│                                        │                             │
│  ┌─ Pythia ─────────────────────────── ▼ ──────────────────────┐    │
│  │                                                              │    │
│  │   [Anomaly Detector] ──복합 패턴──▶ [LLM Analyzer]           │    │
│  │      임계값 판단                  Spring AI + OpenAI         │    │
│  │           │                              │                   │    │
│  │       저장│                              │                   │    │
│  │           ▼                              │                   │    │
│  │   ┌──────────────────────┐               │                   │    │
│  │   │ PostgreSQL (로컬)    │               │                   │    │
│  │   │  메트릭 데이터        │               │                   │    │
│  │   │  수집된 JVM 지표      │               │                   │    │
│  │   └──────────────────────┘               │                   │    │
│  │                                          │                   │    │
│  │           ┄┄단순 임계값 초과┄┄┄┄┄┄┄┄┄┄┄┄│                   │    │
│  │                                          ▼                   │    │
│  └──────────────────────────────────────── [Alert] ─────────────┘    │
│                                          Slack / Email               │
└─────────────────────────────────────────────────────────────────────┘

범례: [서비스] [수집/분석] [메시징] [시각화] [저장] [감지/알림]
```

---

## 서비스별 상세 설명

### Argus (Metric Collector)

Prometheus `/api/v1/targets` API를 주기적으로 조회하여 모니터링 대상 서비스 목록을 **자동으로 감지**합니다. 별도 등록 없이 `prometheus.yml`에 추가된 서비스가 자동으로 모니터링 대상이 됩니다.

**폴링 주기**

| 주기 | 대상 지표 |
|------|-----------|
| 60초 | CPU, 힙 사용률, GC pause time, 스레드 수 |
| 60초 | HTTP 응답시간, HikariCP 커넥션 |

**수집 메트릭 목록**

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
| DB 커넥션 | `hikaricp_connections_active` | 커넥션 사용 현황 |
| DB 커넥션 | `hikaricp_connections_pending` | 커넥션 병목 선행 감지 |

수집된 메트릭은 Kafka `jvm.metrics.raw` 토픽으로 발행됩니다.

---

### Pythia (Anomaly Detector + LLM Analyzer)

Kafka에서 메트릭을 소비하여 이상 감지 및 분석을 수행합니다. 내부적으로 **Anomaly Detector**와 **LLM Analyzer** 두 모듈로 구성되며, 수집된 메트릭은 PostgreSQL에 저장하여 추후 조회 및 분석에 활용합니다.

#### Anomaly Detector (Pythia 내부 모듈)

소비한 메트릭을 PostgreSQL에 저장하면서 임계값 룰을 적용해 1차 판단을 수행합니다.

```
단순 임계값 초과 (예: 힙 사용률 > 85%)
    └──▶ LLM 없이 바로 Alert 발행

복합 패턴 감지 (예: GC 급증 + P99 동시 튐)
    └──▶ LLM Analyzer로 전달
```

LLM 호출을 최소화하여 비용과 응답속도를 모두 최적화합니다.

#### LLM Analyzer (Pythia 내부 모듈)

Spring AI + OpenAI 기반으로 복합 패턴을 분석합니다. PostgreSQL에 누적된 메트릭을 분석 윈도우(`pythia.alert.analysis-window`) 단위로 조회하여 컨텍스트로 활용하고, 근거 있는 분석 결과를 생성합니다.

> 예시 출력: "GC pause time 급증과 P99 응답시간 동시 상승 패턴이 감지되었습니다. 힙 메모리 튜닝 또는 메모리 누수 점검이 필요합니다."

---

### Kafka 토픽 구조

| 토픽 | 발행자 | 소비자 | 설명 |
|------|--------|--------|------|
| `jvm.metrics.raw` | Argus | Pythia | 수집된 원시 메트릭 |
| `jvm.alert` | Pythia | Alert | 알림 이벤트 |

파티션 키는 `serviceId`를 사용하여 동일 서비스의 메트릭 순서를 보장합니다.

---

### PostgreSQL (로컬)

| 용도 | 설명 |
|------|------|
| 메트릭 데이터 저장 | Pythia가 Kafka에서 소비한 JVM 지표를 시계열로 누적 저장 |
| LLM 분석 컨텍스트 제공 | 분석 윈도우 기간의 메트릭을 조회하여 LLM 입력으로 활용 |

수집된 JVM 지표를 로컬 PostgreSQL에 저장함으로써 LLM 분석 시 일정 시간 범위의 과거 추이를 함께 참조할 수 있습니다.

---

## Target Service 연동 방법

모니터링 대상 서비스에 아래 설정만 추가하면 연동이 완료됩니다.

**1. 의존성 추가 (build.gradle)**

```groovy
implementation 'org.springframework.boot:spring-boot-starter-actuator'
implementation 'io.micrometer:micrometer-registry-prometheus'
```

**2. 설정 추가 (application.yml)**

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

**3. prometheus.yml에 타겟 등록**

```yaml
scrape_configs:
  - job_name: 'spring-apps'
    static_configs:
      - targets:
          - 'order-service:8080'
          - 'payment-service:8081'
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

## 배포 구성

```yaml
# docker-compose.yml 구성 요소
services:
  argus:         # Metric Collector
  pythia:        # Anomaly Detector + LLM Analyzer
  prometheus:    # 메트릭 수집/저장
  grafana:       # 시각화 대시보드
  kafka:         # 메시지 브로커
  zookeeper:     # Kafka 의존성
  postgres:      # 수집된 JVM 메트릭 저장
```
