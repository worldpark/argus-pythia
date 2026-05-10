# Task 010 — Grafana docker-compose 작성 계획

## Context
현재 `docker/prometheus/docker-compose.yml`에는 Prometheus 단일 서비스만 정의되어 있다. 메트릭 시각화를 위한 Grafana 인스턴스를 동일 compose 파일에 추가하여, 로컬 개발 환경에서 `docker compose up -d` 한 번으로 Prometheus + Grafana 스택이 함께 기동되고 Grafana가 Prometheus를 데이터소스로 즉시 사용할 수 있는 상태를 만든다.

사용자 프롬프트는 `docker/prometheus/docker-compose.yml`에 추가하도록 명시했고(Task 문서의 `docker/docker-compose.yml` 표기보다 우선), 기존 prometheus-net 네트워크/볼륨 패턴과 일치한다.

## 설계 방식 및 이유
- **단일 compose 파일에 service 추가**: 기존 prometheus 서비스와 한 네트워크/한 라이프사이클로 묶여야 "연동"이 자연스럽다. 별도 compose 파일을 두면 네트워크 공유와 기동 순서 관리가 번거로워진다.
- **Datasource provisioning 사용**: Grafana 컨테이너가 처음 기동될 때 Prometheus를 자동 등록한다. UI 수동 설정을 없애 "Prometheus와 연동" 요구를 코드로 보장한다. 파일 1개 추가로 비용이 작다.
- **Dashboard provisioning은 포함하지 않음**: Task 범위는 compose 작성이므로 대시보드 정의는 별도 Task로 분리한다 (과도한 설계 금지).
- **익명 접근/로그인 자동화는 하지 않음**: 로컬 기본 admin/admin 계정으로 충분. 환경변수로 보안 우회를 추가하면 운영 환경 혼선 위험.
- **healthcheck + depends_on(condition: service_healthy)**: prometheus가 ready된 후 grafana가 datasource 검증을 시도하도록 순서 보장.

## 구성 요소

### 신규/수정 파일
| 경로 | 변경 유형 | 역할 |
|---|---|---|
| `docker/prometheus/docker-compose.yml` | 수정 | grafana 서비스, 볼륨 추가 |
| `docker/prometheus/grafana/provisioning/datasources/datasource.yml` | 신규 | Prometheus 데이터소스 자동 등록 |

### grafana 서비스 정의 항목
- image: `grafana/grafana:11.2.0` (LTS 계열 안정 버전 고정)
- container_name: `grafana`
- ports: `3100:3000`
- volumes:
  - `grafana-data:/var/lib/grafana` (대시보드/사용자 상태 영속화)
  - `./grafana/provisioning:/etc/grafana/provisioning:ro`
- environment:
  - `GF_SECURITY_ADMIN_USER=admin`
  - `GF_SECURITY_ADMIN_PASSWORD=admin`
- depends_on:
  - `prometheus: { condition: service_healthy }`
- healthcheck: `wget -qO- localhost:3000/api/health`
- restart: `unless-stopped`
- networks: `prometheus-net`

### datasource.yml 핵심 필드
- `apiVersion: 1`
- `datasources[0].name: Prometheus`
- `datasources[0].type: prometheus`
- `datasources[0].url: http://prometheus:9090` (compose 내 서비스명으로 DNS 해석)
- `datasources[0].access: proxy`
- `datasources[0].isDefault: true`

### volumes 섹션 추가
- `grafana-data: { name: grafana-data }`

## 데이터 흐름
1. `docker compose -f docker/prometheus/docker-compose.yml up -d` 실행
2. prometheus 컨테이너 기동 → `:9090/-/healthy` 응답하면 healthy 상태 진입
3. grafana 컨테이너 기동 (depends_on 충족 후) → provisioning 디렉토리 스캔 → datasource.yml 로드 → Prometheus 데이터소스 등록
4. 호스트 `localhost:3100` → grafana 컨테이너 `3000` 포트로 포워딩
5. Grafana Explore에서 PromQL 입력 → grafana 컨테이너 → `prometheus-net` 내부 DNS `prometheus:9090` → Prometheus 응답
6. Prometheus는 기존 설정대로 `host.docker.internal` 경유 host의 Spring Boot `/actuator/prometheus` scrape

## 예외 처리 전략
- **포트 충돌 (3100 사용 중)**: docker compose가 기동 실패 메시지 출력 → 사용자가 점유 프로세스 정리 후 재시도 (코드/설정에서 처리할 영역 아님).
- **prometheus healthcheck 실패**: depends_on(condition: service_healthy)에 의해 grafana가 대기. 일정 시간 후에도 unhealthy면 사용자가 prometheus 로그 확인.
- **datasource.yml 문법 오류**: grafana 기동 로그에 provisioning error로 기록됨. 컨테이너는 기동되나 데이터소스가 등록되지 않음 → 검증 단계에서 발견.
- **볼륨 권한 문제(Windows Docker Desktop)**: 명명 볼륨(`grafana-data`)을 사용하므로 호스트 바인드 마운트 권한 이슈 회피. provisioning 디렉토리는 read-only 마운트.
- 컨테이너 내부 예외는 Grafana/Prometheus 자체가 처리하므로 별도 코드 대응 없음.

## 검증 방법
```powershell
docker compose -f docker/prometheus/docker-compose.yml up -d
docker compose -f docker/prometheus/docker-compose.yml ps
```
1. `ps` 출력에서 prometheus, grafana 모두 `healthy` 상태 확인
2. 브라우저 `http://localhost:9090/targets` → 기존 argus-services target 정상 노출 확인
3. 브라우저 `http://localhost:3100` 접속 → admin/admin 로그인
4. Connections → Data sources → `Prometheus` 항목 자동 등록 + "Test" 클릭 시 success
5. Explore → PromQL `up` 실행 → 결과 그래프 출력
6. 정리:
```powershell
docker compose -f docker/prometheus/docker-compose.yml down
```

## 트레이드오프
| 결정 | 채택 | 대안 | 사유 |
|---|---|---|---|
| compose 파일 위치 | 기존 `docker/prometheus/docker-compose.yml`에 추가 | `docker/grafana/docker-compose.yml` 신규 분리 | 사용자 프롬프트 명시 + 단일 네트워크/lifecycle 공유로 운영 단순화 |
| Datasource 자동 등록 | provisioning yml 1개 추가 | UI에서 수동 등록 | "Prometheus 연동" 요구를 코드로 재현 가능하게 보장. 비용 작음 |
| Dashboard provisioning | 미포함 | 기본 JVM 대시보드 자동 import | Task 범위 초과 — 별도 Task로 분리 |
| 이미지 태그 | `11.2.0` 고정 | `latest` | 재현성 확보, 로컬 환경 간 동작 차이 방지 |
| 인증 설정 | 기본 admin/admin 유지 | 익명 접근 활성화(`GF_AUTH_ANONYMOUS_ENABLED`) | 로컬용이라도 의도치 않은 외부 노출 시 위험. 기본값 유지가 안전 |
| 데이터 영속화 | 명명 볼륨 사용 | tmpfs / 미설정 | 컨테이너 재기동 시 사용자 설정/탐색 기록 유지. Windows에서 바인드 마운트 권한 이슈도 회피 |

## 작업 순서
1. `docker/prometheus/grafana/provisioning/datasources/` 디렉토리 생성 후 `datasource.yml` 작성
2. `docker/prometheus/docker-compose.yml`에 grafana 서비스 블록과 `grafana-data` 볼륨 추가
3. `docker compose -f docker/prometheus/docker-compose.yml up -d` 실행
4. 위 검증 방법 6단계 수행
