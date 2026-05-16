# Task 019 — argus, pythia Dockerfile / docker-compose 작성 계획

## Context
현재 `docker/` 하위에는 인프라용 compose 파일만 존재한다 (`kafka/`, `prometheus/`, `docker-compose.postgres.yml`). Spring Boot 어플리케이션인 **argus**(메트릭 수집/Kafka Producer, port 80)와 **pythia**(Kafka Consumer + Spring AI + PostgreSQL, port 8080) 자체를 컨테이너로 띄우는 정의가 없다.

본 Task는 두 어플리케이션을 컨테이너화하여 기존 인프라 컨테이너(kafka, postgres, prometheus)와 동일 Docker 네트워크에서 `docker compose up -d` 한 번으로 동작하도록 만든다. 이후 구현 Task에서 별도 호스트 빌드/실행 없이 컨테이너 환경에서 즉시 검증 가능한 상태가 목표다.

Task 문서 명시: 작성 위치는 `docker/was/`, 포트 포워딩은 argus 80:80 / pythia 8080:8080, 환경변수는 `.env`.

## 1. 설계 방식 및 이유

### 1.1 멀티스테이지 Dockerfile (gradle build → JRE runtime)
- **builder stage**: `eclipse-temurin:21-jdk` + 프로젝트 소스 + gradle wrapper로 `bootJar` 실행
- **runtime stage**: `eclipse-temurin:21-jre` + builder의 `build/libs/*.jar` 만 복사
- **이유**: JDK 이미지를 그대로 배포하면 이미지 크기 ↑ 및 보안 표면 ↑. JRE-only 런타임으로 분리하면 이미지 약 절반 수준. Spring Boot 4 + Java 21 요구사항(루트 `CLAUDE.md`, 각 모듈 `build.gradle`)을 그대로 충족.
- **대안 미채택**: Spring Boot Layered JAR 분리 최적화는 본 Task 범위 초과(과도한 설계 금지). 후속 최적화 Task로 분리 가능.

### 1.2 모듈별 Dockerfile 위치 = 모듈 루트
- `argus/Dockerfile`, `pythia/Dockerfile` 두 개를 각 모듈 디렉토리에 둔다.
- **이유**: build context를 각 모듈로 한정 → 빌드 시간 단축, 다른 모듈 변경이 캐시 무효화하지 않음. Gradle wrapper와 build.gradle 이 모듈 내부에 있어 context를 모듈 루트로 두는 것이 자연스러움.
- **대안 미채택**: 단일 멀티프로젝트 Dockerfile은 현 프로젝트가 멀티모듈 Gradle이 아니라 모듈마다 독립 Gradle 구성을 갖고 있어 부적합 (`argus/settings.gradle`, `pythia/settings.gradle` 각각 존재).

### 1.3 compose 위치 = `docker/was/docker-compose.yml`
- 루트 `CLAUDE.md`의 Docker 파일 규칙("모든 docker 관련 파일은 `/docker` 경로", "환경별 compose 파일은 `/docker` 하위에서 분리")을 따른다.
- Task가 명시한 `docker/was/` 디렉토리를 신규 생성.

### 1.4 기존 인프라 네트워크 재사용 (external network)
- 기존 compose 들이 사용 중인 네트워크: `argus-net`(kafka, postgres), `prometheus-net`(prometheus, grafana). 모두 **우리가 운영하는 컨테이너**.
- `docker/was/docker-compose.yml`에서는 두 네트워크를 `external: true`로 선언하고 argus는 양쪽 모두에, pythia는 `argus-net`에 join.
- **이유**:
  - argus → kafka(`kafka:29092`), argus → prometheus(`prometheus:9090`) 컨테이너 간 통신 필요.
  - pythia → kafka(`kafka:29092`), pythia → postgres(`postgres:5432`) 통신 필요.
  - argus는 prometheus의 scrape 대상이 아니므로 prometheus → argus 방향 통신은 없음. argus의 `prometheus-net` join은 단방향 호출용.
  - external network 패턴으로 인프라 compose를 먼저 띄운 뒤 was compose를 띄우면 결합도 최소화.

### 1.5 환경변수는 `.env` + Spring Boot relaxed-binding 활용
- `application.yml` 의 값(예: `spring.kafka.bootstrap-servers`)을 코드 수정 없이 환경변수로 오버라이드한다.
- 매핑 규칙: `spring.kafka.bootstrap-servers` → `SPRING_KAFKA_BOOTSTRAP_SERVERS`, `prometheus.base-url` → `PROMETHEUS_BASE_URL` (= `http://prometheus:9090`, 우리가 운영하는 컨테이너), `spring.datasource.url` → `SPRING_DATASOURCE_URL`.
- **이유**: Task 범위는 docker 작성이며 application.yml 수정은 out-of-scope. Spring Boot의 environment variable binding으로 충분히 컨테이너 전용 값(localhost → 내부 DNS)을 주입 가능.
- `.env`는 git 추적 제외 권장 → `.env.example`을 함께 제공하여 신규 클론 환경에서 복사 후 비밀값만 채워 사용.

### 1.6 prometheus 서비스 vs targets 의 책임 경계
- **Prometheus 서버 컨테이너**: 우리가 `docker/prometheus/docker-compose.yml`로 운영. argus는 컨테이너 DNS `prometheus:9090`으로 직접 호출.
- **`docker/prometheus/targets.yml` (scrape 대상 목록)**: 사용자가 직접 운영하는 외부 어플리케이션(메트릭 노출 호스트)을 가리키므로 **사용자가 직접 갱신**. 본 Task에서는 일체 수정하지 않는다.
- argus 자체는 prometheus의 scrape 대상이 아니므로 argus 노출/포트 매핑(80:80)은 운영자/외부 클라이언트용일 뿐 prometheus와 무관.

## 2. 구성 요소

### 2.1 신규 파일
| 경로 | 역할 |
|---|---|
| `argus/Dockerfile` | argus 멀티스테이지 빌드 정의 |
| `argus/.dockerignore` | build context 슬림화 (build/, .gradle/, .idea/, *.iml) |
| `pythia/Dockerfile` | pythia 멀티스테이지 빌드 정의 |
| `pythia/.dockerignore` | 동일 목적 |
| `docker/was/docker-compose.yml` | argus, pythia 서비스 정의 |
| `docker/was/.env.example` | 환경변수 템플릿 (실 비밀값은 사용자가 `.env`로 복사 후 입력) |

### 2.2 Dockerfile 핵심 구조 (argus, pythia 공통)
```
FROM eclipse-temurin:21-jdk AS builder
WORKDIR /app
COPY gradle gradle
COPY gradlew gradlew
COPY settings.gradle build.gradle ./
COPY src src
RUN chmod +x gradlew && ./gradlew clean bootJar -x test --no-daemon

FROM eclipse-temurin:21-jre
WORKDIR /app
ENV TZ=Asia/Seoul
COPY --from=builder /app/build/libs/*-SNAPSHOT.jar app.jar
EXPOSE <80|8080>
ENTRYPOINT ["java","-jar","/app/app.jar"]
```
- `-x test`로 빌드 시간 단축 (테스트는 CI/로컬에서 별도 수행). 이 결정은 CLAUDE.md "테스트 없이 기능 추가 금지" 규칙과 충돌하지 않음 — 본 Task는 Dockerfile 인프라 작성이며 테스트 자체를 제거하는 것이 아님.
- `EXPOSE`는 argus=80, pythia=8080.

### 2.3 docker-compose.yml 핵심 구조
```yaml
services:
  argus:
    build:
      context: ../../argus
      dockerfile: Dockerfile
    container_name: argus
    ports:
      - "80:80"
    env_file:
      - .env
    environment:
      SPRING_KAFKA_BOOTSTRAP_SERVERS: kafka:29092
      PROMETHEUS_BASE_URL: http://prometheus:9090
      TZ: Asia/Seoul
    networks:
      - argus-net
      - prometheus-net
    restart: unless-stopped

  pythia:
    build:
      context: ../../pythia
      dockerfile: Dockerfile
    container_name: pythia
    ports:
      - "8080:8080"
    env_file:
      - .env
    environment:
      SPRING_DATASOURCE_URL: jdbc:postgresql://postgres:5432/pythia
      SPRING_DATASOURCE_USERNAME: ${DB_USERNAME}
      SPRING_DATASOURCE_PASSWORD: ${DB_PASSWORD}
      SPRING_KAFKA_BOOTSTRAP_SERVERS: kafka:29092
      SPRING_MAIL_USERNAME: ${MAIL_USERNAME}
      SPRING_MAIL_PASSWORD: ${MAIL_PASSWORD}
      SPRING_AI_OPENAI_API_KEY: ${OPENAI_API_KEY}
      TZ: Asia/Seoul
    networks:
      - argus-net
    restart: unless-stopped

networks:
  argus-net:
    external: true
    name: argus-net
  prometheus-net:
    external: true
    name: prometheus-net
```
- `depends_on`은 **사용하지 않는다**: 의존 서비스(kafka, postgres, prometheus)가 다른 compose 파일에 있어 같은 compose 그래프 안에서 dependency 정의가 불가. 외부 네트워크 join 후 어플리케이션 자체의 재시도/실패 로깅에 위임.
- argus는 prometheus 호출을 위해 `prometheus-net`에도 join, pythia는 호출 경로가 없어 `argus-net`만으로 충분.

### 2.4 `.env.example` 키 (값은 더미)
```
# Pythia DB
DB_USERNAME=pythia
DB_PASSWORD=pythia

# Pythia Mail (Gmail SMTP)
MAIL_USERNAME=changeme@example.com
MAIL_PASSWORD=changeme

# Pythia OpenAI
OPENAI_API_KEY=sk-xxxxxxxx
```
- `PROMETHEUS_BASE_URL`은 컨테이너 DNS(`http://prometheus:9090`) 고정값이므로 compose의 `environment`에 직접 기입, `.env` 키로 노출하지 않는다.
- 모든 비밀값은 `.env`에 채워 넣되 `.env` 자체는 git 추적 제외.

## 3. 데이터 흐름

### 3.1 빌드 단계
1. 사용자: `docker compose -f docker/was/docker-compose.yml build`
2. compose가 각 서비스의 build context로 이동
3. argus context(`argus/`): Dockerfile builder stage → gradle wrapper로 bootJar → runtime stage에 jar 복사
4. pythia도 동일
5. 결과 이미지: `was-argus:latest`, `was-pythia:latest` (compose 자동 명명)

### 3.2 기동 단계
1. (선행) 사용자: `docker compose -f docker/kafka/docker-compose.yml up -d`, `docker compose -f docker/docker-compose.postgres.yml up -d`, `docker compose -f docker/prometheus/docker-compose.yml up -d`
2. 위 단계에서 `argus-net`, `prometheus-net` 네트워크가 생성됨
3. 사용자: `docker compose -f docker/was/docker-compose.yml up -d`
4. compose가 `.env` 로드 → 환경변수가 컨테이너에 주입
5. argus 컨테이너: `argus-net` + `prometheus-net` join → `kafka:29092` producer 연결 + `prometheus:9090` 조회 → port 80 listen
6. pythia 컨테이너: `argus-net` join → `postgres:5432` JDBC 연결 + `kafka:29092` consumer 연결 → port 8080 listen
7. (사용자 운영 영역) prometheus는 `docker/prometheus/targets.yml`에 등록된 사용자 운영 호스트들을 scrape — argus/pythia는 scrape 대상이 아님

### 3.3 메시지/리퀘스트 흐름 (런타임)
- argus가 컨테이너 내부 prometheus(`prometheus:9090`)에서 메트릭을 pull → Kafka topic 발행 → pythia가 consume → threshold 판단 → 이메일 발송. 본 Task는 컨테이너 간 통신 경로만 보장하며, prometheus가 실제 scrape 하는 외부 어플리케이션(targets) 가용성은 사용자 책임.

## 4. 예외 처리 전략

| 케이스 | 처리 |
|---|---|
| 외부 네트워크 미생성 (`argus-net not found`, `prometheus-net not found`) | compose가 즉시 실패. 사용자에게 kafka/postgres/prometheus compose를 먼저 띄우라는 안내를 검증 절차에 명시 |
| prometheus 컨테이너 미기동 상태에서 argus 기동 | argus 부팅은 성공하나 `prometheus:9090` 호출이 실패 → 어플리케이션 재시도/로그. `restart: unless-stopped`로 자동 복구 |
| prometheus targets에 사용자 운영 호스트가 미등록/오설정 | argus는 prometheus 자체 응답은 받지만 빈 결과 또는 일부만 수신 → 사용자가 `targets.yml` 갱신 (본 Task 범위 외) |
| kafka/postgres 미기동 상태에서 was 기동 | Spring Boot 어플리케이션 자체 재시도(Kafka admin client / Hikari) 로깅. 컨테이너 레벨에서는 `restart: unless-stopped`로 자동 복구 |
| `.env` 파일 부재 | `env_file: - .env` 항목 때문에 compose가 실패. `.env.example`을 복사하라는 안내를 검증 절차에 포함 |
| 비밀값 누락(MAIL_PASSWORD 등) | Spring 기동 시 `${MAIL_USERNAME}` 미해결로 `IllegalArgumentException`. 컨테이너 로그에서 확인 가능 |
| 포트 충돌 (80, 8080) | compose 기동 단계에서 `bind: address already in use`. 사용자가 호스트 점유 프로세스 정리 |
| Gradle 빌드 실패 | builder stage에서 컨테이너 빌드 중단. 빌드 로그로 사용자가 진단 |
| Dockerfile cache 미스 (gradle 재다운로드) | 정상 동작이지만 빌드 시간 증가. 향후 의존성 캐시 최적화는 별도 Task |

## 5. 검증 방법

### 5.1 사전 준비
```powershell
# .env 생성 (실 비밀값 채우기)
Copy-Item docker/was/.env.example docker/was/.env

# 인프라 컨테이너 기동
docker compose -f docker/kafka/docker-compose.yml up -d
docker compose -f docker/docker-compose.postgres.yml up -d
docker compose -f docker/prometheus/docker-compose.yml up -d
```

### 5.2 빌드 & 기동
```powershell
docker compose -f docker/was/docker-compose.yml build
docker compose -f docker/was/docker-compose.yml up -d
docker compose -f docker/was/docker-compose.yml ps
```
1. `ps` 출력에서 argus, pythia 컨테이너가 `Up` 상태

### 5.3 동작 확인
```powershell
# argus actuator (있다면) 혹은 임의 endpoint
curl http://localhost:80/actuator/health    # argus
curl http://localhost:8080/actuator/health  # pythia

# 컨테이너 → 인프라 연결 확인
docker exec argus sh -c "getent hosts kafka || nslookup kafka"
docker exec pythia sh -c "getent hosts postgres || nslookup postgres"

# 로그 확인 (Kafka 연결, JDBC 연결 성공 여부)
docker logs --tail 100 argus
docker logs --tail 100 pythia
```
1. argus 로그에 Kafka producer 초기화 + `prometheus:9090` 조회 성공 메시지
2. pythia 로그에 Hikari pool 시작 + Kafka consumer group 가입 메시지
3. `docker exec argus sh -c "wget -qO- http://prometheus:9090/-/healthy"` → prometheus 응답 확인
4. (사용자 영역) `targets.yml`에 등록된 외부 호스트는 사용자가 직접 확인

### 5.4 정리
```powershell
docker compose -f docker/was/docker-compose.yml down
```

## 6. 트레이드오프

| 결정 | 채택 | 대안 | 사유 |
|---|---|---|---|
| 빌드 방식 | 멀티스테이지 (JDK 빌드 + JRE 런타임) | 단일 JDK 이미지 | 이미지 크기/공격 표면 감소. 추가 복잡도는 미미 |
| 빌드 도구 위치 | 컨테이너 내부 gradlew 실행 | 호스트 빌드 산출물을 COPY | 빌드 재현성 보장(로컬 JDK 버전 의존 X). 빌드 시간은 다소 증가 |
| 테스트 실행 | `-x test`로 스킵 | 빌드 시 테스트 동시 실행 | 컨테이너 빌드는 배포용. 테스트는 별도 CI/로컬 단계에서 수행하는 것이 일반 관행 |
| 네트워크 | 기존 `argus-net`, `prometheus-net` external 참조 | was 전용 새 네트워크 생성 후 인프라 compose 수정 | 다른 Task의 compose 파일 수정 없이 분리된 변경 — Task 범위 준수 |
| depends_on | 미사용 | external 컨테이너에 health 기반 대기 추가 | 다른 compose 그래프의 서비스는 depends_on 대상이 될 수 없음. 어플리케이션 자체 재시도에 위임 |
| application.yml 수정 | 미수정, 환경변수로 오버라이드 | application-docker.yml profile 추가 | Task 범위 최소화. relaxed-binding으로 충분히 처리 가능 |
| `.env` 추적 | git 추적 제외, `.env.example`만 커밋 | `.env` 자체를 커밋 | 비밀값 노출 방지. 신규 클론 환경에서 복사 한 줄로 셋업 |
| Prometheus 서버 | 우리가 운영하는 컨테이너로 참조, argus가 `prometheus:9090` 직접 호출 | `.env`로 외부 주소 주입 | 사용자 명시 — prometheus는 우리가 운영, targets만 사용자 영역 |
| `docker/prometheus/targets.yml` | 미수정, 사용자가 직접 갱신 | 함께 갱신 | 사용자가 운영하는 외부 어플리케이션 목록은 사용자 책임 |
| argus의 prometheus-net join | 포함 | 미포함 후 host.docker.internal 우회 | 컨테이너 DNS 사용이 더 단순/안정적, 양 compose가 모두 docker 환경 |
| pythia의 prometheus-net join | 미포함 | 함께 join | pythia는 prometheus 호출/노출 경로가 없음 (application.yml 기준) |

## 7. 작업 순서

1. `argus/Dockerfile`, `argus/.dockerignore` 작성
2. `pythia/Dockerfile`, `pythia/.dockerignore` 작성
3. `docker/was/.env.example` 작성
4. `docker/was/docker-compose.yml` 작성
5. 검증 절차(5.1 → 5.4) 수행
