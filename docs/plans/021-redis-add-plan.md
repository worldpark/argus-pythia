# 021. pythia 프로젝트에 Redis 모듈 추가 - Plan

> Source Task: docs/tasks/021-redis-add.md
> Target: pythia
> 본 Plan은 Redis 연동 기반(의존성/설정/구성 Bean/Docker Compose)만 추가하며 비즈니스 로직은 후속 Task로 분리한다.

---

## 1. 구현 목표
pythia 애플리케이션에 Spring Data Redis 연동 기반을 마련한다. 이후 Task에서 RedisTemplate / StringRedisTemplate 을 즉시 주입하여 캐시, Pub/Sub, 분산 락 등의 기능을 확장할 수 있는 최소 구성을 제공한다.

---

## 2. 설계 방식 및 이유

### 2.1 Starter 선택: spring-boot-starter-data-redis
- Spring Boot 4.0.6 라인에서 Redis 연동의 표준 진입점.
- Lettuce 클라이언트를 기본 포함하므로 별도 의존성 추가 없이 connection factory 가 자동 구성된다.
- 후속 Task 에서 @EnableCaching, RedisCacheManager, RedisMessageListenerContainer 등을 자연스럽게 확장할 수 있다.

### 2.2 클라이언트: Lettuce (기본 유지)
- Spring Boot 가 제공하는 기본값. Netty 기반 Non-blocking, Thread-safe 하여 단일 인스턴스 공유 가능.
- Jedis 는 별도 의존성 + thread pool 튜닝 필요. 본 Task 범위(연동 기반)에서 Jedis 로 전환할 동기 없음.
- 후속 Task 에서 클라이언트 교체가 필요해지면 의존성/Bean 만 교체하면 되므로 결정을 미룰 수 있다.

### 2.3 RedisTemplate Bean 명시적 구성 여부
- Spring Boot autoconfig 가 RedisTemplate<Object, Object> 와 StringRedisTemplate 을 이미 제공한다.
- 그러나 본 Task 에서 명시적으로 RedisConfig 를 추가한다. 이유:
  1. CLAUDE.md 의 패키지/Config 패턴 ({도메인}.config.*Config) 을 통일적으로 따르기 위함.
  2. 후속 Task 에서 Key/Value 직렬화 커스터마이즈 (StringRedisSerializer + GenericJackson2JsonRedisSerializer) 를 일관된 위치에서 확장할 수 있도록 한다.
  3. 본 Plan 단계에서는 autoconfig 가 제공하는 RedisConnectionFactory 를 그대로 위임하고, StringRedisTemplate 만 @Bean 으로 노출해 Bean 이 정상 생성됨을 검증 가능하게 만든다.
- 직렬화 커스터마이즈, Pub/Sub Listener, @EnableCaching 은 본 Task out of scope.

### 2.4 환경변수 분리 전략
- application.yml 의 모든 Redis 항목은 ${REDIS_HOST:localhost} 형태의 placeholder + 기본값 패턴을 사용한다 (기존 ${DB_USERNAME:pythia} 패턴과 동일).
- 운영/로컬 환경 분기는 Spring Boot 표준의 환경변수 주입으로 처리 가능하며, profile 별 yml 분리는 본 Task 에서 도입하지 않는다.

### 2.5 테스트 환경 처리 전략 (가장 중요한 결정)
- 기존 application-test.yml 은 spring.autoconfigure.exclude 로 Kafka/Mail autoconfig 를 비활성화하여 외부 의존성 없이 컨텍스트가 뜨도록 구성되어 있다.
- Redis 도 동일하게 테스트 프로파일에서 autoconfig 를 exclude 한다.
  - 추가 대상: org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration
  - 추가 대상: org.springframework.boot.autoconfigure.data.redis.RedisRepositoriesAutoConfiguration (사용 안 함)
- RedisConfig 는 운영 환경에서만 동작하면 되므로 @ConditionalOnBean(RedisConnectionFactory.class) 로 조건부 등록한다.
  - 선택 이유: autoconfig 가 꺼지면 connection factory bean 이 없으므로 자연스럽게 RedisConfig 내부 Bean 도 등록되지 않는다. 테스트 환경에서 어떤 Redis 관련 Bean 도 생성되지 않아 H2 기반 테스트에 영향 없음.


---

## 3. 영향 범위

### 3.1 신규 파일
- pythia/src/main/java/com/example/pythia/redis/config/RedisConfig.java
- docker/redis/docker-compose.yml

### 3.2 수정 파일
- pythia/build.gradle  --  Redis starter 의존성 추가
- pythia/src/main/resources/application.yml  --  spring.data.redis.* 연결 설정 추가
- pythia/src/test/resources/application-test.yml  --  Redis autoconfig exclude 추가

### 3.3 신규 테스트 (선택)
- 본 Task 에서는 PythiaApplicationTests.contextLoads 통과로 충분하다고 판단한다. (테스트 프로파일에서 Redis autoconfig 가 exclude 되므로 RedisConfig Bean 도 등록되지 않아 신규 테스트의 검증 가치가 낮다.)
- 운영 환경 검증은 수동으로 ./gradlew :pythia:bootRun 후 로그에서 RedisConnectionFactory 초기화 로그 확인.

---

## 4. 구현 상세

### 4.1 pythia/build.gradle
- 역할: Redis starter 의존성 추가.
- 변경 내용:
  - 의존성 한 줄 추가: implementation "org.springframework.boot:spring-boot-starter-data-redis" (실제 코드에서는 기존 스타일대로 작은따옴표 사용)
- 위치: 기존 spring-boot-starter-data-jpa 라인 근처. 기능별 그룹핑/알파벳 순서를 따른다.
- Lettuce 는 starter 에 포함되므로 별도 명시 불필요.

### 4.2 pythia/src/main/resources/application.yml
- 역할: Redis 연결 설정 노출.
- 추가 위치: spring: 블록 하위, kafka: 다음 또는 kafka: 와 mail: 사이.
- 추가 내용 (key/value 만 명세):
  - spring.data.redis.host = ${REDIS_HOST:localhost}
  - spring.data.redis.port = ${REDIS_PORT:6379}
  - spring.data.redis.password = ${REDIS_PASSWORD:}
  - spring.data.redis.timeout = 3s
  - spring.data.redis.lettuce.shutdown-timeout = 200ms
- password 기본값은 빈 문자열 (로컬 compose 에서 인증 미사용).
- timeout 은 명령 timeout. 짧게 두어 장애 전파 빠르게.
- database 인덱스는 기본 0 사용 (명시 생략).
- 후속 Task 에서 connection pool 등 추가 시 lettuce.pool.* 확장.

### 4.3 pythia/src/main/java/com/example/pythia/redis/config/RedisConfig.java
- 역할: 명시적 Redis Bean 등록 진입점. 본 Task 에서는 StringRedisTemplate 만 명시적으로 노출.
- 패키지: com.example.pythia.redis.config
- 어노테이션: @Configuration, @ConditionalOnBean(RedisConnectionFactory.class)
- Bean:
  - StringRedisTemplate stringRedisTemplate(RedisConnectionFactory connectionFactory)
    - 본문: new StringRedisTemplate(connectionFactory)
- 제약:
  - RedisTemplate<Object, Object> 는 Spring Boot autoconfig 가 이미 제공하므로 중복 정의 회피.
  - 직렬화 커스터마이즈 / 객체 직렬화 / Cache / Pub/Sub 등은 본 클래스의 후속 확장 지점으로만 남긴다.

### 4.4 docker/redis/docker-compose.yml
- 역할: 로컬 Redis 기동.
- 구성 원칙: 기존 docker/docker-compose.postgres.yml / docker/kafka/docker-compose.yml 스타일을 따른다.
- 서비스 명세:
  - service name: redis
  - image: redis:7.4-alpine
  - container_name: jvm-monitoring-redis
  - ports: 6379:6379
  - environment: TZ=Asia/Seoul
  - command: redis-server --appendonly yes
  - volumes: redis-data:/data
  - healthcheck: redis-cli ping (interval 10s, timeout 5s, retries 10, start_period 10s)
  - networks: argus-net
- 네트워크 블록:
  - networks.argus-net.name = argus-net, driver = bridge (기존 postgres/kafka compose 와 동일 정의 방식)
- 볼륨 블록:
  - volumes.redis-data.name = redis-data
- AOF (--appendonly yes) 로 재기동 시 데이터 보존 (로컬 개발 편의).
- 인증은 본 Task 범위 외. 운영 적용 시 --requirepass 와 REDIS_PASSWORD 환경변수로 연동.

### 4.5 pythia/src/test/resources/application-test.yml
- 역할: 테스트 컨텍스트에서 Redis autoconfig 비활성화.
- 변경 위치: spring.autoconfigure.exclude 목록 확장.
- 추가 항목:
  - org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration
  - org.springframework.boot.autoconfigure.data.redis.RedisRepositoriesAutoConfiguration
- 기존 exclude (Kafka, Mail) 는 유지.


---

## 5. Spring Boot 컨벤션
- 패키지 구조: com.example.pythia.redis.config (기존 kafka.config, ai.config, alert.config, email.config 패턴 일치).
- 어노테이션: @Configuration, @Bean, @ConditionalOnBean.
- 외부 연동 규칙 일치: Redis 접근은 Repository/Service 에서만 수행될 예정 (본 Task 에서는 등장하지 않음).
- 예외 처리: 본 Task 는 비즈니스 코드를 추가하지 않으므로 CustomException 신설 없음. Redis 연결 실패 시 Spring Boot Lettuce 의 기본 재시도/예외 (RedisConnectionFailureException) 에 의존.

---

## 6. 데이터 흐름
- 현재 Task 시점에는 비즈니스 데이터 흐름이 없다.
- Spring Boot autoconfig 가 LettuceConnectionFactory 를 생성 -> RedisConfig 가 StringRedisTemplate Bean 을 노출 -> 후속 Task 에서 Service 계층이 주입받아 사용.
- 컨텍스트 로드 시점에 connection 이 lazy 로 형성되므로 Redis 미기동 상태에서도 부팅 자체는 통과한다 (첫 명령 호출 시점에 실패). Lettuce 는 기본적으로 lazy connect.

---

## 7. 예외 처리 전략
- 본 Task 에서는 어떤 명령도 실행하지 않으므로 별도 Exception 정의/래핑 없음.
- Spring Boot 기본 동작:
  - 부팅: 컨텍스트는 정상 로드 (lazy connect).
  - 후속 Task 에서 명령 실행 시 RedisConnectionFailureException 또는 QueryTimeoutException 발생 -> 비즈니스 Service 단에서 CustomException 으로 변환 예정.
- 본 Plan 적용 후 Redis 가 꺼져 있어도 애플리케이션 기동/기존 테스트가 깨지지 않아야 한다 (테스트 프로파일 exclude 로 보장).

---

## 8. 검증 방법

### 8.1 빌드 검증
- 명령: ./gradlew :pythia:build
- 기대: 의존성 해석 성공, 컴파일 성공, 기존 모든 테스트 GREEN 유지.

### 8.2 기존 테스트 영향 없음 검증
- 명령: ./gradlew :pythia:test
- 기대: PythiaApplicationTests.contextLoads 를 포함한 전체 테스트가 통과. (테스트 프로파일에서 Redis autoconfig 가 exclude 되므로 LettuceConnectionFactory 가 생성 시도되지 않아 Redis 미기동 환경에서도 무영향.)

### 8.3 Redis 기동 검증
- 명령: docker compose -f docker/redis/docker-compose.yml up -d
- 기대: redis 컨테이너가 healthy 상태가 되고 6379 포트가 노출됨.
- 명령: docker exec redis redis-cli ping -> PONG

### 8.4 애플리케이션 연동 검증 (수동)
- 사전 조건: Redis 컨테이너 기동 상태.
- 명령: ./gradlew :pythia:bootRun
- 기대 로그:
  - LettuceConnectionFactory 초기화 로그.
  - StringRedisTemplate Bean 생성 (DEBUG 로그 활성화 시 확인 가능).
  - 부팅 후 ERROR 없이 정상 동작.
- 자동화된 통합 테스트는 본 Task 에서 도입하지 않는다 (Testcontainers 도입은 별도 Task 로 분리하는 것이 인프라 영향 최소화).


---

## 9. 트레이드오프

### 9.1 Autoconfig 만 사용 vs 명시적 RedisConfig
- 선택: 명시적 RedisConfig (단, 본 Task 에서는 StringRedisTemplate 1개만 노출하여 최소화).
- 이유: 후속 Task 에서 직렬화/캐시/Pub-Sub 확장을 일관된 위치에서 진행하기 위한 hook 확보. 본 Task 에서 Bean 수를 1개로 제한하여 최소한의 Bean 등록 제약 준수.

### 9.2 Lettuce vs Jedis
- 선택: Lettuce.
- 이유: Boot 기본 클라이언트, Non-blocking, 후속 Reactive 전환 여지 보존. Jedis 전환 비용은 단순 (starter exclude + jedis 의존성 명시) 이므로 후속에서 결정 가능.

### 9.3 테스트 시 Redis 처리
- 후보:
  1. Embedded Redis (it.ozimov:embedded-redis 등)  --  외부 라이브러리 의존, Boot 4 호환성 불확실.
  2. Testcontainers  --  도커 데몬 필요, CI 비용 증가.
  3. autoconfig exclude  --  외부 의존성 0, 본 Task 비즈니스 로직 부재로 검증 손실 없음.
- 선택: autoconfig exclude (옵션 3). 본 Task 범위와 가장 정합. 후속 Task 에서 실제 Redis 명령 검증이 필요해지면 Testcontainers 를 별도 Task 로 도입.

### 9.4 인증/TLS
- 로컬 compose 는 인증 없음. 운영 적용 시 requirepass / TLS 는 별도 Task 로 도입. 본 Plan 은 REDIS_PASSWORD placeholder 만 사전 노출.

---

## 10. 영향받는 레이어 및 컨벤션 체크
- Controller / Service / ServiceResponse / Repository / Entity / DTO: 변경 없음.
- 외부 연동 규칙: Redis 직접 호출은 추후 Service/Repository 레이어에서 수행. 본 Task 에서는 호출 경로 미생성.
- CLAUDE.md Docker 파일은 /docker 하위 규칙 준수: docker/redis/docker-compose.yml.
- CLAUDE.md 환경별 compose 파일 분리 준수: Redis 는 자체 디렉터리 (docker/redis/) 로 격리.
- CLAUDE.md 테스트 없이 기능 추가 금지 검토: 본 Task 는 Bean 등록 1건이며 테스트 프로파일에서 비활성화되므로 별도 단위 테스트의 가치가 낮다. 대신 기존 PythiaApplicationTests.contextLoads 가 회귀를 막는다. 설계 결정 근거는 본 Plan 문서로 대체.

---

## 11. 완료 조건 (Acceptance Checklist)
- [ ] pythia/build.gradle 에 spring-boot-starter-data-redis 의존성이 추가됨.
- [ ] pythia/src/main/resources/application.yml 에 spring.data.redis.host/port/password/timeout/lettuce.shutdown-timeout 항목이 환경변수 placeholder 형태로 추가됨.
- [ ] pythia/src/main/java/com/example/pythia/redis/config/RedisConfig.java 가 @ConditionalOnBean(RedisConnectionFactory.class) 와 함께 생성되고 StringRedisTemplate Bean 을 노출함.
- [ ] docker/redis/docker-compose.yml 이 추가되어 redis:7.4-alpine 이 argus-net 네트워크에서 healthcheck 와 함께 기동됨.
- [ ] pythia/src/test/resources/application-test.yml 의 spring.autoconfigure.exclude 에 RedisAutoConfiguration, RedisRepositoriesAutoConfiguration 이 추가됨.
- [ ] ./gradlew :pythia:build 가 성공한다.
- [ ] ./gradlew :pythia:test 가 그린 (특히 PythiaApplicationTests.contextLoads 통과).
- [ ] docker compose -f docker/redis/docker-compose.yml up -d 후 컨테이너가 healthy 상태가 된다.
- [ ] 본 Task 범위에 Controller/Service/Repository 구현, 직렬화 커스터마이즈, @EnableCaching, Pub/Sub, Distributed Lock 코드가 포함되지 않는다 (Out of Scope 준수).
