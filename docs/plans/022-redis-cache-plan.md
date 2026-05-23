# 022. ViolationStateStore in-memory store -> Redis 기반 저장소 대체 — Plan (Redisson 분산 락 채택)

> 대상 모듈: pythia
> 관련 Task: docs/tasks/022-redis-cache.md (Task §6 에서 "Distributed Lock 도입" 항목 제거됨)
> 관련 fix-log: docs/plans/fix-logs/022-redis-cache-distributed-lock-allowed.md
> 선행 Task: 021 (Redis 기반 인프라 도입 — StringRedisTemplate Bean, application-test.yml 의 Redis autoconfig exclude)

---

## 1. 목표 / 비목표

### 1.1 목표
- ViolationStateStore 의 내부 저장소를 ConcurrentHashMap 에서 Redis Hash 기반으로 전환한다.
- 다중 인스턴스/재기동 환경에서도 위반 카운트와 lastSentSeverity 가 일관되게 유지되도록 한다.
- 기존 ViolationStateStore 의 public API 시그니처(shouldSend, clear)와 호출 의미는 그대로 유지한다.
- 위반 누적 / 마지막 발송 갱신 / 발송 여부 판단을 **Redisson 분산 락(RLock)** 으로 원자적으로 처리한다.

### 1.2 비목표
- Alert 임계치 정책/규칙 변경 없음.
- 인터페이스 추상화(ViolationStateStore interface + InMemoryImpl/RedisImpl) 도입 없음.
- @EnableCaching, Pub/Sub, Stream 도입 없음.
- Controller/API/Repository 계층 신설 없음.
- 직렬화 커스터마이즈된 RedisTemplate<Object,Object> Bean 신설 없음. StringRedisTemplate 와 RedissonClient 만 사용.
- Lua script 도입 없음 (대안 검토 후 제외 — 8 절 트레이드오프 참조).

---

## 2. 설계 방식 및 이유

### 2.1 원자성 보장 메커니즘: Redisson 분산 락 (RLock.tryLock)

기존 ConcurrentHashMap.compute 는 다음 5단계를 단일 lock 안에서 원자적으로 처리한다.

1. 현재 카운트(warning/critical), lastSentSeverity 조회
2. 입력 severity 카운트 +1
3. 반대 severity 카운트 0 으로 reset (severity 교차 시 연속성 끊김 의미)
4. count >= window AND lastSent != severity 면 lastSent 갱신 + boolean true
5. 그렇지 않으면 boolean false

Redis 환경에서 동일 의미를 보장하기 위한 후보 비교:

| 후보 | 장점 | 단점 | 채택 |
| --- | --- | --- | --- |
| (A) Redisson 분산 락 (RLock.tryLock) | Java 코드만으로 흐름 유지, try-finally 로 안전 해제, Mockito 단위 테스트 가능, 디버깅/로그 부착 용이 | 신규 의존성, lock 획득에 별도 round-trip, Lettuce 와 별도 connection 풀 | **채택** |
| (B) Lua script (RedisScript<Long> + EVAL/EVALSHA) | 단일 round-trip, 서버측 단일 스레드 보장 | Lua 디버깅/Hot-reload 어려움, Lua 자체 단위 테스트 한계, 통합 테스트 별도 task 필요 | 제외 |
| (C) WATCH/MULTI/EXEC 낙관적 잠금 | 순수 Spring API | 충돌 시 재시도 루프 필요, Connection 모드 전환, 코드 복잡 | 제외 |

-> (A) Redisson 채택. 사유 요약:
- 비즈니스 로직이 Java 안에 머무르므로 디버깅/로그/단위 테스트 가능.
- 알림 평가 빈도가 낮아 lock 획득 round-trip 영향 무시 가능.
- 시도 후속 task 의 통합 검증 부담이 Lua 대비 가벼움.

### 2.2 자료구조: Redis Hash 유지

ViolationState 의 필드는 3개(warningCount, criticalCount, lastSentSeverity)다.

| 후보 | 장점 | 단점 | 채택 |
| --- | --- | --- | --- |
| Redis Hash (HMGET/HSET) — via StringRedisTemplate.opsForHash() | 필드 단위 갱신, 직렬화 비용 없음, 기존 RedisConfig 와 일관 (Lettuce 단일 풀 사용) | 빈 hash 표현 약속 필요(필드 미존재 = 0) | **채택** |
| Redisson RMap<String, String> | Redisson 으로 모든 IO 통일 | StringRedisTemplate 대비 의존성 영향 확장, 기존 RedisConfig 와 일관성 깨짐 | 제외 |
| String + JSON | 자료 응집 | 매 호출 GET -> 파싱 -> SET 필요, 변경 폭 큼 | 제외 |

-> Hash via StringRedisTemplate 채택. Redisson 은 **락 책임만** 부담한다.

Hash 필드 명세:
- warningCount : 정수(문자열로 저장). 미존재 시 0.
- criticalCount : 정수(문자열). 미존재 시 0.
- lastSent : WARNING 또는 CRITICAL 문자열. 미존재(nil) 또는 빈 문자열은 전송 이력 없음.

### 2.3 Redis Key 변환 규칙

- 상태 키 prefix: pythia:alert:violation: (default, properties 외부화)
- 락 키 prefix:   pythia:alert:violation:lock: (default, properties 외부화)
- 포맷:
  - 상태 키: {keyPrefix}{kind}:{application}:{instance}:{sub|-}
  - 락 키:   {lockKeyPrefix}{kind}:{application}:{instance}:{sub|-}
  - kind, application, instance 는 ViolationKey 의 필드(MetricKind enum 의 name(), 문자열).
  - sub 가 null 일 때는 고정 토큰 - 로 치환.
- 콜론 충돌 처리: 각 필드 내부의 : 를 _ 로 단순 치환(sanitize).
- 위 변환은 ViolationStateStore 내부의 private helper 로 캡슐화:
  - private String toRedisKey(ViolationKey key)
  - private String toLockKey(ViolationKey key)
  - private static String sanitize(String value) — null -> -, 그 외 : -> _

### 2.4 TTL 정책

- **상태 키 (Hash)**: slide TTL. 매 shouldSend 호출마다 StringRedisTemplate.expire(key, ttl) 호출. 위반이 멈추면 자동 정리.
- **락 키 (RLock)**: tryLock(waitMs, leaseMs, MILLISECONDS) 의 leaseMs 로 자동 만료. 명시 unlock 실패 시에도 leaseMs 후 강제 해제.

### 2.5 락 정책 외부화

ViolationStateProperties 에 다음을 추가한다.
- lockWait  : Duration (default 200ms) — tryLock 대기 시간.
- lockLease : Duration (default 3000ms) — 락 자동 만료 시간 (HMGET -> HSET -> EXPIRE 시간 대비 충분히 여유).
- 사유:
  - 작업 자체가 수 ms 이내. lease 3s 는 충분한 안전 마진.
  - 동일 키 충돌 시 대기 200ms 안에 해제될 가능성이 매우 높음. 실패 시 즉시 예외 -> 상위 catch & log 에서 흡수.

### 2.6 Redisson 자동 구성

- 의존성 org.redisson:redisson-spring-boot-starter 추가 -> RedissonAutoConfiguration 이 RedissonClient Bean 등록.
- application.yml 의 spring.data.redis.host/port/password 를 Redisson starter 가 자동으로 사용한다.
- **별도 RedissonConfig 클래스 작성하지 않음(권장 안)**. 자동 구성으로 충분.
- 만약 host/port 동기화가 안 되거나 클러스터/Sentinel 분기 요구가 생기면 후속 task 에서 com.example.pythia.redis.config.RedissonConfig 를 신설하여 명시적 Config Bean 을 등록한다. 본 task 범위 외.

---

## 3. 영향 범위

### 3.1 신규 파일

| 경로 | 목적 |
| --- | --- |
| pythia/src/main/java/com/example/pythia/alert/config/ViolationStateProperties.java | TTL / keyPrefix / lockKeyPrefix / lockWait / lockLease 외부화 (ConfigurationProperties) |
| pythia/src/main/java/com/example/pythia/alert/exception/ViolationStateException.java | Redis / 락 오류용 커스텀 예외 (CustomException 상속) |
| pythia/src/main/java/com/example/pythia/alert/exception/ViolationStateErrorCode.java | ErrorCode 구현 enum |

### 3.2 수정 파일

| 경로 | 변경 내용 |
| --- | --- |
| pythia/build.gradle | org.redisson:redisson-spring-boot-starter 의존성 추가 |
| pythia/src/main/java/com/example/pythia/alert/state/ViolationStateStore.java | 내부 ConcurrentHashMap 제거, StringRedisTemplate + RedissonClient + ViolationStateProperties 주입, shouldSend/clear 재구현. public API 시그니처 유지, @Component 유지 |
| pythia/src/main/resources/application.yml | pythia.alert.violation-state.* 항목 추가 |
| pythia/src/test/resources/application-test.yml | Redisson autoconfig exclude 추가 (옵션 비교 후 채택안 — 4.6 절) |
| pythia/src/test/java/com/example/pythia/alert/state/ViolationStateStoreTest.java | Mockito 기반 단위 테스트로 재작성 (11개 케이스, 4.7 절) |
| (필요 시) pythia/src/test/java/com/example/pythia/PythiaApplicationTests.java 또는 별도 TestConfiguration | 컨텍스트 로딩 Mock Bean 제공 (옵션 a 채택 시) |

### 3.3 삭제 파일

| 경로 | 사유 |
| --- | --- |
| pythia/src/main/java/com/example/pythia/alert/state/ViolationState.java | Redis Hash 로 상태가 표현되므로 인메모리 DTO 불필요. dead code 회피. 구현 단계에서 grep 으로 참조 없음 재검증 후 삭제 |

### 3.4 변경 불필요

- pythia/src/main/java/com/example/pythia/alert/service/ThresholdEvaluator.java — shouldSend/clear 시그니처 유지. 기존 RuntimeException catch & log 패턴 유지.
- pythia/src/main/java/com/example/pythia/redis/config/RedisConfig.java — StringRedisTemplate Bean 유지.

---

## 4. 구현 상세 (파일별)

### 4.1 ViolationStateStore.java (수정)

- 패키지: com.example.pythia.alert.state
- 어노테이션: @Component 유지
- 의존성 (생성자 주입):
  - StringRedisTemplate redisTemplate
  - RedissonClient redissonClient
  - ViolationStateProperties properties
- public API (시그니처 변경 없음):
  - boolean shouldSend(ViolationKey key, Severity severity, int window)
  - void clear(ViolationKey key)
- 내부 private helper:
  - private String toRedisKey(ViolationKey key)
  - private String toLockKey(ViolationKey key)
  - private static String sanitize(String value)

#### 4.1.1 shouldSend 의사 코드

~~~java
String redisKey = toRedisKey(key);
String lockKey  = toLockKey(key);
long waitMs  = properties.getLockWait().toMillis();
long leaseMs = properties.getLockLease().toMillis();
Duration ttl = properties.getTtl();

RLock lock = redissonClient.getLock(lockKey);
boolean acquired;
try {
    acquired = lock.tryLock(waitMs, leaseMs, TimeUnit.MILLISECONDS);
} catch (InterruptedException e) {
    Thread.currentThread().interrupt();
    throw new ViolationStateException(ViolationStateErrorCode.LOCK_INTERRUPTED, e);
}
if (!acquired) {
    throw new ViolationStateException(ViolationStateErrorCode.LOCK_ACQUISITION_FAILED);
}

try {
    HashOperations<String, String, String> hash = redisTemplate.opsForHash();

    // 1) 현재 상태 조회 (HMGET)
    List<String> current = hash.multiGet(redisKey,
            List.of("warningCount", "criticalCount", "lastSent"));
    int warnCount  = parseIntOrZero(current.get(0));
    int critCount  = parseIntOrZero(current.get(1));
    String lastSent = current.get(2);  // null 또는 "" 가능

    // 2) severity +1, 반대 severity 0 reset
    if (severity == Severity.WARNING) {
        warnCount += 1;
        critCount  = 0;
    } else {
        critCount += 1;
        warnCount  = 0;
    }

    // 3) 발송 판정
    int activeCount = (severity == Severity.WARNING) ? warnCount : critCount;
    boolean shouldSend = false;
    if (activeCount >= window && !severity.name().equals(lastSent)) {
        lastSent = severity.name();
        shouldSend = true;
    }

    // 4) 저장 (HSET multi field)
    Map<String, String> updates = new LinkedHashMap<>();
    updates.put("warningCount", Integer.toString(warnCount));
    updates.put("criticalCount", Integer.toString(critCount));
    updates.put("lastSent", lastSent == null ? "" : lastSent);
    hash.putAll(redisKey, updates);

    // 5) slide TTL
    redisTemplate.expire(redisKey, ttl);

    return shouldSend;
} catch (DataAccessException | SerializationException e) {
    throw new ViolationStateException(ViolationStateErrorCode.REDIS_ACCESS_FAILED, e);
} catch (RedisException e) {
    // org.redisson.client.RedisException / RedisConnectionException
    throw new ViolationStateException(ViolationStateErrorCode.REDIS_ACCESS_FAILED, e);
} finally {
    if (lock.isHeldByCurrentThread()) {
        try {
            lock.unlock();
        } catch (IllegalMonitorStateException ignored) {
            // 이미 만료된 락은 무시
        }
    }
}
~~~

private helper parseIntOrZero(String s): null/빈문자/숫자 아님 -> 0. 그 외 Integer.parseInt(s).

#### 4.1.2 clear 의사 코드

~~~java
String redisKey = toRedisKey(key);
try {
    redisTemplate.delete(redisKey);
} catch (DataAccessException e) {
    throw new ViolationStateException(ViolationStateErrorCode.REDIS_ACCESS_FAILED, e);
}
~~~

clear 는 idempotent DEL 이므로 락 없이 수행한다. race 가 발생해도 결과는 "키 삭제" 단일 상태로 수렴한다.

### 4.2 ViolationStateProperties.java (신규)

- 패키지: com.example.pythia.alert.config
- @ConfigurationProperties(prefix = "pythia.alert.violation-state")
- @EnableConfigurationProperties(ViolationStateProperties.class) 부착 위치: 기존 alert 도메인의 설정 클래스가 있다면 거기에 추가, 없다면 AnalysisWindowProperties 등이 등록된 동일한 방식(메인 애플리케이션 클래스 또는 별도의 작은 @Configuration 클래스)을 따른다. 본 task 에서는 기존 컨벤션과 동일한 위치를 선택한다.
- 필드:
  - private Duration ttl = Duration.ofHours(1);
  - private String keyPrefix = "pythia:alert:violation:";
  - private String lockKeyPrefix = "pythia:alert:violation:lock:";
  - private Duration lockWait = Duration.ofMillis(200);
  - private Duration lockLease = Duration.ofMillis(3000);
- getter/setter 또는 record + ConfigurationProperties 바인딩은 기존 코드 스타일(예: AnalysisWindowProperties 등)을 그대로 따른다.

### 4.3 예외 클래스 (신규)

#### 4.3.1 ViolationStateErrorCode

- 패키지: com.example.pythia.alert.exception
- enum implements ErrorCode
- 상수:
  - REDIS_ACCESS_FAILED("ALERT-STATE-001", "Failed to access Redis for violation state")
  - LOCK_ACQUISITION_FAILED("ALERT-STATE-002", "Failed to acquire violation state lock")
  - LOCK_INTERRUPTED("ALERT-STATE-003", "Interrupted while acquiring violation state lock")
- code(), defaultMessage() 구현.

#### 4.3.2 ViolationStateException

- 패키지: com.example.pythia.alert.exception
- extends CustomException
- 생성자:
  - public ViolationStateException(ViolationStateErrorCode code)
  - public ViolationStateException(ViolationStateErrorCode code, Throwable cause)
- 기존 ThresholdConfigException 의 구조를 그대로 따른다.

### 4.4 application.yml 추가 항목

~~~yaml
pythia:
  alert:
    violation-state:
      ttl: 1h
      key-prefix: "pythia:alert:violation:"
      lock-key-prefix: "pythia:alert:violation:lock:"
      lock-wait: 200ms
      lock-lease: 3s
~~~

(키만 명시. 값은 default 와 동일하나 운영 환경 분리를 위해 등록.)

### 4.5 build.gradle 의존성

pythia/build.gradle 의 dependencies { ... } 블록에 다음 추가:

~~~gradle
implementation "org.redisson:redisson-spring-boot-starter:3.39.0"
~~~

버전 선정 가이드:
- Spring Boot 4.0.x 가 일반화된 시점의 최신 안정 버전을 사용한다.
- 예시 후보: redisson-spring-boot-starter:3.39.0 (Spring Boot 3.x 계열 호환 BOM 동봉) 또는 그 시점의 최신 안정 버전.
- Spring Boot 4 와의 호환성은 implementor 가 의존성 추가 직후 ./gradlew :pythia:dependencies / :pythia:compileJava / :pythia:test 로 검증. 만약 starter 가 Boot 4 와 충돌 시 임시 대안으로 org.redisson:redisson:3.39.0 (starter 없이 코어) + 명시적 RedissonConfig Bean 패턴으로 폴백한다.
- 폴백이 필요한 경우 implementor 는 해당 사실을 fix-log 로 남긴다.

repositories 는 기존 mavenCentral() 로 충분 (Redisson 은 Maven Central 배포).

### 4.6 application-test.yml 갱신 (옵션 비교 + 채택)

문제: Redisson starter 가 추가되면 RedissonAutoConfiguration 이 컨텍스트 로딩 시 RedissonClient Bean 생성 시도 -> PythiaApplicationTests.contextLoads() 등 SpringBootTest 컨텍스트가 Redis 없이 깨질 위험.

#### 옵션 비교

| 옵션 | 방식 | 장점 | 단점 | 채택 |
| --- | --- | --- | --- | --- |
| (a) Redisson autoconfig **exclude** + 컨텍스트 테스트에 Mock Bean 제공 | application-test.yml 의 spring.autoconfigure.exclude 에 Redisson 관련 AutoConfiguration 추가 + 필요한 @SpringBootTest 클래스에 @MockitoBean RedissonClient, @MockitoBean StringRedisTemplate 부착 | Redis/Redisson 인프라 없어도 컨텍스트 로딩 100% 통과 | 영향받는 컨텍스트 테스트마다 Mock Bean 선언 필요. AutoConfiguration FQCN 변경에 취약 | **채택** |
| (b) autoconfig exclude 제거 + lazy connect 신뢰 | Lettuce/Redisson 모두 lazy connect 동작으로 컨텍스트 로딩만 통과시킴 | application-test.yml 단순 | Redisson 의 lazy connect 동작이 starter 버전/구성에 따라 다를 수 있어 컨텍스트 로딩 실패 가능. CI 환경 차이로 flaky 위험 | 제외 |

-> **(a) 채택**. 사유: 결정성/재현성 우선. 컨텍스트 로딩 단계에서 Redis/Redisson 가 전혀 없이도 contextLoads 가 통과해야 한다는 기존 021 규칙과 동일선상.

#### 적용 내용

pythia/src/test/resources/application-test.yml 의 spring.autoconfigure.exclude 항목에 다음을 추가:

~~~yaml
spring:
  autoconfigure:
    exclude:
      - org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration
      - org.springframework.boot.autoconfigure.data.redis.RedisRepositoriesAutoConfiguration
      # Redisson auto-config (starter 버전에 따라 FQCN 달라질 수 있음. implementor 가 의존성 추가 후 정확히 식별)
      - org.redisson.spring.starter.RedissonAutoConfiguration
      - org.redisson.spring.starter.RedissonAutoConfigurationV2
      # (참고 후보) org.redisson.spring.boot.starter.RedissonAutoConfiguration — 일부 starter 버전에서 사용. implementor 가 실제 패키지 확인 후 정리
~~~

implementor 는 starter 의존성 추가 직후 jar 내부의 META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports (또는 legacy META-INF/spring.factories) 를 열어 실제 FQCN 을 확인하여 위 후보 중 일치하는 항목만 남기고 나머지는 제거한다. 최소 1개는 반드시 exclude 되어야 한다.

#### Mock Bean 제공

영향받는 @SpringBootTest 류 테스트 (PythiaApplicationTests 등) 에 다음 패턴 적용:

~~~java
@SpringBootTest
@ActiveProfiles("test")
class PythiaApplicationTests {

    @MockitoBean
    private RedissonClient redissonClient;

    @MockitoBean
    private StringRedisTemplate stringRedisTemplate;

    @Test
    void contextLoads() {
        // 기존 동작 유지
    }
}
~~~

별도 @TestConfiguration 클래스로 추출해도 무방. 본 plan 에서는 가장 호출 빈도가 낮은 @MockitoBean 직접 부착을 권장.

### 4.7 테스트 재작성 (ViolationStateStoreTest.java)

#### 4.7.1 테스트 전략

- **Mockito 기반 단위 테스트** (Testcontainers/Embedded 미사용).
- Mock 대상:
  - RedissonClient redissonClient
  - RLock rLock (redissonClient.getLock(any) 반환값)
  - StringRedisTemplate redisTemplate
  - HashOperations<String, String, String> hashOperations (redisTemplate.opsForHash() 반환값)
- 실제 객체:
  - ViolationStateProperties (직접 인스턴스화 + 기본값 셋업)
  - ViolationStateStore (생성자 주입으로 직접 생성)

#### 4.7.2 테스트 케이스 (최소 11개)

| # | 케이스 | 검증 포인트 |
| --- | --- | --- |
| 1 | shouldSend — lock 성공 + Hash 빈 상태 + severity=WARNING + window=1 | true 반환, hash.putAll 에 warningCount="1" / criticalCount="0" / lastSent="WARNING" 전달, redisTemplate.expire 호출, lock.unlock 호출 |
| 2 | shouldSend — lastSent=WARNING 이미 존재 + 동일 severity 재호출 | false 반환, warningCount 는 +1 되어 putAll 전달, lastSent 는 "WARNING" 유지 |
| 3 | shouldSend — severity 교차 (현재 WARNING 누적 후 CRITICAL 입력) | warningCount=0 reset, criticalCount=1 로 갱신, lastSent 갱신 여부는 window 충족 시에만 |
| 4 | shouldSend — window 미달 (count < window) | false 반환, putAll/expire 정상 호출, lastSent 변경 없음 |
| 5 | shouldSend — lock 획득 실패 (tryLock 이 false 반환) | ViolationStateException(LOCK_ACQUISITION_FAILED) 발생. hash 접근 일절 없음 |
| 6 | shouldSend — tryLock 이 InterruptedException 던짐 | ViolationStateException(LOCK_INTERRUPTED) 발생, Thread.currentThread().isInterrupted() true 검증 |
| 7 | shouldSend — hashOperations.multiGet 호출 시 DataAccessException | ViolationStateException(REDIS_ACCESS_FAILED) 변환, finally 에서 unlock 호출 검증 |
| 8 | shouldSend — finally unlock 동작 | (a) isHeldByCurrentThread()=true -> unlock 1회, (b) isHeldByCurrentThread()=false -> unlock 호출 안 함 (두 케이스 분리 또는 한 케이스 안에서 분기 검증) |
| 9 | clear — redisTemplate.delete(올바른 key) 호출 검증 | sanitize 된 키로 delete 호출됨 |
| 10 | clear — DataAccessException 발생 시 ViolationStateException(REDIS_ACCESS_FAILED) 변환 | |
| 11 | Key sanitization — sub=null -> -, sub="ns:foo" -> ns_foo | shouldSend / clear 양쪽에서 keys 인자 검증 |

#### 4.7.3 단위 테스트 커버리지 개선 사항 (Lua 대비)

- Lua 채택 시 단위 테스트로 검증 불가능했던 항목 (severity +1, reset, lastSent 가드, window 비교) 이 **Java 코드 내부 로직** 으로 이동했으므로 Mockito 만으로 핵심 비즈니스 로직 검증이 가능하다.
- 통합 테스트(실제 Redis/Redisson 호출) 는 별도 task 로 분리 — 본 task 범위 외.

---

## 5. 데이터 흐름

~~~
ThresholdEvaluator
   -> ViolationStateStore.shouldSend(key, severity, window)
         -> toRedisKey(key) -> "pythia:alert:violation:JVM:app1:inst1:-"
         -> toLockKey(key)  -> "pythia:alert:violation:lock:JVM:app1:inst1:-"
         -> redissonClient.getLock(lockKey).tryLock(200ms, 3s, MS)
              -> 실패: ViolationStateException(LOCK_ACQUISITION_FAILED)
              -> 인터럽트: ViolationStateException(LOCK_INTERRUPTED) + interrupt flag 복원
         -> [락 보유 구간]
              1) HMGET redisKey [warningCount, criticalCount, lastSent]
              2) severity +1, 반대 severity 0 reset
              3) activeCount >= window AND lastSent != severity 면 lastSent 갱신 + shouldSend=true
              4) HSET (3 필드 일괄) redisKey
              5) EXPIRE redisKey ttl
              6) return boolean
         -> finally: if lock.isHeldByCurrentThread() then unlock

ThresholdEvaluator
   -> ViolationStateStore.clear(key)
         -> toRedisKey(key) 만 사용 (락 불필요)
         -> redisTemplate.delete(redisKey)   // idempotent DEL
~~~

---

## 6. 예외 처리 매핑표

| 발생 지점 | 원인 예외 | 변환 후 예외 / ErrorCode |
| --- | --- | --- |
| lock.tryLock(...) 반환 false | (정상 반환) | ViolationStateException(LOCK_ACQUISITION_FAILED) |
| lock.tryLock(...) 던지는 InterruptedException | InterruptedException | ViolationStateException(LOCK_INTERRUPTED) + Thread.currentThread().interrupt() |
| redisTemplate.opsForHash().multiGet/putAll, redisTemplate.expire, redisTemplate.delete 에서 DataAccessException 자손 (RedisConnectionFailureException, RedisSystemException, QueryTimeoutException) | DataAccessException | ViolationStateException(REDIS_ACCESS_FAILED) |
| Spring SerializationException | SerializationException | ViolationStateException(REDIS_ACCESS_FAILED) |
| Redisson org.redisson.client.RedisException / RedisConnectionException | RedisException | ViolationStateException(REDIS_ACCESS_FAILED) |
| finally lock.unlock() 의 IllegalMonitorStateException (lease 만료 후 unlock 시도) | IllegalMonitorStateException | 무시 (로그도 선택적) — 락이 이미 자동 해제된 정상 케이스 |

호출자 영향:
- ThresholdEvaluator.evaluate(...) 는 이미 try { ... } catch (RuntimeException e) { log.error(...) } 패턴을 가지므로 모든 ViolationStateException 은 상위 흐름(스케줄/메시지 컨슈머)으로 전파되지 않는다. 호출부 변경 없음.
- 향후 GlobalExceptionHandler 에서 ViolationStateException 을 ErrorCode 기반으로 매핑하고 싶을 때 즉시 가능.

---

## 7. 검증 방법

### 7.1 자동 검증

- ./gradlew :pythia:build 성공 (테스트 포함).
- 재작성된 ViolationStateStoreTest GREEN (4.7 절 11개 케이스).
- PythiaApplicationTests.contextLoads() GREEN — Mock Bean 부착으로 Redis/Redisson 인프라 없이 통과.
- 기존 ThresholdEvaluatorTest (있을 경우) GREEN 유지. ViolationStateStore 를 Mock 으로 주입하는 형태라면 영향 없음. 구현 단계에서 실제 의존 형태를 재확인하여 fix-log 또는 PR description 에 보고한다.

### 7.2 수동 검증 (PR 전 권장, 자동화는 후속 task)

- 로컬 Redis 컨테이너 기동 -> pythia 부팅 -> ThresholdEvaluator 흐름 트리거.
- redis-cli HGETALL pythia:alert:violation:JVM:app:inst:- 로 카운트/lastSent 확인.
- redis-cli PTTL <key> 로 슬라이딩 TTL 갱신 확인.
- severity 교차 시나리오 (WARNING -> CRITICAL -> WARNING) 에서 반대 severity 카운트가 0 으로 리셋되는지 확인.
- 동시 호출 시나리오 (스크립트로 2개 프로세스 동시 호출) 에서 pythia:alert:violation:lock:... 키가 잠시 존재했다 사라지는지, 카운트가 정확히 +1씩 누적되는지 확인.
- clear 트리거 시 redis-cli EXISTS <key> 가 0 으로 떨어지는지 확인.

---

## 8. 트레이드오프

| 결정 | 선택 | 대안 | 사유 / 단점 |
| --- | --- | --- | --- |
| 원자성 | **Redisson 분산 락 (RLock.tryLock)** | Lua script / WATCH-MULTI-EXEC | Java 안에서 로직 유지 -> 디버깅/테스트 용이. 단점: 신규 의존성, lock 획득 round-trip 추가, Lettuce 와 별도 connection 풀 |
| 자료구조 | Redis Hash via StringRedisTemplate | Redisson RMap / JSON String | 기존 RedisConfig 와 일관 (Lettuce 단일 풀). 단점: Redisson 락 / Lettuce IO 가 동일 Redis 인스턴스를 가리키되 connection 풀은 별개 — 운영 영향 미미하나 명시 |
| TTL | slide (매 호출 EXPIRE) | fixed | 위반이 멈추면 자동 정리. 단점: 만료 직전 발송 후 호출 멈추면 다음 사이클에서 카운트 0부터 시작 — 의도된 동작 |
| 락 정책 | lockWait=200ms, lockLease=3s (외부화) | 하드코딩 / 더 긴 lease | 작업 자체가 수 ms -> lease 3s 는 충분 + 안전 마진. 너무 짧으면 작업 중 자동 해제 위험, 너무 길면 장애 시 해제 지연 |
| Redisson 구성 | starter 자동 구성 (spring.data.redis.* 재사용) | 명시적 RedissonConfig + Config Bean | 단순성 우선. 클러스터/Sentinel 분기 요구 시 후속 task 에서 도입 |
| 테스트 | Mockito 단위 테스트 (11개) | Testcontainers / Embedded Redis | 인프라 비용 0, 빠른 실행. **Lua 채택 대비 핵심 로직 커버리지 개선** (severity reset / window / lastSent 가드 모두 단위 테스트 검증 가능). 통합 테스트는 별도 task |
| 설정 외부화 | properties (5개 항목) | 상수 하드코딩 | 설정 항목 증가하나 운영 친화 |
| 추상화 | 단일 클래스 내부 교체 | ViolationStateStore interface + Redis*Impl / InMemory*Impl | Task 3 절 "저장소 대체에 집중" 명시 -> 과도한 설계 회피 |
| ViolationState.java | 삭제 | 유지 | dead code 회피. 향후 인메모리 fallback 시 재작성 필요(Out of Scope) |
| Key sanitization | 콜론 -> 언더스코어 단순 치환 | URL-encode / SHA1 hash | 충돌 가능성 매우 낮은 환경. 충돌 시 별도 task 에서 hash 기반으로 강화 |
| clear 시 락 사용 여부 | 락 없이 단순 DEL | 락 후 DEL | DEL 은 idempotent. race 발생해도 단일 상태(키 없음)로 수렴 -> 단순 DEL 선택 |
| application-test.yml | Redisson autoconfig exclude + Mock Bean | exclude 제거 + lazy connect | 결정성/재현성 우선. AutoConfiguration FQCN 변경에 취약하나 implementor 가 의존성 추가 직후 확인 |

---

## 9. 완료 조건 (체크리스트)

- [ ] pythia/build.gradle 에 org.redisson:redisson-spring-boot-starter 의존성이 추가되고 ./gradlew :pythia:compileJava 가 통과한다.
- [ ] ViolationStateStore 가 ConcurrentHashMap 의존을 모두 제거하고 StringRedisTemplate + RedissonClient + ViolationStateProperties 만 주입한다.
- [ ] shouldSend(ViolationKey, Severity, int) 및 clear(ViolationKey) 의 시그니처가 유지된다.
- [ ] shouldSend 가 RLock.tryLock(waitMs, leaseMs, MILLISECONDS) 패턴으로 락을 획득하고, try-finally 에서 isHeldByCurrentThread()=true 일 때만 unlock 한다.
- [ ] severity 교차 시 반대 severity 카운트가 0 으로 reset 되는 의미가 Java 로직 안에서 보장된다.
- [ ] lastSent != severity AND activeCount >= window 조건에서만 true 가 반환되고, lastSent 가 갱신된다.
- [ ] Redis Hash 필드 (warningCount, criticalCount, lastSent) 가 명시된 규칙대로 저장된다.
- [ ] 매 shouldSend 호출마다 redisTemplate.expire(redisKey, ttl) 가 호출되어 TTL 이 갱신된다(slide).
- [ ] 락 키 prefix pythia:alert:violation:lock: 가 상태 키 prefix 와 분리되어 사용된다.
- [ ] TTL / keyPrefix / lockKeyPrefix / lockWait / lockLease 가 ViolationStateProperties 로 외부화되고 default 가 명시된 값(1h / pythia:alert:violation: / pythia:alert:violation:lock: / 200ms / 3s)이다.
- [ ] Redis 접근/직렬화/락 예외가 ViolationStateException (with ViolationStateErrorCode) 으로 변환된다 — REDIS_ACCESS_FAILED / LOCK_ACQUISITION_FAILED / LOCK_INTERRUPTED 3종.
- [ ] LOCK_INTERRUPTED 변환 시 Thread.currentThread().interrupt() 로 interrupt flag 가 복원된다.
- [ ] ViolationState.java 가 삭제되고(또는 사용처가 없음이 재검증되고), 컴파일 에러가 없다.
- [ ] ViolationStateStoreTest 가 Mockito 기반으로 재작성되어 4.7 절 11개 케이스가 통과한다.
- [ ] PythiaApplicationTests.contextLoads() 가 Redisson autoconfig exclude + Mock Bean 부착으로 Redis/Redisson 인프라 없이 통과한다.
- [ ] application.yml 에 pythia.alert.violation-state.{ttl, key-prefix, lock-key-prefix, lock-wait, lock-lease} 항목이 등록된다.
- [ ] application-test.yml 에 Redisson 관련 AutoConfiguration FQCN 이 spring.autoconfigure.exclude 에 추가된다 (실제 FQCN 은 implementor 가 starter 의존성 추가 직후 확인).
- [ ] ThresholdEvaluator 호출부에 코드 변경이 없다.
- [ ] ./gradlew :pythia:build 가 성공한다.
- [ ] PR description 에 다음이 명시된다:
  - 원자성 메커니즘이 Lua -> Redisson 으로 전환된 배경(fix-log 링크)
  - Mockito 단위 테스트로 핵심 로직이 모두 커버됨 (Lua 대비 커버리지 개선)
  - 실제 Redis 통합 테스트는 후속 task 에서 도입 예정
