# Task 013: Pythia Kafka Consumer 환경 구성 — 구현 계획

## Context

Argus는 Task 007/012로 `jvm.metrics.raw`, `http.metrics.raw`, `hikari.metrics.raw` 세 토픽에 `JacksonJsonSerializer` 기반 메시지를 발행하는 파이프라인을 갖췄으나, 수신 측인 Pythia에는 아직 Kafka Consumer 환경이 없다. Pythia에는 Task 011 미러링 결과로 `kafka.dto.{jvm,http,hikari}` 패키지에 세 DTO record가 이미 자리잡았지만(`build.gradle`에 spring-kafka 미추가, `application.yml`은 `spring.application.name`만 존재), 메시지를 받아들일 listener·config·역직렬화 설정이 모두 비어있다. 본 task는 이 빈 자리를 채워 세 토픽이 정상적으로 소비되는 출발점(=후속 임계값/룰 처리의 진입점)을 마련한다.

---

## 1. 설계 방식 및 이유

### 방향: 토픽별 Consumer 클래스 1:1 분리 + 타입별 ContainerFactory

| 결정 | 선택 | 이유 |
|------|------|------|
| Consumer 단위 | **도메인별 Consumer 3개 (`Jvm/Http/Hikari`)** | Argus의 Producer 1:1 패턴(`*MetricSnapshotProducer`) 미러링. 컴파일 타임에 토픽 ↔ DTO가 묶여 잘못된 라우팅 차단. 후속 Task(임계값/룰)에서 도메인별 의존 주입이 쉬워짐 |
| 역직렬화 | **`JacksonJsonDeserializer<T>` (타입 명시 생성자)** | Pythia CLAUDE.md 규칙: Spring Boot 4 환경에서 `JsonDeserializer` deprecated → `JacksonJsonDeserializer` 사용. Producer가 `spring.json.add.type.headers: false`로 발행하므로 헤더 의존 없이 타입 강제 가능 |
| ContainerFactory | **DTO 타입별 3개 빈 등록** (`jvmKafkaListenerContainerFactory` 등) | DTO별 타입 안전성 확보. `@KafkaListener(containerFactory = "...")`로 명시 바인딩 → 토픽 ↔ 디시리얼라이저 정합성 보장 |
| `bootstrap-servers` | **`application.yml`의 `spring.kafka.bootstrap-servers: localhost:9092`** | Task Context 명시 값. Argus 측과 동일 키 위치(스프링 자동 구성 기본 키) |
| group-id | **`spring.kafka.consumer.group-id: pythia`** | 단일 그룹으로 시작 — 동일 인스턴스가 모든 토픽 파티션을 함께 받음. 추후 도메인별 분리 필요 시 `@KafkaListener(groupId=...)` 오버라이드 가능 |
| auto-offset-reset | **`earliest`** | 개발/검증 단계에서 누락 메시지 없이 재현 가능. 운영 전환 시 `latest`로 조정 가능 |
| Listener 본문 | **로깅 + 후속 hook을 위한 빈 메서드 자리만** | Task 명세는 "정상 consume"까지가 AC. 실제 알림/저장은 후속 Task. 본 task에서 서비스 의존을 만들면 책임 범위 초과 |
| ErrorHandlingDeserializer | **본 task에서는 래핑하지 않음** | poison pill 처리, DLQ는 후속 task 영역. 기본 컨테이너 에러 핸들러로 시작하여 회귀 영향 최소화 |
| 패키지 위치 | **`com.example.pythia.kafka.consumer`** | Task 제약("별도의 consumer 패키지 생성")에 맞춤. 기존 DTO 패키지(`kafka.dto`)와 동일 루트(`kafka`) 아래 형제 패키지로 위치 |
| Config 위치 | **`com.example.pythia.kafka.config`** | DTO/Consumer와 한 루트(`kafka`) 아래 모음. 추후 Producer가 추가될 일은 거의 없으나 패키지 분리는 책임 명확 |

---

## 2. 구성 요소

### 신규 파일

**`pythia/build.gradle` (수정)**
- 의존성 추가: `implementation 'org.springframework.kafka:spring-kafka'` (또는 `org.springframework.boot:spring-boot-starter-kafka`)
- 테스트 의존성(선택): `testImplementation 'org.springframework.kafka:spring-kafka-test'` — `@EmbeddedKafka` 통합 테스트가 필요할 때만

**`pythia/src/main/resources/application.yml` (수정)**
```yaml
spring:
  application:
    name: pythia
  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      group-id: pythia
      auto-offset-reset: earliest
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      # value-deserializer는 ContainerFactory별로 코드에서 명시 (JacksonJsonDeserializer<T>)

pythia:
  kafka:
    topic:
      jvm-metrics-raw: jvm.metrics.raw
      http-metrics-raw: http.metrics.raw
      hikari-metrics-raw: hikari.metrics.raw
```

**`com.example.pythia.kafka.config.KafkaConsumerConfig.java` — `@Configuration`**

목적: 타입별 ConsumerFactory + ConcurrentKafkaListenerContainerFactory 빈 3쌍 등록.

| 빈 이름 | 타입 |
|---------|------|
| `jvmConsumerFactory` | `ConsumerFactory<String, JvmMetricSnapshotDto>` |
| `jvmKafkaListenerContainerFactory` | `ConcurrentKafkaListenerContainerFactory<String, JvmMetricSnapshotDto>` |
| `httpConsumerFactory` | `ConsumerFactory<String, HttpMetricSnapshotDto>` |
| `httpKafkaListenerContainerFactory` | `ConcurrentKafkaListenerContainerFactory<String, HttpMetricSnapshotDto>` |
| `hikariConsumerFactory` | `ConsumerFactory<String, HikariMetricSnapshotDto>` |
| `hikariKafkaListenerContainerFactory` | `ConcurrentKafkaListenerContainerFactory<String, HikariMetricSnapshotDto>` |

내부 구조:
- 공통 props 헬퍼(`commonConsumerProps`): `bootstrap-servers`, `group-id`, key deserializer, `auto-offset-reset` 주입(`KafkaProperties` 빈에서 읽음)
- 타입별 팩토리: `new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), new JacksonJsonDeserializer<>(TargetDto.class))`
- 타입별 ContainerFactory: 위 ConsumerFactory를 `setConsumerFactory`로 주입

**`com.example.pythia.kafka.consumer.JvmMetricSnapshotConsumer.java` — `@Component`**
- `@KafkaListener(topics = "${pythia.kafka.topic.jvm-metrics-raw}", containerFactory = "jvmKafkaListenerContainerFactory")`
- 메서드 시그니처: `void consume(@Payload JvmMetricSnapshotDto snapshot, @Header(KafkaHeaders.RECEIVED_KEY) String key, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic)`
- 본문: 수신 로깅(`debug`) + null/FAILED 상태 분기 로깅(`warn`). 후속 처리 hook은 비워둠

**`com.example.pythia.kafka.consumer.HttpMetricSnapshotConsumer.java` — `@Component`** — 동일 구조, `http.metrics.raw` / Http 팩토리 / HttpDto

**`com.example.pythia.kafka.consumer.HikariMetricSnapshotConsumer.java` — `@Component`** — 동일 구조, `hikari.metrics.raw` / Hikari 팩토리 / HikariDto

### 수정하지 않는 파일 (제약/안전)
- 기존 `com.example.pythia.kafka.dto.**` — Task 011 결과물 그대로 사용
- `PythiaApplication.java` — `@SpringBootApplication`만으로 `@KafkaListener` 자동 활성화(`@EnableKafka`는 Spring Boot 자동 구성에서 처리)
- Argus 측 Producer/토픽 설정 — 변경 없음

---

## 3. 데이터 흐름

```
[Argus]                                      [Kafka Broker @ localhost:9092]                  [Pythia]
JvmMetricSnapshotProducer ──send──▶  jvm.metrics.raw    ──poll──▶  jvmConsumerFactory
                                                                          │ (StringDeserializer + JacksonJsonDeserializer<JvmDto>)
                                                                          ▼
                                                          jvmKafkaListenerContainerFactory
                                                                          │
                                                                          ▼
                                                          JvmMetricSnapshotConsumer.consume(snapshot, key, topic)
                                                                          │ debug log + status 분기 warn
                                                                          ▼
                                                                  (후속 Task의 hook)

HttpMetricSnapshotProducer ──send──▶  http.metrics.raw   ──▶ httpConsumerFactory   ──▶ httpKafkaListenerContainerFactory   ──▶ HttpMetricSnapshotConsumer.consume(...)
HikariMetricSnapshotProducer──send──▶ hikari.metrics.raw ──▶ hikariConsumerFactory ──▶ hikariKafkaListenerContainerFactory ──▶ HikariMetricSnapshotConsumer.consume(...)
```

스프링 자동 구성이 `KafkaProperties`를 읽어 default `ConsumerFactory`를 만들지만, 본 설계는 타입별 팩토리를 **명시 지정**(`@KafkaListener(containerFactory=...)`)하므로 default factory 경로를 타지 않는다. 결과적으로 한 Pythia 인스턴스가 세 토픽의 파티션을 동시에 폴링하고, 메시지마다 컨테이너 팩토리 → DTO 디시리얼라이저 → 리스너 메서드 순으로 흐른다.

---

## 4. 예외 처리 전략

| 단계 | 상황 | 처리 |
|------|------|------|
| Deserialization | Producer가 보낸 JSON이 DTO record 시그니처와 어긋남(필드 추가/타입 불일치) | `JacksonJsonDeserializer`가 `SerializationException` throw → 컨테이너의 기본 에러 핸들러가 로깅 후 offset commit (Spring Boot 4 default `DefaultErrorHandler` 동작). 본 task는 ErrorHandlingDeserializer 래핑을 추가하지 않음 — poison pill DLQ는 후속 task |
| Listener | `snapshot == null` (이론상 발생 X — Producer가 null 발행 안 함) | `@Payload(required=false)` 미지정. null이면 컨테이너가 `ListenerExecutionFailedException` 발생시킴 — 기본 핸들러가 로깅 후 다음 메시지로 |
| Listener | `snapshot.status() == FAILED` 또는 핵심 데이터 필드(`cpu`/`p99`/`active` 등)가 null인 "빈 결과" 메시지 | listener 내부에서 `log.warn("empty/failed snapshot received: topic={} key={} status={}", ...)` 후 정상 리턴. **예외를 던지지 않음** — 컨슈머가 같은 메시지를 무한 재시도하는 사고 방지 |
| Listener | 후속 hook에서의 RuntimeException | 본 task에서는 hook 없음. 추후 hook 추가 시점에 도메인별 정책(재시도/스킵) 결정 — 현재는 default 핸들러가 catch하여 offset 처리 |
| 시작 시 | broker 미기동 (localhost:9092 unreachable) | spring-kafka가 백그라운드에서 재연결 시도 → 앱 부팅은 실패하지 않음(production-ready 기본). 로깅으로 운영 식별 |
| 설정 누락 | `pythia.kafka.topic.*` 키가 없음 | `@KafkaListener(topics="${...}")`의 SpEL 해석 실패 → 부팅 실패 (fail-fast 의도) |

핵심 원칙: **컨슈머는 "받기"의 책임만 진다.** 비즈니스 판단(임계 위반/알림)은 후속 task의 hook에서 수행하므로, 본 task에서는 어떤 메시지도 listener 내부 예외로 인해 stuck 되지 않게 한다.

---

## 5. 검증 방법

### 단위 테스트 (JUnit 5 + Mockito)
기존 명명 규칙(한글 `_` 분리 + `@DisplayName`) 사용.

| 테스트 클래스 | 시나리오 |
|---------------|----------|
| `JvmMetricSnapshotConsumerTest` | (1) 정상 DTO(모든 필드 채움, status=OK)를 `consumer.consume(...)` 직접 호출 → 예외 없이 종료, debug 로깅 검증(SLF4J Logger mock 또는 `LogCaptor`) (2) 빈 결과(필드 null + status=FAILED) 전달 → 예외 없음 + warn 로깅 검증 |
| `HttpMetricSnapshotConsumerTest` | 동일 2 시나리오 |
| `HikariMetricSnapshotConsumerTest` | 동일 2 시나리오 |
| `KafkaConsumerConfigTest` (선택) | `@SpringBootTest(classes = KafkaConsumerConfig.class)` 또는 `ApplicationContextRunner`로 6개 빈(`jvm/http/hikari` × `ConsumerFactory/ContainerFactory`)이 등록되는지 검증 |

> 단위 테스트는 broker 없이 listener 메서드를 POJO처럼 직접 호출. AC 두 항목("성공", "빈 결과")이 이 단위 테스트로 충족된다.

### 통합 검증 (선택, 본 task의 AC를 보강)
- `@SpringBootTest` + `@EmbeddedKafka(topics = {"jvm.metrics.raw","http.metrics.raw","hikari.metrics.raw"})` + `KafkaTemplate`으로 메시지 발행 → `Awaitility`로 listener 호출 확인. spring-kafka-test 의존이 필요해 트레이드오프 있음 — 본 task에선 단위 테스트로 AC 충족시키고 통합은 옵션
- 수동: Argus를 같은 broker(localhost:9092)에 띄우고 Pythia 부팅 → Pythia 로그에서 세 토픽의 debug 라인이 60초 주기로 출력되는지 확인

### 회귀 검증
- 기존 `PythiaApplicationTests` 컨텍스트 로딩 통과
- `./gradlew :pythia:test` BUILD SUCCESSFUL
- Argus 측 Producer 테스트는 무관(수정 없음)

---

## 6. 트레이드오프

1. **Consumer 클래스 3개 vs 통합 단일 Consumer**: 분리는 클래스 수 증가. 통합하면 `if (topic.equals(...))` 분기 + 페이로드 타입을 `Object`로 받아야 함 → 컴파일 타임 안전성 상실. 도메인별 hook 분기에도 분리가 유리하므로 분리 채택.
2. **ContainerFactory 빈 6개(타입별 ConsumerFactory + ContainerFactory) vs 단일 default + properties 오버라이드**: spring.kafka.consumer.value-deserializer를 default로 두면 단일 타입만 강제됨. 다타입 지원하려면 `JacksonJsonDeserializer`의 type mapping(`spring.json.value.default.type`) 또는 `__TypeId__` 헤더가 필요한데, 후자는 Argus 측에서 `add.type.headers: false`로 꺼져 있어 불가. 결국 **타입별 팩토리 분리가 가장 안전한 단일 선택지**.
3. **ErrorHandlingDeserializer 미래핑**: 잘못된 페이로드 한 건이 들어오면 default 핸들러가 로깅+커밋하므로 stuck은 안 되나, "어떤 raw 바이트였는지"는 잃는다. DLQ/원문 보존이 필요해지는 시점에 ErrorHandlingDeserializer + DeadLetterPublishingRecoverer를 별 task로 도입.
4. **단일 group-id `pythia` vs 토픽별 group-id**: 단일이면 운영 단순하지만, 한 도메인의 backpressure가 다른 도메인 폴링 지연으로 번질 수 있음. 본 task 트래픽(60초 주기 × 3토픽)에선 무시 가능 → 단일 채택. 추후 트래픽 증가 시 분리.
5. **`auto-offset-reset: earliest`**: 개발 시 메시지 누락 없이 재현 가능. 운영 전환 시 `latest`로 변경하지 않으면 컨슈머 재시작 시 묵은 메시지가 한꺼번에 처리될 수 있음 — 후속 운영 task에서 환경별 프로파일로 분리.
6. **`@EmbeddedKafka` 통합 테스트 미포함**: 빌드 시간/의존성 증가가 실익보다 큼. 단위 테스트 + 수동 통합으로 AC 충족, 정식 통합 테스트는 후속.
7. **Listener 본문이 로깅뿐**: 비즈니스 가치 적어 보이나, "consume 환경 자체"가 task 산출물. 후속 hook 도입 시 메서드 시그니처를 깨지 않으므로 retrofit 비용 낮음.

---

## 핵심 파일 경로

신규/수정 (절대 경로):
- `C:\side_project\pythia\build.gradle` — spring-kafka 의존 추가
- `C:\side_project\pythia\src\main\resources\application.yml` — kafka 속성 + 토픽 키 3개
- `C:\side_project\pythia\src\main\java\com\example\pythia\kafka\config\KafkaConsumerConfig.java` — 신규
- `C:\side_project\pythia\src\main\java\com\example\pythia\kafka\consumer\JvmMetricSnapshotConsumer.java` — 신규
- `C:\side_project\pythia\src\main\java\com\example\pythia\kafka\consumer\HttpMetricSnapshotConsumer.java` — 신규
- `C:\side_project\pythia\src\main\java\com\example\pythia\kafka\consumer\HikariMetricSnapshotConsumer.java` — 신규
- `C:\side_project\pythia\src\test\java\com\example\pythia\kafka\consumer\JvmMetricSnapshotConsumerTest.java` — 신규
- `C:\side_project\pythia\src\test\java\com\example\pythia\kafka\consumer\HttpMetricSnapshotConsumerTest.java` — 신규
- `C:\side_project\pythia\src\test\java\com\example\pythia\kafka\consumer\HikariMetricSnapshotConsumerTest.java` — 신규

참조 (수정 없음):
- `C:\side_project\pythia\src\main\java\com\example\pythia\kafka\dto\jvm\JvmMetricSnapshotDto.java` 외 두 DTO — Task 011 산출물
- `C:\side_project\argus\src\main\java\com\example\argus\messaging\JvmMetricSnapshotProducer.java` — 발행 측 패턴 원본 (직렬화 헤더 정책 확인용)
- `C:\side_project\argus\src\main\resources\application.yml` — broker/토픽 명세 정합성 확인용

## 후속 작업 (본 task 범위 외)
- 도메인별 hook(임계값 평가, 알림, 상태 저장) 도입
- ErrorHandlingDeserializer + DLQ(`*.metrics.dlq`) 도입
- `@EmbeddedKafka` 통합 테스트 트랙 추가
- 환경별 프로파일(`auto-offset-reset`, `group-id`) 분리
