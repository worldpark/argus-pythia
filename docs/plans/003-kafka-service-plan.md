● 003. Kafka Producer 구성 — 구현 계획                                                                                                              
                                                                                                                                                    
  1. 선택한 방식 및 이유                                                                                                                            
                                                                                                                                                    
  ┌───────────────────┬──────────────────────────────────────────────┬──────────────────────────────────────────────────────────────────────────┐     │       항목        │                     선택                     │                                   이유                                   │   
  ├───────────────────┼──────────────────────────────────────────────┼──────────────────────────────────────────────────────────────────────────┤     │ 클라이언트        │ spring-kafka                                 │ Task 제약 사항. KafkaTemplate을 통해 Spring 생명주기·트랜잭션·테스트     │
  │ 라이브러리        │                                              │ 지원과 자연스럽게 통합                                                   │
  ├───────────────────┼──────────────────────────────────────────────┼──────────────────────────────────────────────────────────────────────────┤
  │ 직렬화 방식       │ JsonSerializer (spring-kafka 제공)           │ Task 요구: DTO를 JSON으로 직렬화. 별도 ObjectMapper 빈 주입 없이 표준    │
  │                   │                                              │ 직렬화기 사용                                                            │
  ├───────────────────┼──────────────────────────────────────────────┼──────────────────────────────────────────────────────────────────────────┤
  │ Producer 설정     │ Spring Boot auto-configuration (application.yml 기반)│
  │ 위치              │                                              │ 타입 가시성이 떨어짐                                                     │
  ├───────────────────┼──────────────────────────────────────────────┼──────────────────────────────────────────────────────────────────────────┤
  │ 토픽 자동 생성    │ 사용 안 함 (이미 003 docker-compose에서      │ 브로커 측 KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false". Producer는 토픽      │
  │                   │ kafka-init이 생성)                           │ 존재를 가정                                                              │
  ├───────────────────┼──────────────────────────────────────────────┼──────────────────────────────────────────────────────────────────────────┤
  │ 키 직렬화         │ StringSerializer                             │ key는 serviceId(string)                                                  │
  ├───────────────────┼──────────────────────────────────────────────┼──────────────────────────────────────────────────────────────────────────┤
  │ 파티션 분배       │ 기본 해시 파티셔너                           │ serviceId 기준으로 동일 서비스의 메시지가 같은 파티션에 모여 순서 보장   │
  └───────────────────┴──────────────────────────────────────────────┴──────────────────────────────────────────────────────────────────────────┘

  ▎ 참고: argus/CLAUDE.md는 빌드 도구를 Maven으로 명시하지만 실제 프로젝트는 Gradle(build.gradle)이다. Task 003은 빌드 도구를 변경하지 않으므로 기존
  ▎  Gradle 환경 그대로 진행.

  ---
  2. 구성 요소

  2.1 신규 파일

  ┌──────────────────────────┬─────────────────────────────┬──────────────────────────────────────────────────────────────────────────────────┐
  │           파일           │           패키지            │                                       역할                                       │
  ├──────────────────────────┼─────────────────────────────┼──────────────────────────────────────────────────────────────────────────────────┤
  │ MetricEvent.java         │ com.example.argus.dto       │ Kafka 메시지 페이로드 DTO (record). 필드: service, metric, value, timestamp      │
  ├──────────────────────────┼─────────────────────────────┼──────────────────────────────────────────────────────────────────────────────────┤
  │ MetricEventProducer.java │ com.example.argus.messaging │ KafkaTemplate.send(topic, key, value) 래퍼. 입력: serviceId, MetricEvent         │
  └──────────────────────────┴─────────────────────────────┴──────────────────────────────────────────────────────────────────────────────────┘

  ▎ CLAUDE.md 패키지 규칙 준수: Kafka 코드는 messaging 패키지로 분리, DTO는 dto 패키지.

  2.2 신규 의존성 (build.gradle)

  implementation 'org.springframework.kafka:spring-kafka'
  testImplementation 'org.springframework.kafka:spring-kafka-test'   // (선택) embedded broker 미사용 시 제거 가능

  이번 Task의 단위 테스트는 KafkaTemplate Mock으로 충분 → spring-kafka-test는 추가하지 않고 시작, 통합 테스트가 필요해지면 후속 Task에서 도입.

  2.3 설정 파일 변경 (application.yml)

  spring:
    kafka:
      bootstrap-servers: localhost:9092
      producer:
        key-serializer: org.apache.kafka.common.serialization.StringSerializer
        value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
        properties:
          spring.json.add.type.headers: false   # Consumer가 다른 언어/모듈일 가능성 고려
          acks: all

  argus:
    kafka:
      topic:
        metrics-raw: jvm.metrics.raw

  ---
  1. 구조 설계

  3.1 컴포넌트 다이어그램

  ┌────────────────────────────┐
  │  (호출자: 후속 Task의 서비스) │
  └────────────┬───────────────┘
               │ produce(serviceId, MetricEvent)
               ▼
  ┌────────────────────────────┐        ┌─────────────────────────┐
  │  MetricEventProducer       │ ─────▶ │  KafkaTemplate          │
  │  (messaging)               │        │  <String, MetricEvent>  │
  └────────────────────────────┘        └────────────┬────────────┘
                                                     │ JSON 직렬화
                                                     ▼
                                        ┌─────────────────────────┐
                                        │  Kafka broker           │
                                        │  topic: jvm.metrics.raw │
                                        │  key: serviceId         │
                                        └─────────────────────────┘

  3.2 네트워크 / 포트

  ┌───────────────┬────────────────────────────────────┬───────────────────────────────────────────────────────────────────────────────────┐
  │     항목      │                 값                 │                                       비고                                        │
  ├───────────────┼────────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────┤
  │ Argus → Kafka │ localhost:9092 (EXTERNAL listener) │ 003 docker-compose의 KAFKA_ADVERTISED_LISTENERS=EXTERNAL://localhost:9092 와 일치 │
  ├───────────────┼────────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────┤
  │ 토픽          │ jvm.metrics.raw                    │ 파티션 3, 복제 1 (기 생성)                                                        │
  ├───────────────┼────────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────┤
  │ 메시지 키     │ serviceId (String)                 │ 동일 서비스 메시지의 파티션·순서 보장                                             │
  ├───────────────┼────────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────┤
  │ 메시지 값     │ MetricEvent (JSON)                 │ JsonSerializer                                                                    │
  └───────────────┴────────────────────────────────────┴───────────────────────────────────────────────────────────────────────────────────┘

  3.3 MetricEvent DTO

  record MetricEvent(
      String service,
      String metric,
      double value,
      long timestamp
  )

  - 불변 record로 정의 (Java 21).
  - service는 메시지 본문에도 포함되며, 키로 쓸 serviceId는 호출자가 별도로 전달 (둘이 동일해도 명시적으로 분리하여 키 책임을 호출 시점에 노출).

  3.4 MetricEventProducer 인터페이스

  ┌────────┬──────────────────────────────────────────────────────────────────────────────┬─────────────────────────────────────────────────────┐
  │ 메서드 │                                   시그니처                                   │                        동작                         │
  ├────────┼──────────────────────────────────────────────────────────────────────────────┼─────────────────────────────────────────────────────┤
  │ send   │ CompletableFuture<SendResult<String, MetricEvent>> send(String serviceId,    │ kafkaTemplate.send(topic, serviceId, event) 호출,   │
  │        │ MetricEvent event)                                                           │ future 반환                                         │
  └────────┴──────────────────────────────────────────────────────────────────────────────┴─────────────────────────────────────────────────────┘

  - 토픽명은 @Value("${argus.kafka.topic.metrics-raw}")로 주입.
  - 예외 처리는 호출자 future 콜백에 위임 (이번 Task에서는 별도 CustomException 변환 없음 — 후속 Task에서 도입).
  - send 결과에 대해 성공/실패 콜백 처리
  - 실패 시 로그 기록

  ---
  1. 실행 흐름

  2. 호출자가 MetricEventProducer.send("svc-A", new MetricEvent("svc-A","cpu",0.42,168...)) 호출
  3. Producer가 KafkaTemplate.send("jvm.metrics.raw", "svc-A", event) 위임
  4. KafkaTemplate
       ├─ key:   StringSerializer    → "svc-A" 바이트
       └─ value: JsonSerializer      → {"service":"svc-A","metric":"cpu","value":0.42,"timestamp":...}
  5. Kafka 클라이언트가 브로커(localhost:9092)로 전송
  6. acks=all 응답 확인 후 SendResult future 완료

  ---
  5. 검증 방법

  5.1 단위 테스트 (Task 요구)

  테스트: MetricEventProducerTest#send_호출시_올바른_topic_key_value로_KafkaTemplate_send를_호출한다
  대상: MetricEventProducer
  방식: KafkaTemplate Mock. ArgumentCaptor로 topic/key/value 검증. topic="jvm.metrics.raw", key=serviceId, value=전달된 MetricEvent
  ────────────────────────────────────────
  테스트: MetricEventTest#JSON_직렬화시_4개_필드가_모두_포함된다
  대상: MetricEvent + JsonSerializer
  방식: new JsonSerializer<MetricEvent>().serialize(topic, event) 결과를 ObjectMapper로 파싱하여 service/metric/value/timestamp 4개 키 존재 확인

  ▎ CLAUDE.md "테스트 규칙": 서비스 레이어 단위 테스트 필수 — Mockito 사용.

  5.2 수동 검증 (로컬)

  1. (사전) docker-compose -f docker/kafka/docker-compose.yml up -d
  2. ./gradlew bootRun (Argus 기동)
  3. 임시 진입점(예: 테스트 코드 또는 후속 Task의 Controller)으로 send 호출
  4. kafka-console-consumer 또는 kafka-ui(profile: ui)로
     topic=jvm.metrics.raw 메시지 수신 확인
     - key가 serviceId인지
     - value가 JSON이고 4개 필드가 있는지

  5.3 성공 기준

  - 단위 테스트 2종 모두 통과
  - ./gradlew build 정상
  - (수동 시) kafka-ui에서 메시지가 jvm.metrics.raw에 적재됨


  ---
  1. 후속 작업에 전달할 정보

  ┌───────────────────────┬─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
  │         항목          │                                                        내용                                                         │
  ├───────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
  │ 외부 API              │ MetricEventProducer.send(String serviceId, MetricEvent event) 만 노출                                               │
  ├───────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
  │ 토픽명 상수           │ application.yml의 argus.kafka.topic.metrics-raw 키로 주입. 후속 Service는 Producer만 의존하면 되고 토픽명 직접 참조 │
  │                       │  불필요                                                                                                             │
  ├───────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
  │ MetricEvent           │ long (epoch millis 가정). 후속 PrometheusQueryService → Producer 연결 시 변환 책임은 호출자 측                      │
  │ 타임스탬프            │                                                                                                                     │
  ├───────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
  │ 파티션 보장           │ serviceId 동일 시 동일 파티션 → 후속 Consumer(jvm.alert 처리 등)에서 서비스별 순서 가정 가능                        │
  ├───────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
  │ 직렬화 호환           │ spring.json.add.type.headers=false 설정으로 Consumer는 type 헤더 없이 역직렬화 가능. 다른 언어/서비스 Consumer 도입 │
  │                       │  가능                                                                                                               │
  └───────────────────────┴─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘

  ---
  7. 제외 범위 (이번 Task에서 하지 않음)

  - Consumer 구현 (별도 Task)
  - 재시도/DLQ/에러 토픽 정책
  - Kafka 트랜잭션, idempotent producer 세부 튜닝
  - Prometheus 조회 결과 → MetricEvent 변환 로직 (Task 001 결과를 사용하는 별도 후속 Task)
  - 통합 테스트 (@EmbeddedKafka, Testcontainers)
  - CustomException으로의 Kafka 예외 변환 (메시징 예외 정책이 정해진 후속 Task에서 도입)
  - Controller / REST API 노출 (이번 Task는 messaging 계층까지만)
  - 운영 환경 보안 설정 (SASL/SSL) — 로컬 PLAINTEXT 기준

  ---
  변경 요약 (3-파일 + 설정)

  ┌──────┬──────────────────────────────────────────────────────────────────────────────┐
  │ 종류 │                                     경로                                     │
  ├──────┼──────────────────────────────────────────────────────────────────────────────┤
  │ 신규 │ argus/src/main/java/com/example/argus/dto/MetricEvent.java                   │
  ├──────┼──────────────────────────────────────────────────────────────────────────────┤
  │ 신규 │ argus/src/main/java/com/example/argus/messaging/MetricEventProducer.java     │
  ├──────┼──────────────────────────────────────────────────────────────────────────────┤
  │ 신규 │ argus/src/test/java/com/example/argus/messaging/MetricEventProducerTest.java │
  ├──────┼──────────────────────────────────────────────────────────────────────────────┤
  │ 신규 │ argus/src/test/java/com/example/argus/dto/MetricEventTest.java               │
  ├──────┼──────────────────────────────────────────────────────────────────────────────┤
  │ 수정 │ argus/build.gradle (spring-kafka 의존성 추가)                                │
  ├──────┼──────────────────────────────────────────────────────────────────────────────┤
  │ 수정 │ argus/src/main/resources/application.yml (kafka 설정 + 토픽명 추가)          │
  └──────┴──────────────────────────────────────────────────────────────────────────────┘