# Task 007 — PromQL 결과 → Kafka 메시지 Producer 계획

## Context
Task 005에서 PromQL 정의, Task 006에서 `PrometheusResponse → JvmMetricSnapshotDto` 변환이 끝나
`JvmMetricSnapshotAssembler.assemble()`이 완성된 상태다. 이번 Task 007은 그 결과 DTO를 Kafka
토픽 `jvm.metrics.raw` 로 발행하는 production 경로를 잇는 단계로, downstream Pythia 서비스에서
스냅샷을 소비할 수 있게 만드는 것이 목표다. 이후 Task에서 스케줄러/트리거가 이 publisher만 호출하면
바로 작동하도록 구조만 정리한다.

현재 `MetricEventProducer.send`는 단일 스칼라 record(`MetricEvent`)를 받도록 되어 있어, 본 Task 입력
정의(`메시지 data = JvmMetricSnapshotDto`)와 맞지 않는다. 따라서 producer 시그니처와 제네릭 타입을
바꾸고, assemble → send를 잇는 얇은 publisher 한 개를 추가한다.

## 설계 방식 및 이유

| 항목 | 선택 | 이유 |
| --- | --- | --- |
| Producer 값 타입 | `KafkaTemplate<String, MetricEvent>` → `KafkaTemplate<String, JvmMetricSnapshotDto>` 로 교체 | Task가 "data는 JvmMetricSnapshotDto" 라고 명시. wrapper envelope 도입은 과설계 (consumer도 한 개) |
| 오케스트레이션 위치 | `service/metric/snapshot/` 하위 새 클래스 `JvmMetricSnapshotPublisher` | CLAUDE.md "Kafka Producer/Consumer는 별도 패키지 분리" 규칙 유지. messaging 계층은 발행만, 비즈 흐름은 service에 둠 |
| Partition key | `JvmMetricSnapshotDto.application` (사용자 확인 완료) | 동일 앱의 모든 instance 메시지가 동일 파티션 → 앱 단위 순서 보장 |
| FAILED snapshot 처리 | 그대로 발행 (사용자 확인 완료) | consumer가 SnapshotStatus를 보고 판단. application이 null인 경우 fallback key `unknown` 사용 |
| `MetricEvent` 처리 | 삭제 (관련 테스트 `MetricEventTest`도 삭제) | 변경 후 사용처 0. dead code 유지 회피. 단 Task 003 plan 문서는 그대로 둠 |

## 구성 요소

### 신규 파일
- `argus/src/main/java/com/example/argus/service/metric/snapshot/JvmMetricSnapshotPublisher.java`
  - `@Service`, 의존성: `JvmMetricSnapshotAssembler`, `MetricEventProducer`
  - `public CompletableFuture<SendResult<String, JvmMetricSnapshotDto>> publish()`
    1. `assembler.assemble()` 호출 → snapshot 획득
    2. `serviceId = snapshot.application() != null ? snapshot.application() : "unknown"`
    3. `producer.send(serviceId, snapshot)` 반환
- `argus/src/test/java/com/example/argus/service/metric/snapshot/JvmMetricSnapshotPublisherTest.java`
  - Mockito 단위 테스트
  - assembler 결과를 stub하여 producer.send에 전달되는 key/value 검증
  - application=null 케이스에서 `"unknown"` key 검증
  - assembler 예외 → 그대로 전파 검증

### 수정 파일
- `argus/src/main/java/com/example/argus/messaging/MetricEventProducer.java`
  - import: `MetricEvent` 제거 → `JvmMetricSnapshotDto` 추가
  - 필드: `KafkaTemplate<String, JvmMetricSnapshotDto>` 로 제네릭 변경
  - `send(String serviceId, JvmMetricSnapshotDto snapshot)` 시그니처/리턴 타입 변경
  - 로그 문구는 `MetricEvent` → `JvmMetricSnapshot` 으로만 수정 (구조 동일)
  - 클래스명/파일명은 유지 — 본 Task는 producer "리네이밍" 범위가 아님 (수정 범위 제한 규칙)
- `argus/src/test/java/com/example/argus/messaging/MetricEventProducerTest.java`
  - mock `KafkaTemplate` 제네릭 타입 변경
  - 테스트 데이터: `MetricEvent` 객체 → `JvmMetricSnapshotDto` 빈/샘플 객체로 교체
  - `ArgumentCaptor<JvmMetricSnapshotDto>` 로 변경
  - 검증 항목 (topic / key / value 동일성)은 유지

### 삭제 파일
- `argus/src/main/java/com/example/argus/dto/MetricEvent.java`
- `argus/src/test/java/com/example/argus/dto/MetricEventTest.java`

### 변경 없음
- `application.yml` (`argus.kafka.topic.metrics-raw=jvm.metrics.raw` 그대로)
- `JvmMetricSnapshotDto`, `JvmMetricSnapshotAssembler`, `PrometheusResponse`, `PromQL` 정의 일체
- Kafka serializer 설정 (`JacksonJsonSerializer` 그대로 사용 — DTO record 직렬화 호환)

## 데이터 흐름

1. (이후 Task의 트리거) → `JvmMetricSnapshotPublisher.publish()`
2. `JvmMetricSnapshotPublisher` → `JvmMetricSnapshotAssembler.assemble()`
3. `JvmMetricSnapshotAssembler` → `PrometheusQueryService.queryByMetric(...)` 다회 호출
4. `PrometheusQueryService` → `PrometheusClient.query(promql)` → `PrometheusResponse`
5. `PrometheusQueryService` 결과를 `MetricPointMapper.toPoint(...)` 가 `MetricPointDto` 로 변환
6. `JvmMetricSnapshotAssembler` 가 sub-DTO(Cpu/Memory/Gc/Thread) 조립 → `JvmMetricSnapshotDto`
7. `JvmMetricSnapshotPublisher` 가 `serviceId = snapshot.application() ?? "unknown"` 결정
8. `MetricEventProducer.send(serviceId, snapshot)` → `KafkaTemplate.send("jvm.metrics.raw", serviceId, snapshot)`
9. `JacksonJsonSerializer` 가 DTO 를 JSON 바이트로 직렬화 → 브로커 전송
10. `whenComplete` 콜백에서 성공 시 offset debug 로그, 실패 시 error 로그

## 예외 처리 전략

| 발생 위치 | 처리 |
| --- | --- |
| Assembler 내부 (Prometheus 호출 실패 등) | 이미 Task 006에서 sub-DTO status로 흡수 → 정상 snapshot 반환됨. publisher가 별도 처리 불필요 |
| Assembler가 catch 못한 예외 | publisher에서 throw 그대로 전파 (호출자에서 결정) |
| `KafkaTemplate.send` 동기 단계 예외 (직렬화 실패 등) | RuntimeException 으로 그대로 throw — `MetricEventProducer` 가 wrapping하지 않음 (현 동작 유지) |
| `KafkaTemplate.send` 비동기 실패 | `whenComplete`에서 error 로그만, 호출자에게는 failed `CompletableFuture` 반환 (현 동작 유지) |
| `application` null | `"unknown"` 으로 대체 — null key 직렬화/파티셔닝 회피 |

CLAUDE.md "외부 API 오류는 별도 Exception" 규칙은 이미 `PrometheusQueryException` 으로 처리됨. Kafka 실패는
이번 Task에서는 별도 CustomException 으로 변환하지 않음 — 이후 retry/circuit-breaker 도입 시 다룰 영역.

## 검증 방법

### 단위 테스트
```bash
cd argus
./mvnw -q -Dtest=MetricEventProducerTest test
./mvnw -q -Dtest=JvmMetricSnapshotPublisherTest test
```
- `MetricEventProducerTest`: topic="jvm.metrics.raw", key=serviceId, value=snapshot 검증
- `JvmMetricSnapshotPublisherTest`:
  - normal: assembler.assemble() stub → producer.send 가 (snapshot.application(), snapshot) 으로 호출되는지
  - null application: `"unknown"` key 사용
  - assembler throws → publisher 도 throw

### 통합 검증 (수동)
```bash
docker compose -f docker/docker-compose.kafka.yml up -d
./mvnw spring-boot:run
# Kafka 콘솔 컨슈머로 토픽 구독
docker exec -it kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic jvm.metrics.raw --from-beginning
```
- 이후 Task의 트리거가 없으므로, 임시로 `JvmMetricSnapshotPublisher.publish()` 를 테스트 코드 또는
  `@PostConstruct` 보조 트리거 한 번으로 호출해 메시지 발행 여부 확인 (본 코드에는 커밋하지 않음)
- JSON payload에 application/instance/cpu/memory/gc/thread/status 필드가 포함되는지 확인

### Build / 컴파일
```bash
cd argus && ./mvnw -q compile test-compile
```
`MetricEvent` 삭제로 인한 미참조가 남지 않는지 확인.

## 트레이드오프

| Trade-off | 선택한 쪽 | 포기한 쪽 |
| --- | --- | --- |
| Producer 값 타입을 `JvmMetricSnapshotDto`로 강결합 vs 제네릭/`Object` | 강결합 | 추후 다른 DTO 타입을 같은 producer로 보내려면 리팩터링 필요. 현재 토픽-DTO 1:1 이므로 단순함 우선 |
| Publisher를 신규 service 클래스로 분리 vs Assembler에 publish 메서드 추가 | 분리 | Assembler 책임이 "조립"으로 명확. publish는 별도 관심사. 클래스 1개 늘어남 |
| `MetricEvent` 삭제 vs 보존 | 삭제 | "수정 범위 명확히 제한" 규칙과 살짝 충돌하나, dead code 유지 시 코드 의도 혼란. 삭제는 같은 PR 내에서 안전히 가능 |
| FAILED snapshot 발행 vs 스킵 | 발행 | 네트워크/메시지 트래픽 약간 증가. 대신 consumer 가 메트릭 수집 실패 자체를 관측 가능 (관측성 우선) |
| application null → `"unknown"` fallback vs 발행 스킵 | fallback | 데이터 일관성/관측성. 단 `unknown` 키가 한 파티션에 몰릴 위험은 있음 (실제로는 정상 환경에서 거의 발생 안 함) |
| Producer 클래스명 그대로 두기 vs `MetricSnapshotProducer`로 리네이밍 | 그대로 | 본 Task가 "send 함수 수정 가능"만 허용 — 클래스 리네이밍은 범위 밖. 후속 Task에서 정리 가능 |

## 작업 순서 (구현 시)
1. `MetricEventProducer` 제네릭/시그니처 변경
2. `MetricEventProducerTest` 갱신
3. `JvmMetricSnapshotPublisher` 추가
4. `JvmMetricSnapshotPublisherTest` 추가
5. `MetricEvent`, `MetricEventTest` 삭제
6. `mvnw test` 통과 확인 → 수동 통합 검증
