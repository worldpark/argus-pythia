# Task 009 — JVM Metric Snapshot Scheduler 구현 계획

## Context

현재 `JvmMetricSnapshotPublisher.publish()`는 호출 시점에만 동작하며, 정기적으로 실행되는 진입점이 없다. Argus의 핵심 흐름(Prometheus 쿼리 → DTO 조립 → Kafka 발행)이 완결되어 있으나, 이를 60초 주기로 자동 트리거하는 스케줄러가 부재해 외부 컨슈머(`jvm.metrics.raw` 토픽)는 메시지를 받지 못한다.

본 Task는 Spring `@Scheduled`를 적용한 단일 진입 메서드를 신설해 `publish()`를 60초 주기로 호출하고, Kafka 발행 결과를 로그로 관측 가능하게 한다. 프로젝트 어디에도 아직 `@EnableScheduling`/`@Scheduled`가 없으므로 부수적으로 main application class에 활성화 어노테이션도 추가한다.

기능 동작은 정확히 한 군데(신규 scheduler 클래스)에서만 일어나며, 기존 Publisher/Assembler/Producer/MetricPointMapper는 손대지 않는다.

## 설계 방식 및 이유

| 결정 | 선택 | 이유 |
| --- | --- | --- |
| 스케줄링 활성화 위치 | `ArgusApplication`에 `@EnableScheduling` 추가 | 추가 `@Configuration` 클래스 신설 회피. 어노테이션 1개라 main 비대화 무시 가능 |
| 스케줄러 클래스 위치 | `service/metric/snapshot/JvmMetricSnapshotScheduler.java` | Publisher와 동일 패키지에 두어 협력 관계 명확화. 현재 schedule 사용처가 1곳뿐(Task 제약)이라 별도 `scheduler/` 패키지 신설은 과설계 |
| 트리거 방식 | `@Scheduled(fixedDelay = 60_000L, initialDelay = 0L)` | 이전 실행 완료 후 60초 → 실행 시간이 길어져도 overlap/큐잉 없음. `initialDelay=0`으로 앱 시작 직후 1회 즉시 실행 |
| 주기 값 관리 | 어노테이션 상수 하드코딩 | Task 범위 "60초 고정". 프로퍼티 외부화는 환경별 변동 요구가 생긴 후 도입 (YAGNI) |
| Future 처리 | `publisher.publish().whenComplete((r, ex) -> log...)` | Kafka send 실패가 silent하게 묻히는 것 방지. DEBUG(성공) / ERROR(실패) 분기 |
| 동기 예외 처리 | 메서드 진입부 try/catch (방어적) | `assemble()`은 모든 실패를 status 흡수해 throw 안 하지만, 스케줄러 보호를 위해 unexpected RuntimeException 차단 — 다음 iteration 정상화 보장 |
| 메서드 시그니처 | `void triggerSnapshot()` (return 없음) | `@Scheduled`는 void 반환 강제. 비동기 결과는 whenComplete로 처리 |

## 구성 요소

### 신규 파일

- `argus/src/main/java/com/example/argus/service/metric/snapshot/JvmMetricSnapshotScheduler.java`
  - `@Component` (또는 `@Service` — 동일 패키지의 Publisher가 `@Service`이나 본 클래스는 비즈니스 로직 없이 트리거 역할만 → `@Component` 권장)
  - `@RequiredArgsConstructor`로 `JvmMetricSnapshotPublisher` 주입
  - `@Scheduled(fixedDelay = 60_000L, initialDelay = 0L)`이 붙은 `public void triggerSnapshot()`
  - 내부: try/catch로 `publisher.publish()` 호출 → `whenComplete`에서 성공/실패 로깅
  - SLF4J Logger 사용
- `argus/src/test/java/com/example/argus/service/metric/snapshot/JvmMetricSnapshotSchedulerTest.java`
  - `@ExtendWith(MockitoExtension.class)`, `@Mock JvmMetricSnapshotPublisher`, `@InjectMocks JvmMetricSnapshotScheduler`
  - 케이스:
    1. `triggerSnapshot_invokesPublisherPublishOnce` — 정상 호출 검증, `verify(publisher, times(1)).publish()`
    2. `triggerSnapshot_publisherThrows_doesNotPropagate` — `when(publisher.publish()).thenThrow(new RuntimeException("boom"))` 시 `triggerSnapshot()`이 예외 escape 없이 종료, 로거 호출 검증(또는 단순 no-throw 검증)
    3. `triggerSnapshot_kafkaFutureFails_logsButDoesNotThrow` — Publisher가 `CompletableFuture.failedFuture(...)` 반환 시 `whenComplete` 분기 정상 동작, 메서드는 throw 없이 종료

### 수정 파일

- `argus/src/main/java/com/example/argus/ArgusApplication.java`
  - 클래스 어노테이션에 `@EnableScheduling` 추가
  - 그 외 변경 없음 (`Clock` Bean 정의 유지)

### 변경 없음

- `JvmMetricSnapshotPublisher` (시그니처/내부 동일)
- `JvmMetricSnapshotAssembler`
- `JvmMetricSnapshotProducer`
- `MetricPointMapper`, `PrometheusQueryService`, `PrometheusClient`
- `application.yml` (스케줄링 프로퍼티 미도입)
- Kafka topic 설정 (`argus.kafka.topic.metrics-raw: jvm.metrics.raw` 그대로)

## 데이터 흐름

1. Spring Boot 시작
   - `ArgusApplication`의 `@EnableScheduling`이 default `TaskScheduler` 활성화
   - `JvmMetricSnapshotScheduler` Bean 등록
2. 앱 시작 직후 1회 즉시 실행 (`initialDelay = 0L`), 이후 매 호출 완료 시점부터 60초 후 다음 실행
   - `TaskScheduler`가 `triggerSnapshot()` 호출
3. `triggerSnapshot()` 내부
   1. try 블록 진입 → `publisher.publish()` 호출
   2. `publisher.publish()` 동기 부분: `assembler.assemble()` → Prometheus 4종 쿼리(CPU/Heap/GC×2/Thread) → `JvmMetricSnapshotDto` 생성 → `producer.send(serviceId, snapshot)` 반환
   3. `producer.send`는 Kafka producer로 보내고 `CompletableFuture<SendResult>` 반환
   4. 반환된 CompletableFuture에 `whenComplete((result, ex) -> ...)` chain
4. Future 완료 시점
   - 성공: `log.debug("snapshot published: offset={}, partition={}", ...)` 형태로 메타데이터 로깅
   - 실패: `log.error("snapshot publish failed", ex)` 로깅
5. 동기 단계에서 unexpected RuntimeException 발생 시
   - `triggerSnapshot()`의 catch에서 `log.error("snapshot trigger failed", e)` 후 swallow
   - 다음 60초 후 정상적으로 재진입
6. fixedDelay 정책: 다음 호출은 직전 호출 **완료 시각 + 60초**. 실행이 길어지면 실제 주기는 60초 + 실행시간 (overlap/큐잉 없음)

## 예외 처리 전략

| 발생 위치 | 처리 |
| --- | --- |
| `publisher.publish()` 동기 호출 중 RuntimeException | `triggerSnapshot()`의 try/catch가 잡아 `log.error` 후 swallow. 다음 주기 영향 없음 |
| `assembler.assemble()` 내부 메트릭 실패 | 이미 모든 실패를 `MetricStatus`로 흡수해 throw 안 함. 본 Task 변경 없음 |
| `producer.send()` 동기 단계 예외 (예: serializer 실패) | 위 try/catch가 흡수 |
| Kafka 비동기 send 실패 (broker down, timeout 등) | `whenComplete((r, ex) -> log.error)` 에서 로깅. 재시도/DLQ는 본 Task 범위 외 |
| `@Scheduled` 메서드에서 던져진 unhandled Throwable | Spring의 기본 동작은 로깅 후 다음 주기 계속이지만, 본 클래스는 명시적 catch로 로깅 형식을 통일하고 ERROR 레벨 보장 |

기존 `PrometheusQueryException`은 Assembler 내부 `resolve` 헬퍼가 이미 흡수하므로 본 Task에서 별도 처리 불필요.

## 검증 방법

### 단위 테스트

```bash
cd argus
./gradlew -q -Dtest=JvmMetricSnapshotSchedulerTest test
./gradlew -q test
```

검증 포인트:
- `triggerSnapshot()` 1회 호출 시 `publisher.publish()` 정확히 1회 호출
- Publisher가 동기 throw 시 `triggerSnapshot()`은 throw하지 않음 (스케줄러 보호 검증)
- Publisher가 failed future 반환 시 `whenComplete` 분기 진입 + `triggerSnapshot()` 정상 종료

### 컴파일/정적 검사

```bash
cd argus && ./gradlew -q compileJava compileTestJava
```

- `@EnableScheduling`/`@Scheduled` import 확인 (`org.springframework.scheduling.annotation.*`)
- `@Component` Bean 등록 확인 (Spring context 시작 시 NoSuchBeanDefinitionException 없는지)

### 통합 검증 (수동)

```bash
docker compose -f docker/docker-compose.kafka.yml up -d
./gradlew bootRun
docker exec -it kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic jvm.metrics.raw --from-beginning
```

체크 항목:
- 애플리케이션 시작 직후 첫 메시지 즉시 도착, 이후 직전 발행 완료 시점 + 60초 간격으로 추가 메시지 발행
- 메시지 페이로드 형식이 `JvmMetricSnapshotDto` JSON과 일치 (application/instance/cpu/memory/gc/thread/status)
- 로그에 `snapshot published` (DEBUG) 또는 `snapshot publish failed` (ERROR) 메시지 노출
- Prometheus 미기동 상태로 시작해도 스케줄러는 죽지 않고 다음 주기에 재시도 (각 메트릭 status가 QUERY_FAILED인 snapshot이 발행됨)

## 트레이드오프

| Trade-off | 선택 | 포기 / 위험 |
| --- | --- | --- |
| `fixedRate` vs `fixedDelay` | fixedDelay | "이전 시작 시점 기준 60초"의 정확성을 포기하는 대신, 실행이 길어져도 호출 큐잉/overlap 없음. Prometheus 응답 지연에 견고 |
| 어노테이션 하드코딩 vs 프로퍼티 외부화 | 하드코딩 | 환경별 주기 변경 시 코드 수정 필요. Task 범위 "60초 고정"이라 YAGNI |
| Scheduler 동일 패키지 vs 별도 `scheduler/` 패키지 | 동일 패키지(`service/metric/snapshot`) | 향후 schedule 사용처 추가 시 패키지 정리 필요. Task 제약상 1곳뿐이라 현재는 OK |
| `whenComplete` 로깅 vs fire-and-forget | whenComplete 로깅 | 코드 4~6 라인 추가. 대신 Kafka 비동기 실패 가시성 확보 |
| `@EnableScheduling` 위치: main vs `@Configuration` 분리 | main(`ArgusApplication`) | main에 어노테이션 1개 추가. `@SchedulingConfiguration` 신설은 과설계 |
| 스케줄러 보호 try/catch vs Spring 기본 동작 의존 | 명시적 try/catch | 미세하게 코드량 증가. 대신 ERROR 레벨/메시지 형식 통일 |
| 첫 실행 시점 (앱 시작 직후 vs 60초 후) | 앱 시작 직후 (`initialDelay = 0L`) | 시작 직후 1회 발행으로 헬스체크/첫 메시지 가시성 확보. 컨텍스트 초기화 직후라 외부 의존(Prometheus/Kafka) 미준비 가능성 → 첫 회 QUERY_FAILED 메시지가 발행될 수 있음 |
| 단위 테스트만 vs `@SpringBootTest`로 timing 검증 | 단위 테스트 | 60초 timing 자체는 Spring 책임이라 검증 불필요. 통합 검증은 수동 단계로 충분 |

## 작업 순서 (구현 시)

1. `ArgusApplication`에 `@EnableScheduling` 추가
2. `JvmMetricSnapshotScheduler` 클래스 작성 (`@Component` + `@Scheduled(fixedDelay=60_000L, initialDelay=0L)` + try/catch + whenComplete 로깅)
3. `JvmMetricSnapshotSchedulerTest` 3 케이스 작성
4. `./gradlew test` 전체 통과 확인
5. 로컬 Kafka 기동 → `./gradlew bootRun` → console-consumer로 60초 주기 메시지 도착 수동 검증
