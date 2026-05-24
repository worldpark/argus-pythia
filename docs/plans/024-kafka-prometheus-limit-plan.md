# 024. 가상 스레드 도입 전 Argus Prometheus/Kafka 부하 제한 안전장치 - Plan

> Source Task: docs/tasks/024-kafka-prometheus-limit.md
> Target: argus
> 본 Plan 은 java.util.concurrent.Semaphore 기반 ConcurrencyLimiter 패턴으로, Prometheus 조회 / Kafka publish 경로에 동시 실행 상한을 적용한다.
> 가상 스레드 활성화, WebClient -> RestClient 교체, Reactor 기반 Prometheus 병렬화(Mono.zip), durable 버퍼 추가는 본 Task 범위 외.

---

## 0. 구현 목표 (한 줄 요약)
Argus 의 Prometheus 조회 경로와 Kafka publish 경로에 외부화된 동시 실행 상한(Semaphore + 짧은 재시도)을 추가하여, 향후 가상 스레드 병렬화가 들어와도 외부 시스템(Prometheus 서버, Kafka broker)에 과도한 동시 진입이 발생하지 않도록 한다.

---

## 1. 설계 방식 및 이유

### 1.1 동시성 제어 메커니즘: java.util.concurrent.Semaphore 채택
| 후보 | 채택 여부 | 사유 |
|---|---|---|
| java.util.concurrent.Semaphore | 채택 | JDK 표준, 추가 의존성 0, tryAcquire(timeout) 으로 짧은 대기 + 명시적 실패 직접 표현 가능. 가상 스레드 친화적(park 기반). |
| Resilience4j Bulkhead (SemaphoreBulkhead) | 미채택 | 내부적으로 동일한 Semaphore 기반인데 BulkheadFullException 매핑 / Registry / Metrics 도입이 필요. argus 는 현재 resilience4j 의존성 미사용(argus/build.gradle 확인) 이며 본 Task 가 단일 안전장치 추가만 요구. pythia 는 retry+circuitbreaker 용도로 별도 사용 중이지만 argus 도입 명분 부족. |
| Resilience4j ThreadPoolBulkhead | 미채택 | 별도 워커 풀로 작업을 위임하는 모델 -> 본 Task 가 명시적으로 제외한 "스레드 모델 변경" 에 해당. 가상 스레드 도입 전 단계에서 추가 풀은 부담. |
| Executors.newFixedThreadPool + 큐 | 미채택 | 동시 실행 상한이 큐 길이와 결합되어 reject 정책이 복잡해진다. 짧은 대기 + 명시적 실패 라는 Task 요구에는 Semaphore 가 더 직관적. |
| Reactor flatMap(maxConcurrency) | 미채택 | Task 가 "Reactor 전면 재작성 금지" 를 명시. 현재 .block() 동기 호출 구조 유지 필요. |

### 1.2 위치: Client/Messaging 계층 (가장 바깥 진입점)
- PrometheusClient.query(...) 의 본문 진입 시점에 prometheusLimiter.execute(...) 로 감싼다.
- 각 *MetricSnapshotProducer.send(...) 의 kafkaTemplate.send(...) 호출 직전 시점에 kafkaPublishLimiter.acquire() 로 진입 제한, 반환 future 의 whenComplete 에서 release.
- 이유:
  1. 외부 시스템에 도달하는 마지막 단일 지점에 위치시켜야 우회 호출 경로 누락이 없다(Scheduler 외 다른 진입점이 늘어나도 자동 보호).
  2. 향후 Scheduler/Assembler 가 가상 스레드 기반 병렬화(StructuredTaskScope, parallelStream 등)로 바뀌어도 Limiter 위치를 옮길 필요 없이 그대로 재사용 가능.
  3. Service 계층(PrometheusQueryService, *Publisher) 은 도메인 로직(상태 검증, 라벨 누적, 버퍼 fallback) 책임에 집중. 동시성 제어와 도메인 로직 분리.

### 1.3 Kafka publish 제한의 의미 정의 (중요)
- KafkaTemplate.send 는 Producer 의 RecordAccumulator 에 적재만 하고 곧바로 CompletableFuture 를 반환한다 -> 호출 자체는 거의 즉시 끝난다.
- 따라서 "현재 publish 중인 카운트" 를 의미 있게 제한하려면 permit release 시점을 send 반환 직후가 아니라 반환된 CompletableFuture 가 완료될 때(whenComplete) 로 잡아야 한다.
- 이 방식이면 "broker 응답을 기다리고 있는 in-flight publish 의 동시성 상한" 을 표현하게 되어, broker 가 느려질 때 Argus 가 무한히 메시지를 쌓아 OOM/큐 폭주를 일으키는 것을 막는다.
- Prometheus 쪽은 .block() 동기 호출이라 메서드 본문 try-finally 로 자연스럽게 release.

### 1.4 짧은 대기 후 재시도 + 명시적 실패
- tryAcquire(acquireTimeout) 으로 짧게 대기.
- 실패 시 설정값 maxAttempts 만큼 재시도(매 시도마다 acquireTimeout 만큼 대기).
- 모두 실패하면 명시적 RuntimeException 으로 실패(아래 4 절).
- 이번 단계에서는 durable 버퍼 / 재시도 큐 없이 호출자에게 예외를 전달한다(Prometheus 측은 PrometheusQueryException 으로 변환되어 기존 Assembler 의 queryFailed 폴백 분기가 그대로 흡수, Kafka 측은 기존 whenComplete 의 buffer fallback 분기가 그대로 흡수).

### 1.5 향후 가상 스레드 도입 시 재사용성
- Limiter 자체는 Semaphore wrapper 라 캐리어 스레드/가상 스레드 모두에서 동작(Semaphore.tryAcquire 는 가상 스레드 친화: park 기반).
- Scheduler/Assembler 측이 StructuredTaskScope 등으로 fan-out 되어도 Client/Messaging 진입점에 Limiter 가 있으므로 그대로 동작.
- ConfigurationProperties 로 외부화되어 부하 환경에 따라 permit 수만 조정하면 됨.

---

## 2. 구성 요소

### 2.1 신규 파일
| 경로 | 책임 |
|---|---|
| argus/src/main/java/com/example/argus/config/ConcurrencyLimitProperties.java | argus.concurrency.* 설정 바인딩. Prometheus / Kafka publish 두 섹션 각각 permits, acquireTimeout, maxAttempts 보유. |
| argus/src/main/java/com/example/argus/common/concurrency/ConcurrencyLimiter.java | Semaphore 1개를 감싸는 재사용 컴포넌트. execute(Callable), acquire(), release(), availablePermits() 노출. |
| argus/src/main/java/com/example/argus/common/concurrency/ConcurrencyLimiterConfig.java | ConcurrencyLimitProperties 를 읽어 prometheusConcurrencyLimiter, kafkaPublishConcurrencyLimiter 두 빈 등록. |
| argus/src/main/java/com/example/argus/exception/ConcurrencyLimitExceededException.java | permit 획득 재시도 소진 시 발생. limiter 이름 / 시도 횟수 / 누적 대기 시간 포함. RuntimeException 상속(기존 PrometheusQueryException 의 주석 정책과 동일: 공통 CustomException 미존재). |

### 2.2 수정 파일
| 경로 | 변경 내용 |
|---|---|
| argus/src/main/java/com/example/argus/client/PrometheusClient.java | 생성자에 @Qualifier("prometheusConcurrencyLimiter") ConcurrencyLimiter 주입. query(...) 본문을 limiter.execute(() -> existingQueryLogic()) 로 감싸기. ConcurrencyLimitExceededException 은 PrometheusQueryException 으로 변환. |
| argus/src/main/java/com/example/argus/messaging/JvmMetricSnapshotProducer.java | 생성자에 @Qualifier("kafkaPublishConcurrencyLimiter") ConcurrencyLimiter 주입. send 진입 시 limiter.acquire(), kafkaTemplate.send(...) 호출. 반환된 future 의 whenComplete 에 limiter.release() 등록(예외/정상 모두 1회 release 보장). acquire 실패 시 예외 완료된 CompletableFuture 를 반환하여 상위 Publisher 의 buffer fallback 분기를 그대로 동작시킨다(시그니처 호환 + 기존 fallback 경로 재사용). |
| argus/src/main/java/com/example/argus/messaging/HttpMetricSnapshotProducer.java | 위와 동일 패턴. |
| argus/src/main/java/com/example/argus/messaging/HikariMetricSnapshotProducer.java | 위와 동일 패턴. |
| argus/src/main/resources/application.yml | argus.concurrency.* 키 추가(2.4 참조). |

### 2.3 ConfigurationProperties 명세
- prefix: argus.concurrency
- 필드:
  - prometheus (LimiterSpec): permits=4, acquireTimeout=200ms, maxAttempts=3
  - kafkaPublish (LimiterSpec): permits=8, acquireTimeout=100ms, maxAttempts=3
- LimiterSpec 내부 필드:
  - int permits — 동시 실행 허용 개수 (Semaphore permit 수).
  - Duration acquireTimeout — 1회 tryAcquire 대기 시간.
  - int maxAttempts — 재시도 횟수(첫 시도 포함). 소진 시 ConcurrencyLimitExceededException.
- 기본값 산정 근거:
  - Prometheus: 현재 Assembler 하나의 단일 주기 안에서 메트릭 종류 약 6개 순차 호출 -> 4 permit 이면 향후 가상 스레드 fan-out 시에도 Prometheus 서버에 동시 4 query 로 제한 가능. acquireTimeout 200ms x 3회 ≒ 600ms 까지 대기 후 실패.
  - Kafka publish: 3종 publisher x 동시 발사 + 향후 병렬화 여지 고려해 permit 8. broker ack 지연 시에도 in-flight 8 로 막혀 더 들어오는 send 가 백오프됨. acquireTimeout 100ms x 3회 ≒ 300ms.
  - 외부 환경에서 override 가능 (argus.concurrency.prometheus.permits=2 등).

코드 스케치:

```
@Configuration
@ConfigurationProperties(prefix = "argus.concurrency")
@Getter @Setter
public class ConcurrencyLimitProperties {

    private LimiterSpec prometheus = new LimiterSpec(4, Duration.ofMillis(200), 3);
    private LimiterSpec kafkaPublish = new LimiterSpec(8, Duration.ofMillis(100), 3);

    @Getter @Setter @NoArgsConstructor @AllArgsConstructor
    public static class LimiterSpec {
        private int permits;
        private Duration acquireTimeout;
        private int maxAttempts;
    }
}
```

### 2.4 application.yml 추가 키 예시

```
argus:
  concurrency:
    prometheus:
      permits: 4
      acquire-timeout: 200ms
      max-attempts: 3
    kafka-publish:
      permits: 8
      acquire-timeout: 100ms
      max-attempts: 3
```

### 2.5 ConcurrencyLimiter 시그니처 스케치 (실제 구현은 implementor 단계)

```
package com.example.argus.common.concurrency;

public class ConcurrencyLimiter {
    private final String name;
    private final Semaphore semaphore;
    private final Duration acquireTimeout;
    private final int maxAttempts;

    public ConcurrencyLimiter(String name, int permits, Duration acquireTimeout, int maxAttempts);

    // 동기 실행: permit 획득 -> 실행 -> finally release.
    public <T> T execute(Callable<T> action);

    // 비동기 release 시나리오용: permit 획득만 수행. 실패 시 ConcurrencyLimitExceededException.
    public void acquire();

    // acquire 와 짝을 이루는 release. 예외 완료된 future 의 whenComplete 에서도 호출 안전.
    public void release();

    public int availablePermits();
}
```

### 2.6 빈 등록 스케치

```
@Configuration
@EnableConfigurationProperties(ConcurrencyLimitProperties.class)
public class ConcurrencyLimiterConfig {

    @Bean("prometheusConcurrencyLimiter")
    public ConcurrencyLimiter prometheusConcurrencyLimiter(ConcurrencyLimitProperties p) {
        var s = p.getPrometheus();
        return new ConcurrencyLimiter("prometheus", s.getPermits(), s.getAcquireTimeout(), s.getMaxAttempts());
    }

    @Bean("kafkaPublishConcurrencyLimiter")
    public ConcurrencyLimiter kafkaPublishConcurrencyLimiter(ConcurrencyLimitProperties p) {
        var s = p.getKafkaPublish();
        return new ConcurrencyLimiter("kafka-publish", s.getPermits(), s.getAcquireTimeout(), s.getMaxAttempts());
    }
}
```

---

## 3. 데이터 흐름

### 3.1 Prometheus 조회 경로 (동기 .block())

```
MetricSnapshotScheduler.triggerSnapshot()
  -> *Publisher.publish()
       -> *Assembler.assemble()
            -> PrometheusQueryService.queryByMetric(type) | queryScalar(promql)
                 -> PrometheusClient.query(promql)
                      -> prometheusLimiter.execute(() -> {              [permit 획득 + 짧은 재시도]
                            return prometheusWebClient... .block();
                         })                                              [정상: finally release]
                                                                          [실패: PrometheusQueryException 변환]
```

- permit 획득 실패가 maxAttempts 소진까지 이어지면 ConcurrencyLimitExceededException -> PrometheusClient catch -> PrometheusQueryException 으로 변환 -> PrometheusQueryService -> Assembler 가 기존 queryFailed 분기로 흡수 -> 메트릭 status 만 QUERY_FAILED 로 마킹, 스냅샷 자체는 계속 publish.

### 3.2 Kafka publish 경로 (비동기 future)

```
MetricSnapshotScheduler.triggerSnapshot()
  -> *Publisher.publish()
       -> *Producer.send(serviceId, snapshot)
            (1) kafkaPublishLimiter.acquire()                            [짧은 재시도 후 실패 시]
                   -> 이미 예외 완료된 CompletableFuture 반환
                      (예외: ConcurrencyLimitExceededException)
                   -> 상위 Publisher.whenComplete 에서
                      MetricBufferService.enqueueOnFailure 호출
                      (기존 Redis fallback 경로 재사용)
            (2) acquire 성공 -> try { kafkaTemplate.send(...) } 가 동기 예외 시
                   -> release() 후 예외 완료된 future 반환
            (3) send 가 정상 future 반환
                   -> future.whenComplete((r, ex) -> limiter.release())  [정상/예외 1회 release 보장]
                   -> 반환된 future 반환
```

- 핵심: send 메서드 반환 시점이 아니라 broker ack 까지 permit 을 잡는다. 따라서 broker latency 가 증가하면 후속 send 가 자연스럽게 acquire 단계에서 backoff.

### 3.3 Scheduler 한 주기 안에서 일관 적용
- MetricSnapshotScheduler.triggerSnapshot() 이 JVM/HTTP/Hikari 3종을 순차 호출 -> 각 publish 내부 Assembler 가 여러 Prometheus query 를 호출.
- 모든 query/send 는 같은 prometheusConcurrencyLimiter / kafkaPublishConcurrencyLimiter 빈(싱글톤)을 공유 -> 3종이 동시에 몰려도 전역 상한이 일관되게 적용.

---

## 4. 예외 처리 전략

### 4.1 신규 예외: ConcurrencyLimitExceededException
- 패키지: com.example.argus.exception
- 부모: RuntimeException 직접 상속 (설계 이탈 사유 주석: 공통 CustomException 상위 클래스가 현재 모듈에 없음. PrometheusQueryException 과 동일 정책.)
- 필드: String limiterName, int attempts, long totalWaitedMillis
- 메시지 포맷 예: "concurrency-limit: failed to acquire permit on [limiter] after [attempts] attempts ([totalWaitedMillis]ms)"
- 생성자 2종: (limiterName, attempts, totalWaitedMillis), (limiterName, attempts, totalWaitedMillis, Throwable cause)

### 4.2 ConcurrencyLimiter 내부 정책
- for (int attempt = 1; attempt <= maxAttempts; attempt++) 반복.
- 매 시도 semaphore.tryAcquire(acquireTimeoutMs, MILLISECONDS).
  - true -> action 실행, finally release(), 정상 결과 반환.
  - false -> 누적 대기시간 합산 후 다음 시도. 마지막 실패 시 ConcurrencyLimitExceededException(limiterName, attempts, totalWaitedMillis).
- InterruptedException 발생 시:
  - Thread.currentThread().interrupt() 로 interrupt 상태 복원.
  - 즉시 ConcurrencyLimitExceededException(..., cause=InterruptedException) 으로 변환하여 throw (재시도 X — interrupt 는 의도적 중단 신호).
- execute(Callable) 내부 action 이 던지는 예외:
  - RuntimeException -> 그대로 전파 (try-finally 로 release 보장).
  - Exception (checked) -> RuntimeException 으로 wrap 후 전파 (Callable 시그니처 호환). 본 Task 호출자들은 모두 unchecked 만 던지므로 실질 미발생.

### 4.3 Prometheus 측 변환 매핑 (PrometheusClient)
- 기존 catch 블록 위에 ConcurrencyLimitExceededException 매핑 한 가닥 추가:

```
catch (ConcurrencyLimitExceededException e) {
    log.warn("Prometheus query throttled: {}", e.getMessage());
    throw new PrometheusQueryException("Prometheus query throttled: " + e.getMessage(), e);
}
```

- 기존 Assembler 의 queryFailed 분기가 이를 자연 흡수 -> snapshot 자체는 계속 publish (QUERY_FAILED 상태로).

### 4.4 Kafka 측 변환 매핑 (각 *Producer)
- acquire() 실패 시:
  - log.warn("kafka-publish throttled: serviceId={}, {}", serviceId, e.getMessage())
  - 새 CompletableFuture 를 만들어 completeExceptionally(e) 후 반환.
- 정상 acquire 후 kafkaTemplate.send(...) 가 동기 단계에서 던지는 예외(예: serializer 실패) 가 있을 수 있음 -> try-catch 로 잡아 release 보장 후 동일하게 예외 완료된 future 반환.
- kafkaTemplate.send(...) 가 future 를 반환한 정상 경로에서는 future.whenComplete((r, ex) -> limiter.release()) 로 1회 release.
- 상위 *Publisher 의 기존 whenComplete 가 MetricBufferService.enqueueOnFailure 를 호출 -> Redis fallback 으로 흡수(throttle 케이스도 부하 완화 후 drain 워커가 재시도). 즉, throttle 도 일반적인 send 실패와 동일하게 취급.

### 4.5 permit release 누락 방지
- execute(Callable): try { ... } finally { semaphore.release(); } 표준 패턴.
- Kafka 비동기 경로: acquire 와 release 가 분리되므로 다음 모든 분기에서 release 보장:
  1. acquire() 실패 -> release 호출 금지 (애초에 permit 미보유).
  2. acquire() 성공 후 kafkaTemplate.send 호출 자체가 동기 예외 -> catch 블록에서 limiter.release() + 예외 완료된 future 반환.
  3. acquire() 성공 후 kafkaTemplate.send 가 future 반환 -> future.whenComplete((r, ex) -> limiter.release()) 등록. 이때 future 가 이미 완료 상태여도 whenComplete 콜백은 보장 실행.
- 위 3 분기에서 release 호출이 정확히 acquire 와 1:1 매칭됨을 테스트로 검증(5.4).

### 4.6 로깅 정책
| 시점 | 레벨 | 컨텍스트 |
|---|---|---|
| acquire 실패 (재시도 중) | DEBUG | limiter 이름, 시도 횟수, available permits |
| acquire 최종 실패 | WARN | limiter 이름, 총 시도 횟수, 누적 대기 ms, available permits |
| Prometheus 변환 (PrometheusClient catch) | WARN | promql 요약(앞 80자), limiter 이름 |
| Kafka 변환 (각 Producer catch) | WARN | serviceId, limiter 이름 |
| 정상 release | TRACE 또는 미로깅 | 노이즈 방지 |

- ERROR 는 사용하지 않는다(throttle 은 의도된 정책 동작이며 기존 Assembler/Buffer 가 흡수 가능).

---

## 5. 검증 방법 (테스트 케이스)

모두 JUnit 5 + Mockito 기반(@ExtendWith(MockitoExtension.class)), 기존 argus 테스트 컨벤션 동일(한글 DisplayName 스타일 유지).

### 5.1 신규 테스트 클래스
| 경로 | 검증 대상 |
|---|---|
| argus/src/test/java/com/example/argus/common/concurrency/ConcurrencyLimiterTest.java | Limiter 단위 동작 |
| argus/src/test/java/com/example/argus/config/ConcurrencyLimitPropertiesTest.java | 기본값 + yml 바인딩 |

### 5.2 기존 테스트 수정
| 경로 | 변경 |
|---|---|
| argus/src/test/java/com/example/argus/client/PrometheusClientTest.java | 생성자 인자 추가(테스트 전용 ConcurrencyLimiter — permit 충분, 빠른 timeout — 주입). throttle 시나리오 추가. |
| argus/src/test/java/com/example/argus/messaging/JvmMetricSnapshotProducerTest.java | 동일 패턴 + throttle 시 future 가 예외 완료되는지 검증. |
| argus/src/test/java/com/example/argus/messaging/HttpMetricSnapshotProducerTest.java | 동일. |
| argus/src/test/java/com/example/argus/messaging/HikariMetricSnapshotProducerTest.java | 동일. |
| argus/src/test/java/com/example/argus/service/PrometheusQueryServiceTest.java | 변경 없음 (Client 가 변환을 책임지므로 Service 시그니처 무변경). |
| argus/src/test/java/com/example/argus/service/metric/snapshot/MetricSnapshotSchedulerTest.java | 변경 없음 (Publisher 시그니처 무변경). |
| argus/src/test/java/com/example/argus/service/metric/snapshot/*PublisherTest.java | 변경 없음 (Producer mock 이라 영향 없음). |

### 5.3 ConcurrencyLimiterTest 케이스
1. permit 정상 획득 -> action 실행 후 release : permit=1, action 종료 후 availablePermits()==1.
2. permit 부족 -> 짧은 대기 후 재시도 -> 성공 : permit=1, 다른 스레드에서 점유 -> 일정 시간 후 release -> 메인 스레드가 재시도 중 acquire 성공.
3. 재시도 소진 -> ConcurrencyLimitExceededException : permit=0(또는 점유 후 끝까지 release 안 함), maxAttempts=3, acquireTimeout=10ms -> 예외 메시지에 limiter 이름/시도횟수/누적 대기 ms 포함 검증.
4. action 이 RuntimeException 을 던져도 permit 이 release 됨 : 호출 후 availablePermits() 가 초기치와 동일.
5. InterruptedException 처리 : 메인 스레드를 Thread.interrupt() 후 acquire() 호출 -> ConcurrencyLimitExceededException + Thread.interrupted()==true 검증.
6. 동시 acquire/release 시 permit 카운트 invariant 유지 : permit=3, 10개 스레드가 짧게 점유 -> 종료 후 availablePermits()==3.

### 5.4 Producer 측 throttle 시나리오 (3종 동일)
1. 정상 send : limiter mock 의 acquire() 정상 -> kafkaTemplate.send 호출 -> future 반환 시 release() 1회 호출 검증(verify(limiter, times(1)).release() after future 완료).
2. acquire 실패 시 ConcurrencyLimitExceededException -> future 예외 완료 : doThrow(...).when(limiter).acquire() -> 반환 future 의 isCompletedExceptionally()==true, 원인이 ConcurrencyLimitExceededException. kafkaTemplate.send 미호출 검증.
3. acquire 후 kafkaTemplate.send 가 동기 예외 : when(kafkaTemplate.send(...)).thenThrow(...) -> release() 1회 호출 + 예외 완료된 future 반환.
4. send 가 정상 future 반환 후 future 예외 완료 : 정상 send -> future.completeExceptionally -> release() 1회 호출(중복 호출 X) 검증.

### 5.5 PrometheusClient 측 throttle 시나리오
1. 정상 query : limiter mock 이 action 을 정상 invoke -> 기존 정상 응답 매핑 검증(회귀).
2. limiter 가 ConcurrencyLimitExceededException 을 던질 때 : when(limiter.execute(any())).thenThrow(...) -> PrometheusQueryException 으로 변환되어 throw, cause 가 ConcurrencyLimitExceededException 검증.
3. 기존 WebClientResponseException / 일반 Exception 변환 회귀 : 기존 케이스 그대로 PASS.

### 5.6 ConcurrencyLimitPropertiesTest
- @SpringBootTest 까지는 불필요. Binder.get(env).bind(...) 또는 @EnableConfigurationProperties + @TestConfiguration 으로 가벼운 컨텍스트만 띄워 기본값/override 검증.
- 검증: 기본값(permits=4/8, acquireTimeout=200ms/100ms, maxAttempts=3) + yml override 반영.

### 5.7 회귀 보증
- 기존 MetricSnapshotSchedulerTest, *MetricSnapshotPublisherTest, *MetricSnapshotAssemblerTest, PrometheusQueryServiceTest, MetricBufferServiceTest 무변경 PASS.
- 기존 PrometheusClientTest 의 정상/에러 케이스가 그대로 PASS (생성자에 limiter 주입 추가만 반영).

---

## 6. 트레이드오프

### 6.1 채택 메커니즘(Semaphore)의 단점
- Resilience4j 대비 메트릭/대시보드가 자동으로 노출되지 않는다. 운영 가시성이 필요한 경우 후속 Task 에서 Micrometer Gauge 로 availablePermits() 를 노출하거나 Resilience4j Bulkhead 로 마이그레이션해야 한다(시그니처 변경 없이 교체 가능하도록 Limiter 가 내부 캡슐화되어 있음).
- queue 가 없어 acquireTimeout 동안만 짧게 대기 -> broker 가 일시적으로 느려질 때 throttle 비율이 spikey 해질 수 있다. 대신 acquireTimeout/maxAttempts 를 환경별로 조정.

### 6.2 의도적으로 빠진 것 (Out of Scope 재확인)
- 가상 스레드 활성화 (spring.threads.virtual.enabled) — 다음 Task.
- WebClient -> RestClient 교체.
- Mono.zip 같은 Prometheus 병렬 조회 전면 재작성.
- durable 버퍼 추가(Redis fallback 은 Task 023 으로 이미 구축됨 — 본 Task 는 그 위에 안전장치만 얹는다).
- broker-side / Prometheus-side rate limit 설정 변경.
- Resilience4j 의존성 도입.

### 6.3 향후 확장 포인트
- 메트릭 노출 : ConcurrencyLimiter 에 MeterRegistry 주입을 추가하고 availablePermits / acquireFailures 카운터를 export. 본 Task 에서는 미적용.
- 인스턴스 단위 분리 : 메트릭 타입별로 별도 limiter 가 필요해지면 LimiterSpec 을 Map<String, LimiterSpec> 로 확장하고 ConcurrencyLimiterConfig 에서 빈을 동적 등록.
- 백프레셔 신호 : availablePermits()==0 이 지속되면 Scheduler 쪽에서 다음 주기를 skip 하는 정책으로 발전 가능.
- 가상 스레드 fan-out : Assembler 가 StructuredTaskScope 로 메트릭 종류를 동시에 조회할 때, Client 진입점의 limiter 만으로 동시성이 자연스럽게 상한선을 유지하므로 Limiter 측 변경 없이 도입 가능.
- Resilience4j 마이그레이션 : Limiter 인터페이스(execute, acquire, release)를 그대로 두고 내부 구현을 Bulkhead 로 교체하면 호출 측 코드 변경 0.

---

## 7. 완료 조건 체크리스트
- [ ] ConcurrencyLimitProperties 추가 + application.yml 키 등록.
- [ ] ConcurrencyLimiter + ConcurrencyLimiterConfig 추가, prometheusConcurrencyLimiter / kafkaPublishConcurrencyLimiter 두 빈 등록.
- [ ] ConcurrencyLimitExceededException 추가.
- [ ] PrometheusClient 가 prometheusConcurrencyLimiter.execute(...) 로 query 를 감싸고, ConcurrencyLimitExceededException 을 PrometheusQueryException 으로 변환.
- [ ] JvmMetricSnapshotProducer, HttpMetricSnapshotProducer, HikariMetricSnapshotProducer 가 kafkaPublishConcurrencyLimiter.acquire() -> kafkaTemplate.send(...) -> whenComplete(release) 패턴 적용.
- [ ] acquire 실패 / 동기 send 예외 / future 예외 완료 모든 분기에서 release 가 정확히 1회 호출되는지 테스트로 검증.
- [ ] permit 부족 -> 재시도 -> 성공 / 재시도 소진 -> ConcurrencyLimitExceededException 시나리오 테스트.
- [ ] InterruptedException 처리 검증.
- [ ] 기존 단위 테스트 전부 PASS (회귀 없음).
- [ ] ./gradlew :argus:test 통과.
- [ ] DTO / PromQL / Kafka 메시지 포맷 변경 0 확인.
- [ ] MetricSnapshotScheduler 한 주기 안에서 JVM/HTTP/Hikari 가 동일 limiter 빈을 공유하는지 (싱글톤) 확인.
