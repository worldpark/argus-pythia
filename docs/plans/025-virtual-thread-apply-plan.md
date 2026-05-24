# 025. Argus 가상 스레드 활성화 및 병렬 수집 경로 적용 - Plan

> Source Task: docs/tasks/025-virtual-thread-apply.md
> Target: argus
> 본 Plan 은 Java 21 가상 스레드를 활성화하고, 세 종(JVM/HTTP/Hikari) Assembler 내부의 Prometheus 조회를 가상 스레드 기반 `CompletableFuture` fan-out 으로 병렬화한다.
> Task 024 산출물(ConcurrencyLimiter) 의 위치/시그니처는 그대로 유지하며, 가상 스레드 환경에서도 동일하게 동작하도록 결합 지점만 점검한다.
> WebClient -> RestClient 교체, Reactor 전면 재작성(`Mono.zip`), PromQL/DTO/Kafka 포맷/스케줄 주기 변경, durable 버퍼 재설계는 본 Task 범위 외.

---

## 0. 구현 목표 (한 줄 요약)
Argus 에 `spring.threads.virtual.enabled=true` 를 켜고, JVM/HTTP/Hikari Assembler 가 호출하는 다수 PromQL 호출을 **공용 가상 스레드 executor** 로 fan-out + `CompletableFuture.allOf(...).join()` 으로 합류시켜, 한 주기 안의 메트릭 수집 시간을 줄이고 가상 스레드의 IO 친화성을 활용한다. Task 024 의 동시성 상한과 재시도 정책은 변경 없이 그대로 적용된다.

---

## 1. 설계 방식 및 이유

### 1.1 가상 스레드 활성화 방식: `spring.threads.virtual.enabled=true` + 공용 executor 빈 등록 (병행)
- `spring.threads.virtual.enabled=true` 만으로 자동 적용되는 영역
  - Tomcat request worker (Spring Boot 4.x 에서 적용. argus 는 webmvc + webflux 둘 다 의존성으로 잡혀 있으나 `server.port=80` 기반의 webmvc 가 주 진입점).
  - `@Async` 의 기본 SimpleAsyncTaskExecutor 가 가상 스레드 모드로 전환 (현재 argus 코드에서는 `@Async` 사용처 없음).
  - `@Scheduled` 의 기본 TaskScheduler (`spring.task.scheduling.*` 에 별도 설정이 없으면 가상 스레드 SimpleAsyncTaskScheduler 가 사용됨).
- 자동 적용되지 않는 영역 (수동 처리 필요)
  - **Assembler 내부의 fan-out 실행자**: `CompletableFuture.supplyAsync(..., executor)` 에 명시적으로 가상 스레드 executor 를 주입해야 한다. 미주입 시 `ForkJoinPool.commonPool()` 로 떨어져 의도가 깨진다.
- 결정
  - 두 가지를 **함께** 적용한다. `spring.threads.virtual.enabled=true` 는 큰 그림(스케줄러/요청 처리)을 가상 스레드로 끌어올리고, Assembler fan-out 은 명시적인 `metricFanoutExecutor` 빈을 주입 받아 사용한다.

### 1.2 병렬화 적용 위치 선택
| 위치 | 적용 여부 | 사유 |
|---|---|---|
| Assembler 내부 (JVM/HTTP/Hikari 각각의 여러 PromQL 호출) | **채택** | 동일 Assembler 안에서 JVM 8개(CPU/HEAP/HEAP_OLD/GC_DUR/GC_CNT/ACTIVE/PEAK/DAEMON), HTTP 3개(P99/RPS/ERROR), Hikari 3개(ACTIVE/USAGE/PENDING)의 PromQL 이 순차 .block() 으로 실행되어 IO wait 누적이 가장 큰 구간. fan-out 효과 즉시 측정 가능. |
| Publisher 간 (JVM/HTTP/Hikari Publisher 3개) | **미채택** | Scheduler 가 이미 `whenComplete` 비동기 콜백으로 3종을 fire-and-await 형태로 발사 중이며, 각 publish 의 비용 대부분은 Assembler 안의 PromQL 호출. Publisher 간 fan-out 은 코드 복잡도만 늘리고 효과 한계. 본 Task 의 "스냅샷 유형 간 병렬화 여부도 명시" 요구에 대해 **명시적 미적용**. |
| Producer 내부 | 미채택 | 이미 비동기 `kafkaTemplate.send` future 반환. 추가 fan-out 무의미. |

### 1.3 병렬 실행 메커니즘 선택
| 후보 | 채택 여부 | 사유 |
|---|---|---|
| `CompletableFuture.supplyAsync(..., virtualExecutor)` + `CompletableFuture.allOf(...).join()` | **채택** | Java 21 정식 API. 기존 동기 코드(`.block()` 기반) 와 자연 결합. 부분 실패 흡수가 `handle/exceptionally` 로 명료. Mockito 친화적. |
| `Executors.newVirtualThreadPerTaskExecutor()` 를 매 호출마다 new | 미채택 | 매 주기마다 executor 인스턴스를 새로 만들 이유 없음. 라이프사이클 관리 곤란. 공용 빈 1개로 충분(가상 스레드는 풀이 아니라 매 작업마다 새 스레드 생성). |
| `StructuredTaskScope` (`StructuredTaskScope.ShutdownOnFailure` 등) | 미채택 | Java 21 기준 **preview API** (JEP 453 - Preview). `--enable-preview` 컴파일/실행 옵션이 필요해 운영 부담 발생. 부분 실패를 허용하는 본 Task 요구(일부 query 실패해도 다른 부분은 정상) 와도 `ShutdownOnFailure` 의 의미가 어긋남. Java 25 정식화 시점에 재검토. |
| `Mono.zip` / Reactor 전면 | 미채택 | Task 가 명시적으로 제외. |
| `parallelStream()` | 미채택 | 기본 ForkJoinPool 사용 -> 가상 스레드 활용 불가. CompletableFuture custom executor 가 더 명료. |

### 1.4 공용 가상 스레드 executor 라이프사이클
- `Executors.newVirtualThreadPerTaskExecutor()` 를 Spring 빈으로 1회 등록(싱글톤).
- 가상 스레드는 작업마다 새로 만들고 종료 후 회수되므로 **풀 크기 튜닝 불필요**. 빈 자체는 thread factory 역할.
- `@Bean(destroyMethod = "close")` 로 ApplicationContext 종료 시 executor 도 함께 close.
- 사용자 코드는 이 빈을 주입받아 `supplyAsync(...)` 의 executor 파라미터로 전달.

### 1.5 Task 024 ConcurrencyLimiter 와의 결합
- `prometheusConcurrencyLimiter` 는 `Semaphore(fair=true)` 기반. **JDK 21 의 가상 스레드는 `Semaphore.tryAcquire` 에서 정상적으로 park 되며 캐리어 스레드를 점유하지 않는다** (JEP 444 정식. pinned 발생하지 않음).
- Assembler 가 N개 가상 스레드로 fan-out 하더라도 PromQL 호출은 결국 `PrometheusClient.query(...) -> limiter.execute(...)` 를 지나므로 동시 진입은 `permits=4` 로 자연 제한.
- permit 부족이 maxAttempts 까지 이어지면 기존과 동일하게 `ConcurrencyLimitExceededException -> PrometheusQueryException` 변환 -> Assembler 의 `queryFailed` 분기로 흡수. 본 Task 에서 새로 처리할 게 없음.
- Kafka publish 측 limiter (`kafkaPublishConcurrencyLimiter`) 는 Scheduler 가 3종을 순차 호출하므로 fan-out 대상 아님. 별도 변경 없음.

### 1.6 `spring.main.keep-alive` 필요성
- 적용 대상: argus 는 `spring-boot-starter-webmvc` 를 의존성으로 가지므로 내장 Tomcat 이 non-daemon 스레드를 유지한다 -> 일반적인 webmvc 기동만으로는 종료되지 않는다.
- 그러나 본 Task 의 "스케줄러 환경에서 가상 스레드 활성화 후 종료/유휴 상태 문제가 없도록 검증한다" 요구를 고려, **방어적으로 `spring.main.keep-alive=true` 를 추가**한다.
  - 이유: `spring.threads.virtual.enabled=true` 환경에서 `@Scheduled` 의 기본 TaskScheduler 가 가상 스레드(데몬) 기반으로 전환된다. webmvc 가 없거나 향후 어떤 이유로 non-daemon 스레드가 없어지는 변화가 생기면 JVM 이 조기 종료될 수 있는데, `spring.main.keep-alive=true` 가 Spring 의 keep-alive non-daemon 스레드를 명시적으로 유지하여 이를 막는다.
  - 부수효과 거의 없음(스레드 1개 추가).
- 채택.

### 1.7 pinned virtual thread 회피
- 신규 코드에서 `synchronized` 블록을 **새로 도입하지 않는다**. 동시성이 필요한 경우 `ReentrantLock` 또는 표준 동시 컬렉션을 사용한다.
- 기존 Assembler 의 `LabelAccumulator` 내부 상태는 fan-out 결과를 단일 main 스레드에서 순차 적용하는 패턴으로 유지 -> 동시 접근 자체가 발생하지 않음(아래 3.2 데이터 흐름 참조).
- WebClient/Reactor Netty 내부의 잠재적 pin 위험은 본 Task 범위 외(WebClient 교체 금지). 다만 측정 절차(5.5)에서 `-Djdk.tracePinnedThreads=full` 옵션으로 운영 시 관찰 가능하도록 문서화.

---

## 2. 구성 요소

### 2.1 신규 파일
| 경로 | 책임 |
|---|---|
| `argus/src/main/java/com/example/argus/config/VirtualThreadExecutorConfig.java` | `metricFanoutExecutor` 라는 이름의 가상 스레드 executor 빈을 등록. `destroyMethod="close"`. |

> 결정: 기존 `SchedulingConfig.java` 에 통합하지 않는다. SchedulingConfig 는 `@EnableScheduling` + `@ConditionalOnProperty` 한 줄짜리 책임만 갖고 있어 의도가 명료. executor 등록은 별도 Config 로 분리해 책임을 섞지 않는다.

### 2.2 수정 파일
| 경로 | 변경 내용 |
|---|---|
| `argus/src/main/resources/application.yml` | `spring.threads.virtual.enabled: true`, `spring.main.keep-alive: true` 추가. 그 외 키 변경 없음. |
| `argus/src/main/java/com/example/argus/service/metric/snapshot/JvmMetricSnapshotAssembler.java` | 생성자에 `@Qualifier("metricFanoutExecutor") Executor fanoutExecutor` 주입. `assemble()` 내부 8개 PromQL 호출(CPU, HEAP, HEAP_OLD, GC_DUR, GC_CNT, ACTIVE, PEAK, DAEMON) 을 `CompletableFuture.supplyAsync(..., fanoutExecutor)` 로 fan-out 후 `allOf().join()` 으로 합류. 라벨 누적은 합류 후 단일 스레드에서 순차 적용. |
| `argus/src/main/java/com/example/argus/service/metric/snapshot/HttpMetricSnapshotAssembler.java` | 동일 executor 주입. 3개 PromQL 호출(P99 / RPS / ERROR_RATE) 을 fan-out. |
| `argus/src/main/java/com/example/argus/service/metric/snapshot/HikariMetricSnapshotAssembler.java` | 동일 executor 주입. ACTIVE / USAGE_RATIO / PENDING 3개 호출을 fan-out (ACTIVE 그룹은 두 query 의 결합이므로 그 안에서도 fan-out). |

### 2.3 신규 ConfigurationProperties — **미도입**
- 본 Task 는 "on/off 토글", "executor 풀 크기 조절" 같은 외부화를 요구하지 않는다.
- `spring.threads.virtual.enabled` 자체가 가상 스레드 활성화의 표준 스위치이며, 별도 토글을 만들면 두 곳에서 같은 상태를 관리하게 되어 혼란을 초래.
- 가상 스레드 executor 는 풀 크기 개념이 없으므로 외부화 가치 0.
- 따라서 `ConcurrencyLimitProperties` 외에 새 properties 클래스는 추가하지 않는다.

### 2.4 VirtualThreadExecutorConfig 스케치

```
package com.example.argus.config;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class VirtualThreadExecutorConfig {

    @Bean(name = "metricFanoutExecutor", destroyMethod = "close")
    public ExecutorService metricFanoutExecutor() {
        return Executors.newVirtualThreadPerTaskExecutor();
    }
}
```

- 빈 타입은 `ExecutorService` 로 노출(주입 측은 `Executor` 로도 받을 수 있음).
- `destroyMethod="close"` 는 Java 21 의 `ExecutorService implements AutoCloseable` 활용 -> ApplicationContext 종료 시 자동 close.

### 2.5 application.yml 추가 키

```yaml
spring:
  threads:
    virtual:
      enabled: true
  main:
    keep-alive: true
```

- 다른 기존 키(`spring.kafka.*`, `spring.data.redis.*`, `argus.*`) 는 변경하지 않는다.

---

## 3. 데이터 흐름

### 3.1 변경 전 (순차)

```
Scheduler.triggerSnapshot()
  -> JvmMetricSnapshotPublisher.publish()
       -> JvmMetricSnapshotAssembler.assemble()
            -> queryService.queryByMetric(CPU_USAGE)         [.block(), 대기 X ms]
            -> queryService.queryByMetric(HEAP_USAGE)        [.block(), 대기 X ms]
            -> queryService.queryByMetric(HEAP_OLD_GEN_USAGE)
            -> queryService.queryByMetric(GC_AVG_DURATION)
            -> queryService.queryByMetric(GC_COUNT)
            -> queryService.queryByMetric(ACTIVE_THREADS)
            -> queryService.queryByMetric(PEAK_THREADS)
            -> queryService.queryByMetric(DAEMON_THREADS)
            // 8개 직렬 IO wait 누적
```

### 3.2 변경 후 (가상 스레드 fan-out)

```
Scheduler.triggerSnapshot()
  -> JvmMetricSnapshotPublisher.publish()
       -> JvmMetricSnapshotAssembler.assemble()
            // 1) fan-out: 메트릭 타입별로 supplyAsync 제출
            f_cpu     = supplyAsync(() -> safeQuery(CPU_USAGE),         metricFanoutExecutor)
            f_heap    = supplyAsync(() -> safeQuery(HEAP_USAGE),        metricFanoutExecutor)
            f_old     = supplyAsync(() -> safeQuery(HEAP_OLD_GEN_USAGE),metricFanoutExecutor)
            f_gcDur   = supplyAsync(() -> safeQuery(GC_AVG_DURATION),   metricFanoutExecutor)
            f_gcCnt   = supplyAsync(() -> safeQuery(GC_COUNT),          metricFanoutExecutor)
            f_active  = supplyAsync(() -> safeQuery(ACTIVE_THREADS),    metricFanoutExecutor)
            f_peak    = supplyAsync(() -> safeQuery(PEAK_THREADS),      metricFanoutExecutor)
            f_daemon  = supplyAsync(() -> safeQuery(DAEMON_THREADS),    metricFanoutExecutor)

            // 2) 합류: 어떤 부분이 실패해도 전체는 join 까지 진행
            CompletableFuture.allOf(f_cpu, f_heap, ...).join()

            // 3) 단일 스레드에서 라벨 누적 + DTO 조립 (LabelAccumulator 동시 접근 없음)
            cpuDto    = CpuUsageDto.from(applyLabels(f_cpu.join(), labels))
            memoryDto = MemoryUsageDto.from(applyLabels(f_heap.join(), labels),
                                            applyLabels(f_old.join(),  labels))
            gcDto     = GcMetricDto.from(applyLabels(f_gcDur.join(), labels),
                                         applyLabels(f_gcCnt.join(), labels))
            threadDto = ThreadMetricDto.from(applyLabels(f_active.join(), labels),
                                             applyLabels(f_peak.join(),   labels),
                                             applyLabels(f_daemon.join(), labels))
            -> SnapshotStatus.from(...) -> JvmMetricSnapshotDto
```

핵심 포인트:
1. `safeQuery(type)` 는 try / catch (PrometheusQueryException) / catch (Exception) 패턴으로 모든 예외를 MappingResult 의 QueryFailed / ParseFailed 로 정규화하는 helper. **fan-out task 자체는 절대 throw 하지 않는다** -> `allOf(...).join()` 이 `CompletionException` 으로 깨지지 않음.
2. 라벨 누적(`LabelAccumulator.accept`) 은 합류 후 main 스레드에서만 호출 -> 동시성 X.
3. HTTP/Hikari 도 동일 패턴. HTTP 는 P99/RPS/ERROR_RATE 3개, Hikari 는 ACTIVE/USAGE_RATIO/PENDING 3개.

### 3.3 ConcurrencyLimiter 와의 상호작용 (Task 024 재사용)

```
가상 스레드 N개가 동시에 PrometheusClient.query 진입
  -> prometheusConcurrencyLimiter.execute(callable)
       -> Semaphore.tryAcquire(200ms, ms)
            - permit (max 4) 보유자: 즉시 진입 -> WebClient .block() (가상 스레드 park, 캐리어 미점유)
            - permit 대기자: tryAcquire 가 가상 스레드 park (캐리어 미점유)
            - 200ms 내 미획득: 다음 attempt (총 3회까지)
            - 최종 실패: ConcurrencyLimitExceededException -> PrometheusClient catch -> PrometheusQueryException
  -> Assembler 의 safeQuery 가 PrometheusQueryException 을 QueryFailed 로 흡수
  -> snapshot 자체는 partial status 로 계속 publish
```

- 가상 스레드 친화 검증: `java.util.concurrent.Semaphore` 는 `LockSupport.park` 기반이므로 JEP 444 정식 가상 스레드 환경에서 캐리어를 점유하지 않는다 (반대로 `synchronized` 블록 안에서 park 되면 pin 발생).

### 3.4 Scheduler / Publisher 변경 없음
- `MetricSnapshotScheduler.triggerSnapshot()` 시그니처/흐름 무변경.
- 3종 `*MetricSnapshotPublisher.publish()` 시그니처/흐름 무변경.
- 변경은 오직 3종 Assembler 의 `assemble()` 내부 구현뿐.

---

## 4. 예외 처리 전략

### 4.1 fan-out task 내부에서 예외를 throw 하지 않는다
- `safeQuery(type)` helper 가 모든 예외를 `MappingResult.QueryFailed(msg)` / `MappingResult.ParseFailed(msg)` 로 정규화.
- 이유: `CompletableFuture.allOf(...).join()` 이 어떤 부분 실패에도 `CompletionException` 으로 깨지지 않게 보장 -> 부분 실패가 다른 부분의 결과를 무효화시키지 않음(스냅샷 PARTIAL 상태 보장).
- 결과적으로 `f_xxx.join()` 호출 시 `MappingResult` 인스턴스(Success/EmptyResult/QueryFailed/ParseFailed) 가 반환됨이 보장됨.

### 4.2 `allOf(...).join()` 의 `CompletionException` 처리
- 4.1 정규화에 의해 정상 경로에서는 발생하지 않음.
- 그러나 방어적으로 `assemble()` 전체를 try-catch CompletionException 으로 감싸지 않는다(이전 상태와 동일하게 RuntimeException 이 발생하면 Publisher 측에서 잡힘 — Publisher 는 이미 Kafka future fallback 을 가지고 있고, Scheduler 도 try-catch 를 갖고 있음).
- 의도치 않은 예외가 발생할 가능성을 줄이기 위해 `safeQuery` 단위에서 `Throwable` 아닌 `Exception` 으로 잡는다(VirtualMachineError 등 Error 는 상위로 전파).

### 4.3 가상 스레드 인터럽트/취소
- `metricFanoutExecutor.close()` 가 ApplicationContext 종료 시 호출됨 -> 진행 중 task 들이 interrupt 됨.
- 진행 중 task 안의 `PrometheusClient.query` -> `limiter.execute` -> `Semaphore.tryAcquire` 가 `InterruptedException` 을 받으면 기존 `ConcurrencyLimiter` 로직(`Thread.currentThread().interrupt()` 호출 + `ConcurrencyLimitExceededException(cause=InterruptedException)` 변환) 이 그대로 동작.
- 변환된 예외는 `safeQuery` 에서 QueryFailed 로 흡수되거나, 종료 컨텍스트에서는 join 이 깨질 수 있음. 단, 종료 시점에 한 주기 snapshot 이 누락되는 것은 의도된 동작.

### 4.4 pinned thread 진단
- 새 코드에서 `synchronized` 도입 금지. `LabelAccumulator` 는 합류 후 단일 스레드에서만 사용 -> 동기화 자체가 불필요.
- 운영 진단용으로 JVM 옵션 `-Djdk.tracePinnedThreads=full` 사용을 README/주석에서 가이드(코드 변경 X).

### 4.5 로깅 정책
- Assembler 의 기존 `log.error("Unexpected error while querying {}", type, e)` 는 fan-out task 안에서 그대로 호출됨 -> 출력 스레드 이름이 가상 스레드 이름(`virtual-...`) 으로 표시될 뿐 내용 동일.
- 새 로그 추가는 측정 절차(5.5) 의 시간 측정용 1개로 제한.

---

## 5. 검증 방법

### 5.1 신규 테스트 클래스
| 경로 | 검증 대상 |
|---|---|
| `argus/src/test/java/com/example/argus/config/VirtualThreadExecutorConfigTest.java` | `metricFanoutExecutor` 빈이 등록되고, 제출한 task 가 `Thread.currentThread().isVirtual() == true` 인 스레드에서 실행됨을 검증. |

### 5.2 기존 테스트 수정
| 경로 | 변경 |
|---|---|
| `argus/src/test/java/com/example/argus/service/metric/snapshot/JvmMetricSnapshotAssemblerTest.java` | 생성자 인자 추가(executor 주입). 테스트에서는 가상 스레드 환경 검증을 위해 `Executors.newVirtualThreadPerTaskExecutor()` 를 `@BeforeEach` 에서 생성하고 `@AfterEach` 에서 close. 기존 케이스 전부 그대로 PASS 해야 한다(회귀). 추가 케이스(5.3) 1개. |
| `argus/src/test/java/com/example/argus/service/metric/snapshot/HttpMetricSnapshotAssemblerTest.java` | 동일 패턴. |
| `argus/src/test/java/com/example/argus/service/metric/snapshot/HikariMetricSnapshotAssemblerTest.java` | 동일 패턴. |
| 그 외 (`*PublisherTest`, `MetricSnapshotSchedulerTest`, `PrometheusClientTest`, `*ProducerTest`, `ConcurrencyLimiterTest`, etc.) | 변경 없음 — 시그니처 무변경. |

### 5.3 신규/추가 케이스
1. `JvmMetricSnapshotAssemblerTest#assemble_runsQueriesOnVirtualThreads`
   - `queryService.queryByMetric(any())` 호출 시점에 현재 스레드가 virtual 인지 기록할 수 있도록 `Mockito.doAnswer(...)` 사용.
   - 모든 stub 응답 정상으로 두고 `assemble()` 호출 -> 기록된 스레드들이 모두 `isVirtual()==true` 임을 assertThat.
2. 회귀: 기존 케이스(`assemble_allSuccess_returnsCompleteSnapshot`, `assemble_cpuQueryFails_returnsPartialWithCpuQueryFailed`, `assemble_allFail_returnsFailedSnapshot`, ...) 가 가상 스레드 fan-out 환경에서도 동일한 DTO 를 만들어야 함.
3. HTTP/Hikari 도 동일 패턴 1개씩 추가(`runsQueriesOnVirtualThreads`).

### 5.4 ConcurrencyLimiter 결합 검증 (수동 통합 — 가벼운 단위 테스트)
- 추가 케이스 1개를 `ConcurrencyLimiterTest` 에 추가:
  - `acquireFromVirtualThreads_doesNotPin_andRespectsPermit`
  - permit=2, 가상 스레드 8개에서 동시 execute, 각 action 이 10ms sleep -> 종료 후 `availablePermits()==2` 유지 + 동시 in-flight 가 2를 초과하지 않음을 AtomicInteger.peak 으로 검증.
  - pin 검증은 본 테스트 범위 밖(JVM flag 필요). 대신 "가상 스레드에서 Semaphore 호출이 동작한다" 만 보장.

### 5.5 성능/회귀 측정 절차 (자동화 X — 문서화만)
> 본 Task 의 "기존 대비 한 주기 처리 시간 비교" 요구를 충족하되, 성능 자동 테스트는 flaky 위험으로 단위 테스트화하지 않고 절차를 본 plan + 운영 노트에 남긴다.

1. 측정 대상: `JvmMetricSnapshotAssembler.assemble()` / `HttpMetricSnapshotAssembler.assemble()` / `HikariMetricSnapshotAssembler.assemble()` 각각의 wall-clock 소요시간.
2. 측정 방법 (코드 변경 최소화):
   - 각 Assembler `assemble()` 시작 시 `long start = System.nanoTime()` 기록, 반환 직전 `log.info("metric-assemble: type={} elapsedMs={}", "JVM", (System.nanoTime()-start)/1_000_000)` 1회. (DEBUG 가 아니라 INFO 로 두어 운영에서 비교 가능. 운영 부담은 60초마다 1회씩 3 로그 -> 무시 가능.)
3. 비교:
   - 브랜치 적용 전(main) 측정값 vs 적용 후 측정값을 동일 Prometheus 인스턴스/동일 부하에서 3주기 평균 비교.
   - 기대: 가상 스레드 fan-out 후 JVM(8 query) 가 가장 큰 단축 효과, HTTP/Hikari(3 query) 는 단축 효과 비교적 작음.
4. 회귀 모니터링:
   - 한 주기 내 QueryFailed 메트릭 수가 적용 전 대비 통계적으로 유의미하게 증가하지 않는지(throttle 폭주 여부) 로그로 확인.
   - JVM 옵션 `-Djdk.tracePinnedThreads=full` 켜고 한 시간 운영 -> pin trace 출력 없음을 확인.
5. 안전성:
   - 위 `log.info` 1줄 추가는 본 Task 의 "관측 가능한 로그 또는 측정 근거를 남길 수 있어야 한다" 요구의 최소 구현이며, 자동 테스트화하지 않음.

### 5.6 스케줄러 조기 종료 검증 방법
- 자동 테스트 미작성(수동 검증). `spring.threads.virtual.enabled=true` + `spring.main.keep-alive=true` 적용 후 로컬에서 `./gradlew :argus:bootRun` 으로 기동 -> 한 주기 이상 정상 동작 + 임의로 60초 이상 idle 시 JVM 이 종료되지 않음을 확인. 본 Task 완료 후 운영 노트에 결과 기록.
- 단위 테스트 차원에서는 `VirtualThreadExecutorConfigTest` 가 `ExecutorService` 빈이 `close()` 가능(`AutoCloseable`) 함을 검증하여 라이프사이클 핸들링이 정상임을 간접 확인.

### 5.7 회귀 보증 체크리스트
- 기존 `MetricSnapshotSchedulerTest`, `*MetricSnapshotPublisherTest`, `*MetricSnapshotAssemblerTest`(생성자 시그니처 변경분 적용 후), `PrometheusQueryServiceTest`, `PrometheusClientTest`, `*ProducerTest`, `ConcurrencyLimiterTest`, `MetricBufferServiceTest`, `MetricBufferStoreTest`, `MetricBufferDrainServiceTest`, DTO 계열 테스트 — 전부 PASS.
- `./gradlew :argus:test` 전체 통과.

---

## 6. 트레이드오프

### 6.1 가상 스레드 적용 이점
- IO-bound Prometheus 호출(`.block()`) 들이 가상 스레드 park 로 캐리어 점유 0 -> 동일 머신에서 더 많은 동시 호출 처리 가능.
- 8개 query 가 직렬 wait 이 아니라 wall-clock 거의 max(query latency) 수준으로 단축 (병렬화 효과). `ConcurrencyLimiter(permits=4)` 가 동시 진입을 4로 제한하므로 실제 wall-clock 은 약 ceil(8/4) * single-query-latency.
- ConcurrencyLimiter 가 외부 시스템 보호를 책임지므로 fan-out 으로 인한 Prometheus 과부하 우려는 차단.

### 6.2 단점/위험
- pinned thread: `synchronized` 또는 native call 안에서 park 시 캐리어 점유 발생. 본 plan 은 신규 `synchronized` 도입을 금지하지만, WebClient/Reactor Netty 의 내부 구현에서 잠재 위험이 있을 수 있음. 측정 절차(5.5) 의 `-Djdk.tracePinnedThreads=full` 로 운영 모니터링.
- 스택 트레이스: 가상 스레드 스택은 더 길어 보일 수 있으나 Java 21 에서 jdb/jstack 지원이 향상되어 디버깅 가능.
- 짧은 query 의 경우: 모든 query 가 매우 빠를 때(<1ms) fan-out overhead 가 직렬보다 더 클 수 있음. 그러나 PromQL 평가 + HTTP RTT 는 일반적으로 수십 ms 단위라 overhead 무시 가능.
- 테스트 안정성: 가상 스레드 도입으로 race 가 드러날 수 있음. 본 plan 의 LabelAccumulator 합류 후 단일 스레드 처리 패턴이 이를 회피.

### 6.3 채택하지 않은 대안
- `StructuredTaskScope` — Java 21 preview, `--enable-preview` 필요. Java 25 정식화 시점에 재검토.
- `Mono.zip` / Reactor 전면 — Task 가 명시적으로 제외.
- `WebClient` -> `RestClient` 교체 — Task 가 명시적으로 제외.
- `parallelStream` — 가상 스레드 미활용.
- Publisher 간 fan-out — Task 024 의 진입점 책임 분리와 충돌 + 효과 미미.
- 새 ConfigurationProperties — 외부화 가치 없음(1.4, 2.3 참조).
- `@Async` + 가상 스레드 SimpleAsyncTaskExecutor — `CompletableFuture` 합류 패턴이 더 명료(allOf, exceptionally 사용). `@Async` 는 본 코드베이스에 사용처 없음.

### 6.4 향후 확장
- 메트릭 종류 추가 시: Assembler 의 fan-out 패턴이 그대로 적용되어 추가 비용 거의 0.
- StructuredTaskScope 정식화(Java 25 이후) 시: `metricFanoutExecutor` 빈을 `StructuredTaskScope` 로 교체. 호출 측 변경 최소.
- Micrometer Timer 도입: 5.5 의 `log.info` 측정을 `Timer.record` 로 대체하여 Prometheus 자체 메트릭으로 노출 가능(본 Task 범위 외).
- pin 알람: `-Djdk.tracePinnedThreads=full` 의 stderr 출력을 로그 수집기로 모니터링하는 운영 절차 추가.

---

## 7. 완료 조건 체크리스트
- [ ] `application.yml` 에 `spring.threads.virtual.enabled: true` + `spring.main.keep-alive: true` 추가.
- [ ] `VirtualThreadExecutorConfig` 추가, `metricFanoutExecutor` 빈 등록 (destroyMethod="close").
- [ ] `JvmMetricSnapshotAssembler` 가 `metricFanoutExecutor` 를 주입받아 8개 PromQL 을 fan-out 합류.
- [ ] `HttpMetricSnapshotAssembler` 가 동일 executor 로 3개 PromQL fan-out 합류.
- [ ] `HikariMetricSnapshotAssembler` 가 동일 executor 로 3개 PromQL fan-out 합류.
- [ ] 3종 Assembler 모두 `safeQuery` 정규화로 fan-out task 내부에서 throw 하지 않음.
- [ ] LabelAccumulator 는 합류 후 main 스레드에서만 갱신 (동시성 X).
- [ ] 신규 `synchronized` 도입 0.
- [ ] 각 Assembler `assemble()` 의 wall-clock 측정 로그 1줄 추가 (5.5).
- [ ] `VirtualThreadExecutorConfigTest` 가 가상 스레드 실행을 검증.
- [ ] 3종 `*AssemblerTest` 에 `runsQueriesOnVirtualThreads` 케이스 추가, 기존 케이스 전부 PASS.
- [ ] `ConcurrencyLimiterTest` 에 `acquireFromVirtualThreads_doesNotPin_andRespectsPermit` 1 케이스 추가.
- [ ] Task 024 의 `ConcurrencyLimitProperties` / `ConcurrencyLimiter` 시그니처 무변경 + 기존 throttle 변환 경로 동작 유지.
- [ ] `MetricSnapshotScheduler`, 3종 `*Publisher`, 3종 `*Producer`, `PrometheusClient`, `PrometheusQueryService` 의 public 시그니처 무변경.
- [ ] `./gradlew :argus:test` 통과.
- [ ] DTO / PromQL / Kafka 메시지 포맷 / 스케줄 주기(60_000ms) 변경 0 확인.
- [ ] 운영 측정 절차(5.5) 가 plan 문서에 남아 있어, 적용 전후 한 주기 처리 시간을 비교 가능.
