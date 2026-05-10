# Task 008 — JvmMetricSnapshotAssembler Refactoring 계획

## Context

`JvmMetricSnapshotAssembler.assemble()`는 4개 sub-DTO(Cpu/Memory/Gc/Thread)를 조립해 `JvmMetricSnapshotDto`를 만든다. 현재 ~200 lines로, 다음 구조가 그대로 3~4번 반복된다.

```
try {
  MappingResult r = map(MetricType.XXX);
  if (r instanceof Success s)      ... XxxDto.success(...)
  else if (r instanceof Empty)     ... XxxDto.empty()
  else                              ... XxxDto.parseFailed(((ParseFailed)r).reason())
} catch (PrometheusQueryException e) { ... XxxDto.queryFailed(e.getMessage()) }
catch (Exception e)                  { log.error(...); ... XxxDto.parseFailed(e.getMessage()) }
```

또한 4개 sub-DTO는 같은 형태의 정적 팩토리(`success/empty/queryFailed/parseFailed`)를 이미 공유하지만, 이를 묶는 공통 타입이 없어 Assembler 내부에서 generic 헬퍼화가 안 된다.

본 Task는 (a) 4개 sub-DTO에 공통 부모 타입을 두고, (b) 각 DTO에 `from(MappingResult)` 정적 팩토리를 추가해 분기 boilerplate를 DTO로 옮기고, (c) Assembler에 1개 generic resolve 헬퍼를 둬 try/catch wrapping을 한 번만 정의한다. 결과적으로 Assembler 본체는 메트릭당 1~2 lines로 축소되고, 후속 Task(스케줄러/트리거 등)에서 같은 패턴으로 메트릭을 추가하기 쉬워진다.

기능 동작(상태 흐름·예외 흐름·application/instance 누적·SnapshotStatus 산출)은 100% 보존한다.

## 설계 방식 및 이유

| 결정 | 선택 | 이유 |
| --- | --- | --- |
| 공통 부모 타입 | `sealed interface JvmMetricResult permits Cpu/Memory/Gc/Thread Dto` | record와 호환(record는 abstract class 상속 불가). 프로젝트 내 기존 `MappingResult` sealed 패턴과 일치. 외부 확장 차단으로 SnapshotStatus 집계 안전성 ↑ |
| 부모에 둘 멤버 | `MetricStatus status()`, `Instant measuredAt()`, `String missingReason()` 세 접근자만 | 이미 4개 record가 동일 component 이름으로 보유. 메트릭 값 필드는 DTO마다 타입/이름 달라 부모 멤버화 불가 |
| 팩토리 패턴 적용 위치 | 각 sub-DTO에 `from(MappingResult)` 정적 팩토리 추가 | "팩토리 메서드 패턴" 요구사항 충족. Mapping → DTO 변환 책임을 DTO 자신이 가짐. Assembler에서 `instanceof` 분기 제거 |
| GC 팩토리 시그니처 | `GcMetricDto.from(MappingResult duration, MappingResult count)` 별도 정의 | GC만 2개 메트릭 조합이라 단일 인자 시그니처 불가. 부분 성공/PARTIAL 판정도 DTO로 캡슐화 |
| Assembler 헬퍼 | `private <T extends JvmMetricResult> T resolve(MetricType, Function<MappingResult,T>, Function<String,T>, Function<String,T>)` 1개 | try/catch wrapping(2종 예외 + log)을 한 곳에 둠. 본 Task 범위를 벗어나는 추상화는 회피 |
| application/instance 누적 | private inner class `LabelAccumulator { void accept(MetricPointDto); String application(); String instance(); }` | 현재 `String[] appAccum/instAccum` 1-원소 배열 트릭은 가독성 나쁨. 누적 책임만 추출 |
| 누적 시점 | `resolve` 헬퍼가 Success인 MappingResult.Success.point()를 LabelAccumulator에 전달 (선택적 BiConsumer) | 누적이 from(MappingResult) 내부에 있으면 DTO가 mutable label state를 알게 됨 — 책임 침범. resolve 단계에서만 누적 |
| `intValueExact()` 변환 | `ThreadMetricDto.from`이 내부에서 `ArithmeticException` catch → `parseFailed` | 변환 실패도 Thread DTO 책임. Assembler는 깨끗 |

## 구성 요소

### 신규 파일

- `argus/src/main/java/com/example/argus/dto/metric/JvmMetricResult.java`
  - `public sealed interface JvmMetricResult permits CpuUsageDto, MemoryUsageDto, GcMetricDto, ThreadMetricDto`
  - 멤버: `MetricStatus status();`, `Instant measuredAt();`, `String missingReason();`
- `argus/src/test/java/com/example/argus/dto/metric/CpuUsageDtoTest.java` (신규)
  - `from(Success/Empty/ParseFailed)` 각 분기 검증
- `argus/src/test/java/com/example/argus/dto/metric/MemoryUsageDtoTest.java` (신규) — 동일 패턴
- `argus/src/test/java/com/example/argus/dto/metric/ThreadMetricDtoTest.java` (신규) — `from(Success)`, `intValueExact` 실패 시 parseFailed, 다른 분기
- `argus/src/test/java/com/example/argus/dto/metric/GcMetricDtoTest.java` (신규) — `from(Success,Success)` / `from(Success,Empty)`/`(Empty,Success)` 부분성공 / 양측 실패 (QUERY_FAILED ≥ PARSE_FAILED ≥ EMPTY 우선순위)

### 수정 파일

- `argus/src/main/java/com/example/argus/dto/metric/CpuUsageDto.java`
  - `implements JvmMetricResult` 추가
  - `static CpuUsageDto from(MappingResult)` 팩토리 추가 (Success → success, Empty → empty, ParseFailed → parseFailed)
  - 기존 `success/empty/queryFailed/parseFailed` 그대로 유지
- `argus/src/main/java/com/example/argus/dto/metric/MemoryUsageDto.java` — 동일 패턴
- `argus/src/main/java/com/example/argus/dto/metric/ThreadMetricDto.java`
  - `implements JvmMetricResult` 추가
  - `static ThreadMetricDto from(MappingResult)` 팩토리 — Success 내부에서 `value.intValueExact()` 시도, `ArithmeticException` 발생 시 `parseFailed("cannot convert to int: ...")`
- `argus/src/main/java/com/example/argus/dto/metric/GcMetricDto.java`
  - `implements JvmMetricResult` 추가
  - `static GcMetricDto from(MappingResult durationResult, MappingResult countResult)` 추가
    - 현재 `buildGcDto` 후반부의 success/partial/queryFailed/parseFailed/emptyResult 결정 로직 그대로 이전
    - reason prefix(`"GC_AVG_DURATION: "`, `"GC_COUNT: "`) 도 이쪽으로 이전
  - 기존 `success/partial/emptyResult/queryFailed/parseFailed` 유지
- `argus/src/main/java/com/example/argus/service/metric/snapshot/JvmMetricSnapshotAssembler.java`
  - 메서드 추가: `private <T extends JvmMetricResult> T resolve(MetricType type, Function<MappingResult,T> ok, Function<String,T> queryFailed, Function<String,T> parseFailed)`
    - 내부: `try { return ok.apply(map(type)); } catch (PrometheusQueryException e) { return queryFailed.apply(e.getMessage()); } catch (Exception e) { log.error(...); return parseFailed.apply(e.getMessage()); }`
  - GC 전용 헬퍼: `private GcMetricDto resolveGc(LabelAccumulator labels)` — duration/count 각각 `mapSafely(MetricType)` 후 `GcMetricDto.from(d, c)` 반환. 단순 `MappingResult` 2회 수집은 inline 가능 (예외별로 wrap한 `MappingResult` 합성 지점 검토; 최초안에서는 try/catch 없이 `MappingResult.ParseFailed`로 강제 변환하는 helper 사용)
  - LabelAccumulator: 메서드 호출 후 `result instanceof Success`일 때 `Success.point()`에서 application/instance 누적
  - `assemble()` 본체:
    ```
    LabelAccumulator labels = new LabelAccumulator();
    Instant collectedAt = Instant.now();
    CpuUsageDto cpu = resolve(CPU_USAGE, withLabels(labels, CpuUsageDto::from), CpuUsageDto::queryFailed, CpuUsageDto::parseFailed);
    MemoryUsageDto memory = resolve(HEAP_USAGE, withLabels(labels, MemoryUsageDto::from), MemoryUsageDto::queryFailed, MemoryUsageDto::parseFailed);
    GcMetricDto gc = resolveGc(labels);
    ThreadMetricDto thread = resolve(ACTIVE_THREADS, withLabels(labels, ThreadMetricDto::from), ThreadMetricDto::queryFailed, ThreadMetricDto::parseFailed);
    SnapshotStatus snapshotStatus = SnapshotStatus.from(List.of(cpu.status(), memory.status(), gc.status(), thread.status()));
    return new JvmMetricSnapshotDto(labels.application(), labels.instance(), collectedAt, cpu, memory, gc, thread, snapshotStatus);
    ```
  - `withLabels`는 MappingResult를 받으면 Success일 때 labels에 점을 누적한 후 위임 mapper 호출하는 wrapper (private helper)
- `argus/src/test/java/com/example/argus/service/metric/snapshot/JvmMetricSnapshotAssemblerTest.java`
  - 기존 시나리오(allSuccess/cpuQueryFails/allFail/gcPartial/memoryEmpty 등) 그대로 유지 — 동작 보존 검증의 핵심
  - 시그니처 변경 없으므로 stub 코드 변경 최소 (assertions 그대로)

### 변경 없음

- `PrometheusResponse` (제약)
- `MetricType`, PromQL 정의 (제약)
- `JvmMetricSnapshotDto`, `MetricStatus`, `SnapshotStatus`
- `MetricPointMapper`, `MappingResult`
- `MetricEventProducer`(or `JvmMetricSnapshotProducer`), `JvmMetricSnapshotPublisher` — Kafka 발행 경로
- `application.yml`, Kafka serializer 설정

## 데이터 흐름 (리팩토링 후)

1. `JvmMetricSnapshotPublisher.publish()` (변경 없음)
2. `JvmMetricSnapshotAssembler.assemble()`
   1. `LabelAccumulator labels = new LabelAccumulator();` 생성
   2. `collectedAt = Instant.now();`
   3. `CpuUsageDto cpu = resolve(CPU_USAGE, withLabels(labels, CpuUsageDto::from), CpuUsageDto::queryFailed, CpuUsageDto::parseFailed);`
      - `resolve` 내부: `map(type)` → `MappingResult` 획득 → `withLabels`가 Success면 `labels.accept(point)` → `CpuUsageDto.from(result)` 반환
      - `PrometheusQueryException` → `CpuUsageDto.queryFailed(msg)`
      - 기타 Exception → log + `CpuUsageDto.parseFailed(msg)`
   4. `MemoryUsageDto memory = resolve(HEAP_USAGE, ...)` 동일
   5. `GcMetricDto gc = resolveGc(labels)`
      - duration/count 각각 `MappingResult` 안전 수집 → label 누적 → `GcMetricDto.from(durationResult, countResult)`로 위임
   6. `ThreadMetricDto thread = resolve(ACTIVE_THREADS, ...)`
      - `ThreadMetricDto.from(MappingResult)` 내부에서 `intValueExact` 처리
   7. `SnapshotStatus snapshotStatus = SnapshotStatus.from(List.of(cpu.status(), memory.status(), gc.status(), thread.status()));`
   8. `return new JvmMetricSnapshotDto(labels.application(), labels.instance(), collectedAt, cpu, memory, gc, thread, snapshotStatus);`
3. 이후 발행 경로(Producer → Kafka → JSON 직렬화)는 동일

## 예외 처리 전략

| 발생 위치 | 처리 |
| --- | --- |
| `PrometheusQueryService.queryByMetric` 내부 `PrometheusQueryException` | `resolve` 헬퍼가 catch → 해당 sub-DTO `queryFailed(msg)` 팩토리 호출 (status = `QUERY_FAILED`) |
| `MetricPointMapper.toPoint` 결과 `MappingResult.ParseFailed` | DTO `from` 팩토리가 처리 → `parseFailed(reason)` (status = `PARSE_FAILED`) |
| `MappingResult.Empty` | DTO `from` 팩토리가 처리 → `empty()` (status = `EMPTY_RESULT`) |
| `intValueExact()` `ArithmeticException` (Thread 한정) | `ThreadMetricDto.from` 내부에서 catch → `parseFailed("cannot convert to int: " + msg)` |
| GC duration/count 부분 성공 | `GcMetricDto.from(d, c)` 내부에서 status별 우선순위(`QUERY_FAILED` > `PARSE_FAILED` > `EMPTY_RESULT`)와 `partial` 판정 |
| GC duration/count 양측 PrometheusQueryException | `resolveGc` 단계에서 각 호출을 try/catch → `MappingResult.ParseFailed`로 변환할 수 없으므로 GC 전용 처리: 예외 메시지를 reason으로 보관해 `GcMetricDto.queryFailed(combined)` 직접 호출 (이 케이스만 GC 전용 분기) |
| 그 외 일반 `Exception` | `resolve` 헬퍼 → `log.error(...)` + `parseFailed(msg)` |
| `JvmMetricSnapshotAssembler` 자체에서 throw | 없음. 모든 실패는 status 필드로 흡수. (현재와 동일) |

CLAUDE.md "외부 API 오류는 별도 Exception" 규칙은 이미 `PrometheusQueryException`로 충족. 본 Task는 흐름 보존만.

## 검증 방법

### 단위 테스트
```bash
cd argus
./gradlew -q -Dtest=JvmMetricSnapshotAssemblerTest test
./gradlew -q -Dtest=CpuUsageDtoTest,MemoryUsageDtoTest,ThreadMetricDtoTest,GcMetricDtoTest test
./gradlew -q -Dtest=JvmMetricSnapshotDtoTest,SnapshotStatusTest test
./gradlew -q test
```

검증 포인트:
- `JvmMetricSnapshotAssemblerTest` 의 기존 시나리오 전부 그대로 통과 (동작 보존)
- 각 sub-DTO `from(MappingResult)` 팩토리는 Success/Empty/ParseFailed 3분기 모두 status·measuredAt·missingReason 동일성 검증
- `GcMetricDto.from(d, c)` 는 success / partial(duration only) / partial(count only) / queryFailed(combined) / parseFailed(combined) / emptyResult(combined) 6 케이스 검증
- `ThreadMetricDto.from` 의 `intValueExact` 실패 케이스 검증

### 컴파일·정적 검사
```bash
cd argus && ./gradlew -q compile test-compile
```
- `sealed`/`permits` 문법 (Java 17+) 확인
- 4개 sub-DTO가 모두 `JvmMetricResult` 를 정확히 implement 하는지 확인 (status/measuredAt/missingReason record component 이름 일치)

### 통합 검증 (수동)
```bash
docker compose -f docker/docker-compose.kafka.yml up -d
./gradlew spring-boot:run
docker exec -it kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic jvm.metrics.raw --from-beginning
```
- JSON payload에 application/instance/cpu/memory/gc/thread/status 필드가 리팩토링 전과 동일하게 직렬화되는지 확인 (sealed interface 추가가 Jackson 직렬화에 영향 없는지 — record component만 직렬화되므로 무영향)

## 트레이드오프

| Trade-off | 선택한 쪽 | 포기한 쪽 / 위험 |
| --- | --- | --- |
| `sealed interface` vs 일반 `interface` | sealed | 외부에서 implement 가능성 닫음. 추후 메트릭 종류 추가 시 `permits` 명시 필요(소소한 비용). 대신 SnapshotStatus 집계의 닫힌 우주 보장 |
| `from(MappingResult)` 팩토리를 DTO에 두기 vs Assembler 헬퍼만 두기 | DTO에 두기 | DTO가 `MappingResult` 타입을 의존(역방향 결합). 대신 팩토리 메서드 패턴 요구사항 직접 충족 + Assembler 명료 |
| GC 전용 `from(d, c)` 시그니처 별도 vs 4개 통일 | 별도 | API 비대칭. 대신 GC의 본질(2 metric)을 강제 통일 시 더 어색한 추상화가 생김 |
| `LabelAccumulator` private 클래스 도입 vs `String[]` 트릭 유지 | 도입 | 클래스 1개 증가. 대신 mutable holder 책임 명확 + 테스트 용이 |
| `resolve<T>` 시그니처에 람다 4개 vs `enum`/`record`로 묶기 | 람다 4개 | 호출부가 살짝 길어짐. 대신 추가 추상 타입 없이 끝남 |
| Thread `intValueExact` 처리를 `ThreadMetricDto.from`에 넣기 vs Assembler에 두기 | DTO 내부 | DTO가 변환 실패 책임도 가짐. 대신 Assembler가 일반화된 `resolve`만 호출 가능 |
| GC duration/count 호출 시 `PrometheusQueryException`은 별도 처리 | 일관성 일부 손실 | `MappingResult` 모델이 query 실패를 표현하지 못해서. 대신 동작은 기존과 동일. 향후 `MappingResult`에 `QueryFailed` variant 추가하면 통일 가능 (본 Task 외) |
| 각 sub-DTO 단위 테스트 신규 4개 추가 | 추가 | 파일 수 증가. 대신 from 팩토리는 새 surface area라 직접 검증 필요 |

## 작업 순서 (구현 시)

1. `JvmMetricResult` sealed interface 작성
2. 4개 sub-DTO에 `implements JvmMetricResult` 추가 + `from` 팩토리 추가
3. 4개 sub-DTO 단위 테스트 신규 작성 (`from` 팩토리 분기 검증)
4. `JvmMetricSnapshotAssembler` 리팩토링 (`LabelAccumulator`, `resolve`, `resolveGc`, `withLabels`)
5. `JvmMetricSnapshotAssemblerTest` 동일 stub으로 통과 확인 (수정 없을 가능성 ↑)
6. `gradleww test` 전체 통과 확인 → Kafka 통합 수동 검증
