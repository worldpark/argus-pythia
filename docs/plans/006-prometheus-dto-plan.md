Task 006: Prometheus 결과 DTO 설계 및 Snapshot 구성 — 구현 계획
                                                                                                                                                                                               Context
                                                                                                                                                                                               - Task 005에서 MetricType enum과 PrometheusQueryService.queryByMetric()까지 구축되어 PromQL 실행은 이미 가능하다.                                                                  
     - 본 Task는 그 결과인 PrometheusResponse를 서비스 내부 도메인 DTO로 변환하고, 4개 JVM 메트릭(CPU/Memory/GC/Thread)을 하나의 스냅샷(JvmMetricSnapshotDto) 으로 조립하는 변환 계층을 
     정의한다.
     - 부분 실패(일부 메트릭만 실패)에서도 Snapshot이 생성되어야 하며, 이후 Kafka 전송/LLM 분석 단계가 일관된 입력을 받도록 만드는 것이 목표이다.
     - 본 Task는 DTO + 변환 구조 정의까지만 수행한다. Controller, DB 저장, Kafka Producer 신규 구현, LLM 분석은 모두 후속 Task에서 다룬다.

     ---
     설계 방식 및 이유

     1) 변환 책임 2단 분리

     - Stage 1 (저수준 매핑): PrometheusResponse → MetricPointDto. 라벨 추출, timestamp/value 파싱 등 메트릭 종류와 무관한 순수 변환. 정적 유틸로 구현.
     - Stage 2 (조립): MetricPointDto → 하위 DTO → JvmMetricSnapshotDto. 메트릭별 의미(usagePercent, count 등)를 부여하고 Snapshot으로 합침. Spring @Service로 구현.
     - 이유: 매핑은 순수 함수라 단위 테스트가 독립적이며, Assembler는 PrometheusQueryService에 의존하므로 분리가 자연스럽다. 향후 다른 Snapshot(HTTP 메트릭 등)이 추가되어도 매핑 계층은
     재사용된다.

     2) record + 정적 팩토리 메서드

     - 모든 DTO는 record로 선언(불변·Jackson 호환·기존 MetricEvent와 일관).
     - 각 하위 메트릭 DTO는 success(...), empty(), queryFailed(reason), parseFailed(reason) 정적 팩토리를 제공해 status invariant(SUCCESS면 missingReason=null, 실패면 값 필드=null)를
     응집 강제.

     3) 부분 실패 격리

     - Assembler는 메트릭 1개당 try-catch 블록을 가지며, 실패해도 다른 메트릭 변환을 계속 진행.
     - 각 하위 DTO는 자체 MetricStatus를 보유. 4개 status를 종합해 SnapshotStatus.from(...) 정적 팩토리에서 단일 규칙으로 결정 → 매직 임계치 방지.

     4) 단일 instance 가정

     - Task 11번 Out of Scope에 "멀티 노드 Aggregation" 명시되어 있어 본 Task는 응답의 첫 result만 사용. 다중 result 발견 시 WARN 로그.
     - 향후 멀티 노드 지원 Task에서 List 형태로 확장 가능하게 MetricPointDto(공통 DTO)는 그대로 유지.

     5) GC: 단일 status, 부분 채움 정책

     - GcMetricDto.status 결정 규칙:
       - duration·count 둘 다 SUCCESS → SUCCESS
       - 둘 다 실패 → 실패 사유에 따라 QUERY_FAILED / PARSE_FAILED / EMPTY_RESULT
       - 한쪽만 실패 → SUCCESS + missingReason에 누락 사유 기록(가능한 필드는 채움)
     - Task 사양상 GcMetricDto 구조 변경 없음.

     ---
     구성 요소

     신규 파일

     argus/src/main/java/com/example/argus/
     ├── dto/metric/
     │   ├── MetricPointDto.java              # record (공통)
     │   ├── MetricStatus.java                # enum
     │   ├── SnapshotStatus.java              # enum (+ from(List<MetricStatus>) 정적 팩토리)
     │   ├── CpuUsageDto.java                 # record + 정적 팩토리
     │   ├── MemoryUsageDto.java              # record + 정적 팩토리
     │   ├── GcMetricDto.java                 # record + 정적 팩토리
     │   ├── ThreadMetricDto.java             # record + 정적 팩토리
     │   └── JvmMetricSnapshotDto.java        # record (Snapshot)
     └── service/metric/
         ├── mapper/
         │   └── MetricPointMapper.java       # 정적 유틸 (final + private ctor)
         └── snapshot/
             └── JvmMetricSnapshotAssembler.java  # @Service

     신규 테스트 파일

     argus/src/test/java/com/example/argus/
     ├── dto/metric/
     │   ├── JvmMetricSnapshotDtoTest.java    # Jackson 직렬화 + record equality
     │   └── SnapshotStatusTest.java          # from() 규칙
     └── service/metric/
         ├── mapper/MetricPointMapperTest.java
         └── snapshot/JvmMetricSnapshotAssemblerTest.java  # 부분 실패 시나리오 중심

     컴포넌트 책임

     ┌──────────────────────────────┬───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┬────────────────────────┐     │           컴포넌트           │                                                           책임                                                            │         의존성         │     ├──────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┼────────────────────────┤     │ MetricPointMapper            │ PrometheusResponse + MetricType → Optional<MetricPointDto>. 라벨에서 application/instance 추출, value[0]→Instant,         │ 없음 (정적 유틸)       │     │                              │ value[1]→BigDecimal                                                                                                       │                        │     ├──────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┼────────────────────────┤     │ JvmMetricSnapshotAssembler   │ 4개 MetricType별로 query 실행 → Mapper 호출 → 하위 DTO 생성 → Snapshot 조립 → SnapshotStatus 계산                         │ PrometheusQueryService │     ├──────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┼────────────────────────┤     │ *Dto records                 │ 불변 데이터 컨테이너 + 정적 팩토리                                                                                        │ 없음                   │     ├──────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┼────────────────────────┤     │ MetricStatus /               │ 상태 표현 + 결정 규칙                                                                                                     │ 없음                   │     │ SnapshotStatus               │                                                                                                                           │                        │     └──────────────────────────────┴───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┴────────────────────────┘
     핵심 필드 (Task 사양 준수)

     ┌──────────────────────┬─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────     ┐
     │         DTO          │                                                                            필드
     │
     ├──────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────     ┤
     │ MetricPointDto       │ String application, String instance, Map<String,String> labels, Instant timestamp, BigDecimal value
     │
     ├──────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────     ┤
     │ CpuUsageDto          │ BigDecimal usagePercent, Instant measuredAt, MetricStatus status, String missingReason
     │
     ├──────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────     ┤
     │ MemoryUsageDto       │ BigDecimal heapUsagePercent, Instant measuredAt, MetricStatus status, String missingReason
     │
     ├──────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────     ┤
     │ GcMetricDto          │ BigDecimal avgDurationSeconds, BigDecimal count, Instant measuredAt, MetricStatus status, String missingReason
     │
     ├──────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────     ┤
     │ ThreadMetricDto      │ Integer activeCount, Instant measuredAt, MetricStatus status, String missingReason
     │
     ├──────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────     ┤
     │ JvmMetricSnapshotDto │ String application, String instance, Instant collectedAt, CpuUsageDto cpu, MemoryUsageDto memory, GcMetricDto gc, ThreadMetricDto thread, SnapshotStatus
     │
     │                      │ status
     │
     └──────────────────────┴─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────     ┘

     ---
     데이터 흐름

     1. 호출자(이후 Scheduler 또는 Service) → JvmMetricSnapshotAssembler.assemble() 호출
     2. Assembler가 application, instance, collectedAt = Instant.now() 결정
     3. 메트릭별 처리 (CPU, HEAP, GC_AVG_DURATION + GC_COUNT, ACTIVE_THREADS):
       - PrometheusQueryService.queryByMetric(type) 호출
       - MetricPointMapper.toPoint(response, type) 호출 → Optional<MetricPointDto>
       - 결과를 하위 DTO로 매핑 (정적 팩토리 활용)
     4. 4개 하위 DTO + 각 status 수집
     5. SnapshotStatus.from(List<MetricStatus>) 로 전체 상태 계산
     6. JvmMetricSnapshotDto 조립 후 반환
     7. (후속 Task) Producer가 본 DTO를 Kafka로 발행

     MetricType ↔ DTO 매핑 표

     ┌─────────────────┬─────────────────────────────────────────────────────┬─────────────────────────────────────────────┐
     │   MetricType    │                      변환 결과                      │                    비고                     │
     ├─────────────────┼─────────────────────────────────────────────────────┼─────────────────────────────────────────────┤
     │ CPU_USAGE       │ CpuUsageDto.usagePercent ← value                    │ 단일 인스턴스                               │
     ├─────────────────┼─────────────────────────────────────────────────────┼─────────────────────────────────────────────┤
     │ HEAP_USAGE      │ MemoryUsageDto.heapUsagePercent ← value             │ by 절 없음 → application/instance null 허용 │
     ├─────────────────┼─────────────────────────────────────────────────────┼─────────────────────────────────────────────┤
     │ GC_AVG_DURATION │ GcMetricDto.avgDurationSeconds ← value              │ 다른 쿼리와 결합                            │
     ├─────────────────┼─────────────────────────────────────────────────────┼─────────────────────────────────────────────┤
     │ GC_COUNT        │ GcMetricDto.count ← value                           │ 다른 쿼리와 결합                            │
     ├─────────────────┼─────────────────────────────────────────────────────┼─────────────────────────────────────────────┤
     │ ACTIVE_THREADS  │ ThreadMetricDto.activeCount ← value.intValueExact() │ int 변환                                    │
     └─────────────────┴─────────────────────────────────────────────────────┴─────────────────────────────────────────────┘

     Snapshot 상태 결정 규칙 (SnapshotStatus.from)

     ┌─────────────────────────────┬──────────┐
     │            조건             │   결과   │
     ├─────────────────────────────┼──────────┤
     │ 모든 하위 status == SUCCESS │ COMPLETE │
     ├─────────────────────────────┼──────────┤
     │ 모든 하위 status != SUCCESS │ FAILED   │
     ├─────────────────────────────┼──────────┤
     │ 그 외 (혼재)                │ PARTIAL  │
     └─────────────────────────────┴──────────┘

     ---
     예외 처리 전략

     ┌──────────────────────────────────────┬───────────────────────────────────────────┬────────────────────────────────────────────────────────────────────────────────────────────────┐     │              발생 지점               │                   원인                    │                                              처리                                              │     ├──────────────────────────────────────┼───────────────────────────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────────┤     │ PrometheusQueryService.queryByMetric │ PrometheusQueryException (HTTP 오류,      │ Assembler가 catch → MetricStatus.QUERY_FAILED + 예외 메시지를 missingReason에 담음             │     │                                      │ status != success)                        │                                                                                                │     ├──────────────────────────────────────┼───────────────────────────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────────┤     │ MetricPointMapper.toPoint            │ data == null / result.isEmpty()           │ Optional.empty() 반환 → Assembler가 MetricStatus.EMPTY_RESULT로 매핑                           │     ├──────────────────────────────────────┼───────────────────────────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────────┤     │ value 파싱 (BigDecimal/Instant)      │ NumberFormatException, NaN/Inf, value     │ Mapper에서 catch → Optional.empty() 반환하지 않고 별도 신호 필요 → Mapper는 MappingResult      │     │                                      │ 길이 부족                                 │ (sealed interface)로 SUCCESS/EMPTY/PARSE_FAILED 구분 반환                                      │     ├──────────────────────────────────────┼───────────────────────────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────────┤     │ 다중 result                          │ 응답에 result 2개 이상                    │ WARN 로그 + 첫 번째만 사용 (단일 instance 가정)                                                │     ├──────────────────────────────────────┼───────────────────────────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────────┤     │ Assembler 내부 unchecked exception   │ NPE 등 예기치 못한 오류                   │ 광역 catch → 해당 메트릭 MetricStatus.PARSE_FAILED 처리, 다른 메트릭 진행 보장                 │     └──────────────────────────────────────┴───────────────────────────────────────────┴────────────────────────────────────────────────────────────────────────────────────────────────┘
     ▎ MappingResult 보강: 단순 Optional은 EMPTY와 PARSE_FAILED를 구분 못 함. sealed interface MappingResult { record Success(MetricPointDto point), record Empty(), record
     ▎ ParseFailed(String reason) } 로 구분해 Assembler가 정확한 MetricStatus를 매핑하게 한다. 이는 과도한 추상화가 아니라 부분 실패 의미 보존을 위한 최소 구조.

     신규 예외 클래스는 만들지 않는다. 기존 PrometheusQueryException 재사용.

     ---
     검증 방법

     단위 테스트 (필수)

     ┌────────────────────────────────┬───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────     ┐
     │             테스트             │                                                                     검증 내용
     │
     ├────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────     ┤
     │ MetricPointMapperTest          │ 정상 응답 → Success / 빈 result → Empty / "NaN"·"Inf"·malformed value → ParseFailed / by 절 없는 응답에서 application·instance가 null로 들어감 /
     │
     │                                │ labels Map 보존
     │
     ├────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────     ┤
     │ SnapshotStatusTest             │ from() 규칙 4개 케이스 (전부 성공, 전부 실패, 일부 실패, GC만 실패 등)
     │
     ├────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────     ┤
     │ JvmMetricSnapshotAssemblerTest │ PrometheusQueryService Mock. 시나리오: ①모두 성공 → COMPLETE, ②CPU만 실패(PrometheusQueryException) → PARTIAL + cpu.status=QUERY_FAILED, ③모두
     │
     │                                │ 실패 → FAILED, ④GC duration만 성공 + GC count 실패 → gc.status=SUCCESS + missingReason 기록, ⑤Memory 빈 결과 → EMPTY_RESULT
     │
     ├────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────     ┤
     │ JvmMetricSnapshotDtoTest       │ JacksonJsonSerializer로 직렬화 시 모든 필드 포함 + MetricStatus/SnapshotStatus enum이 문자열로 직렬화 + null 필드 처리 (Kafka 호환 검증)
     │
     └────────────────────────────────┴───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────     ┘

     End-to-End 수동 확인 (선택)

     # Spring Boot 앱 + Prometheus 기동 상태에서
     ./gradlew test --tests "*JvmMetricSnapshotAssembler*"
     ./gradlew test --tests "*MetricPointMapper*"

     본 Task에서는 Controller가 없으므로 실제 Snapshot 생성은 후속 Task의 Scheduler/Service 통합 후 검증.

     기존 테스트 회귀 확인

     - PrometheusClientTest, PrometheusQueryServiceTest, MetricEventTest, MetricEventProducerTest 모두 PASS 유지 (수정 없음).

     ---
     트레이드오프

     ┌─────────────────────┬─────────────────────────────────────────┬────────────────────────────┬─────────────────────────────────────────────────────────────────────────────┐
     │        결정         │                  채택                   │            대안            │                                  채택 이유                                  │
     ├─────────────────────┼─────────────────────────────────────────┼────────────────────────────┼─────────────────────────────────────────────────────────────────────────────┤
     │ 변환 계층 분리      │ Mapper (정적 유틸) + Assembler (서비스) │ Assembler 단일 클래스      │ 매핑은 순수 함수라 독립 테스트 가능. 책임 1개씩 응집                        │
     ├─────────────────────┼─────────────────────────────────────────┼────────────────────────────┼─────────────────────────────────────────────────────────────────────────────┤
     │ GC 모델             │ 단일 GcMetricDto + 부분 채움            │ sub-status 추가 / DTO 분리 │ Task 사양 그대로 유지. missingReason으로 부분 실패 표현 가능, 호출부 단순   │
     ├─────────────────────┼─────────────────────────────────────────┼────────────────────────────┼─────────────────────────────────────────────────────────────────────────────┤
     │ Multi-instance      │ 첫 result만 사용                        │ List 보유                  │ Out of Scope에 멀티 노드 명시. YAGNI; 단일 노드 환경 가정에 부합            │
     ├─────────────────────┼─────────────────────────────────────────┼────────────────────────────┼─────────────────────────────────────────────────────────────────────────────┤
     │ Mapping 결과 표현   │ sealed MappingResult                    │ Optional<MetricPointDto>   │ EMPTY와 PARSE_FAILED 의미 구분 필요. sealed interface 비용 < 의미 손실 비용 │
     ├─────────────────────┼─────────────────────────────────────────┼────────────────────────────┼─────────────────────────────────────────────────────────────────────────────┤
     │ DTO 팩토리          │ 정적 팩토리 메서드                      │ canonical ctor 직접 사용   │ invariant 응집, 호출부 가독성 ↑. 반복적 null 채움 회피                      │
     ├─────────────────────┼─────────────────────────────────────────┼────────────────────────────┼─────────────────────────────────────────────────────────────────────────────┤
     │ Mapper 빈 등록      │ 정적 유틸(@Component 미사용)            │ Spring 빈                  │ 의존성 없음, mocking 불필요. 빈 등록 비용 회피                              │
     ├─────────────────────┼─────────────────────────────────────────┼────────────────────────────┼─────────────────────────────────────────────────────────────────────────────┤
     │ value 파싱 타입     │ BigDecimal                              │ double                     │ 정밀도 보존. Task 사양 명시                                                 │
     ├─────────────────────┼─────────────────────────────────────────┼────────────────────────────┼─────────────────────────────────────────────────────────────────────────────┤
     │ Assembler 호출 형태 │ 동기 직렬 (4 + GC 추가 1 = 5번 query)   │ CompletableFuture 병렬     │ 본 Task 범위는 구조 정의. 병렬화는 후속 최적화 Task                         │
     └─────────────────────┴─────────────────────────────────────────┴────────────────────────────┴─────────────────────────────────────────────────────────────────────────────┘

     ---
     비-목표 (이번 Task에서 하지 않는 것)

     - Kafka Producer 신규 구현 (기존 MetricEventProducer는 MetricEvent용. Snapshot용 Producer는 후속 Task)
     - Scheduler / 호출자 구현
     - DB 저장
     - Controller / REST API
     - Multi-instance Aggregation
     - range query 처리
     - 새 PromQL 추가, 기존 PromQL 수정
     - PrometheusResponse 수정

     ---
     핵심 파일 (참고/수정 대상)

     읽기만 (수정 금지)

     - argus/src/main/java/com/example/argus/dto/PrometheusResponse.java
     - argus/src/main/java/com/example/argus/service/metric/MetricType.java
     - argus/src/main/java/com/example/argus/service/PrometheusQueryService.java
     - argus/src/main/java/com/example/argus/exception/PrometheusQueryException.java

     신규 (위 "구성 요소" 섹션 참조)

     - dto/metric/ 하위 8개 파일
     - service/metric/mapper/MetricPointMapper.java
     - service/metric/snapshot/JvmMetricSnapshotAssembler.java
     - 4개 테스트 파일

● 계획 파일이 C:\Users\pangko\.claude\plans\task-binary-quail.md에 저장되었습니다.
                                                                                          
  원 요청이 "실제 코드는 작성하지 않는다"였으므로 구현은 진행하지 않습니다. 계획 핵심 요약:
                                                                                                                                                                                            - 2단 분리: MetricPointMapper(정적 유틸) + JvmMetricSnapshotAssembler(@Service)
  - DTO: 모두 record + 정적 팩토리(success/empty/queryFailed/parseFailed)                                                                                                                   - GC: 단일 status, 한쪽만 실패 시 SUCCESS + missingReason로 부분 채움                                                                                                                 
  - Multi-instance: 첫 result만 사용 (Out of Scope 준수)                                                                                                                                  
  - 부분 실패 의미 보존: Mapper는 sealed MappingResult로 EMPTY/PARSE_FAILED 구분
  - 검증: Mapper / SnapshotStatus.from / Assembler(부분 실패 5개 시나리오) / Snapshot 직렬화 4개 단위 테스트
  - GC duration/count 중 하나만 성공한 경우 SUCCESS가 아니라 PARTIAL로 표현하라.
  - MetricStatus에 PARTIAL을 추가하거나, 동일 의미를 명확히 표현할 수 있는 구조로 수정하라.
  - 다중 result는 이번 Task에서 첫 result만 사용하되 WARN 로그를 남긴다.