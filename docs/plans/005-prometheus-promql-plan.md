● Task 005 구현 계획서: Prometheus PromQL 작성                                                                                                                                            
                                                      
  1. 설계 방식 및 이유                                                                                                                                                                    
                                                                                                                                                                                          
  기본 방향: 메트릭 타입과 PromQL 표현식을 1:1로 묶은 enum 기반 카탈로그를 도입하고, 기존 PrometheusClient/PrometheusQueryService 흐름에 얇게 추가한다.                                                                                                                                                                                                                             
  이유:                                                                                                                                                                                     - 9개 메트릭 타입이 사전에 고정되어 있고 Task 입력으로 명시되어 있어, 타입 안전한 enum이 문자열 상수보다 적합하다.
  - PromQL은 Micrometer 기본 metric name에 묶인 정적 표현식이므로 별도 템플릿 엔진/외부 설정으로 빼지 않는다 (과도한 설계 회피).
  - 기존 PrometheusClient.query(String)이 이미 raw PromQL을 받는 구조이므로, 그 위에 "메트릭 타입 → PromQL 매핑" 레이어 한 겹만 추가한다.

  2. 구성 요소

  ┌───────────┬──────────────────────────────────────────────────────────────┬───────────────────────────────────────────────────────────────────────────────────────────────────────┐
  │   구분    │                             경로                             │                                                 역할                                                  │
  ├───────────┼──────────────────────────────────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────┤
  │ 신규 enum │ argus/.../service/metric/MetricType.java                     │ 9개 메트릭 타입 + 각 타입에 대응하는 PromQL 표현식 보유                                               │
  ├───────────┼──────────────────────────────────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────┤
  │ 기존 수정 │ argus/.../service/PrometheusQueryService.java                │ queryByMetric(MetricType) 메서드 추가. MetricType.getPromql()로 PromQL 꺼내 client 호출 후            │
  │           │                                                              │ PrometheusResponse 그대로 반환                                                                        │
  ├───────────┼──────────────────────────────────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────┤
  │ 기존      │ client/PrometheusClient                                      │ 변경 없음                                                                                             │
  │ 재사용    │                                                              │                                                                                                       │
  ├───────────┼──────────────────────────────────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────┤
  │ 기존      │ dto/PrometheusResponse                                       │ 변경 없음                                                                                             │
  │ 재사용    │                                                              │                                                                                                       │
  ├───────────┼──────────────────────────────────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────┤
  │ 기존      │ exception/PrometheusQueryException                           │ 변경 없음                                                                                             │
  │ 재사용    │                                                              │                                                                                                       │
  ├───────────┼──────────────────────────────────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────┤
  │ 신규      │ service/metric/MetricTypeTest.java                           │ 각 enum이 비어있지 않은 PromQL을 반환하는지 검증                                                      │
  │ 테스트    │                                                              │                                                                                                       │
  ├───────────┼──────────────────────────────────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────┤
  │ 신규      │ service/PrometheusQueryServiceTest.java (기존 파일에 케이스  │ queryByMetric 단위 테스트 (Mockito)                                                                   │
  │ 테스트    │ 추가)                                                        │                                                                                                       │
  └───────────┴──────────────────────────────────────────────────────────────┴───────────────────────────────────────────────────────────────────────────────────────────────────────┘

  MetricType 매핑 (Micrometer 기본 metric name 기준):
  - CPU_USAGE → avg by (application, instance) (avg_over_time(process_cpu_usage[1m]))
  - HEAP_USAGE → sum by (application, instance) (jvm_memory_used_bytes{area="heap"})/sum by (application, instance) (jvm_memory_max_bytes{area="heap"}) * 100
  - GC_AVG_DURATION → rate(jvm_gc_pause_seconds_sum[1m]) / clamp_min(rate(jvm_gc_pause_seconds_count[1m]), 1)
  - GC_COUNT → increase(jvm_gc_pause_seconds_count[1m])
  - ACTIVE_THREADS → avg_over_time(jvm_threads_live_threads[1m])
  - HTTP_P99_RESPONSE_TIME → histogram_quantile(0.99,sum by (application, instance, uri, le)(rate(http_server_requests_seconds_bucket[1m])))
  - HTTP_RPS → sum by (uri) (rate(http_server_requests_seconds_count[1m]))
  - HIKARI_ACTIVE_CONNECTIONS → avg_over_time(hikaricp_connections_active[1m])
  - HIKARI_PENDING_CONNECTIONS → avg_over_time(hikaricp_connections_pending[1m])

  3. 데이터 흐름

  Caller
    → PrometheusQueryService.queryByMetric(MetricType type)
         → type.getPromql()                       // enum 내부에서 PromQL 문자열 획득
         → PrometheusClient.query(promql)         // WebClient GET /api/v1/query
              → Prometheus (localhost:9090)
         ← PrometheusResponse                     // status / data.resultType / data.result[]
    ← PrometheusResponse (status="success" 검증 후 그대로 반환)

  - queryByMetric은 PrometheusResponse 원본을 반환한다 (Task 출력 스펙이 그 구조 그대로). 결과 가공/스칼라 환산은 out of scope.
  - 기존 queryScalar(String)은 그대로 유지 (다른 호출자 호환).

  4. 예외 처리 전략

  - 네트워크/HTTP 오류: PrometheusClient가 이미 PrometheusQueryException으로 변환 → 그대로 전파.
  - status != "success": PrometheusQueryService.queryByMetric에서 검증 후 PrometheusQueryException("non-success status: ...") throw.
  - null 메트릭 타입: enum 파라미터이므로 컴파일 단계에서 차단. 명시적 null 체크는 추가하지 않음 (CLAUDE.md "발생 불가 시나리오 검증 금지" 준수).
  - 빈 result 배열: 정상 응답 (스크랩 미수신 가능)이므로 예외 아님 — 호출자가 판단하도록 그대로 반환.

  5. 검증 방법

  단위 테스트 (Mockito):
  1. MetricTypeTest: 9개 enum 각각 getPromql()이 non-null/non-blank 문자열 반환.
  2. PrometheusQueryServiceTest 추가 케이스:
    - queryByMetric이 enum의 PromQL을 그대로 client에 위임하는지 (Mockito verify로 인자 매칭).
    - status="success" → 동일 객체 반환.
    - status="error" → PrometheusQueryException.
    - client 예외 전파.

  수동 검증 (Acceptance Criteria):
  - 로컬 localhost:9090 Prometheus 기동 + Argus actuator 메트릭 노출 상태에서, 9개 메트릭 타입 모두에 대해 PrometheusResponse.status == "success" 확인.
  - 별도 통합 테스트는 환경 의존도가 높아 본 Task에서는 추가하지 않음 (수동 확인으로 대체).

  6. 트레이드오프

  ┌────────────────────────────────────────────┬────────────────────────────────┬────────────────────────────────────────────────────────────────────────────────────────────────────┐
  │                    선택                    │              대안              │                                             채택 이유                                              │
  ├────────────────────────────────────────────┼────────────────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────────────┤
  │ enum 내부에 PromQL 하드코딩                │ properties/yaml 외부화         │ Task 범위 내 9개로 고정, 운영 중 변경 빈도 낮음. 외부화는 과한 설계.                               │
  ├────────────────────────────────────────────┼────────────────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────────────┤
  │ queryByMetric은 PrometheusResponse 원본    │ DTO로 한 번 더 가공            │ Task 출력 스펙이 PrometheusResponse 구조 그대로이며, "결과 가공"은 out of scope.                   │
  │ 반환                                       │                                │                                                                                                    │
  ├────────────────────────────────────────────┼────────────────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────────────┤
  │ avg_over_time(...[1m]) 기반 instant query  │ query_range 사용               │ 출력 포맷이 단일 value 배열 형태 (instant). range query는 matrix 반환되어 스펙 불일치.             │
  ├────────────────────────────────────────────┼────────────────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────────────┤
  │ HEAP_USAGE에 area="heap" 라벨 필터         │ jvm_memory_usage_after_gc 사용 │ Micrometer 기본 노출 metric은 jvm_memory_used_bytes. GC 이후 metric은 일부 GC에서만 제공되어       │
  │                                            │                                │ 호환성 낮음.                                                                                       │
  ├────────────────────────────────────────────┼────────────────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────────────┤
  │ GC_AVG_DURATION에 rate / rate 수식         │ jvm_gc_pause_seconds_sum만     │ Prometheus에서 평균은 sum/count 비율이 표준. counter 단독은 의미 불명확.                           │
  │                                            │ 노출                           │                                                                                                    │
  └────────────────────────────────────────────┴────────────────────────────────┴────────────────────────────────────────────────────────────────────────────────────────────────────┘

  작업 범위 요약

  - 변경: PrometheusQueryService (메서드 1개 추가) + 신규 MetricType enum + 테스트 2개 파일.
  - 변경 없음: Client / DTO / Exception / Config.
  - out of scope 준수: scrape 설정, metric name 커스터마이징, 결과 분석, 알림 — 모두 손대지 않음.
  - 모든 PromQL은 application, instance 기준으로 그룹핑한다.(단, 엔드포인트 메트릭은 uri 포함)