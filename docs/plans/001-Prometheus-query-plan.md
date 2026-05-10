  1. 패키지 구조

  com.example.argus
  ├── ArgusApplication.java                (기존)
  ├── client
  │   └── PrometheusClient.java            ★ Task File
  ├── service
  │   └── PrometheusQueryService.java      ★ Task File
  ├── dto
  │   └── PrometheusResponse.java          ★ Task File
  ├── config
  │   └── WebClientConfig.java             (보조: WebClient 빈 등록)
  └── exception
      └── PrometheusQueryException.java    (보조: 외부 API 오류 변환 규칙)

  CLAUDE.md의 표준 패키지(controller / service / repository / domain / dto / client / messaging)를 그대로 준수합니다. 본   Task는 컨트롤러/리포지토리/엔티티가 필요 없으므로 추가하지 않습니다 ("하나의 Task는 하나의 기능만").

  ---
  2. 클래스 설계 (역할)

  2.1 PrometheusClient (client)

  - 책임: Prometheus HTTP API(GET /api/v1/query) 호출만 담당. JSON ↔ PrometheusResponse 매핑까지가 경계.
  - 외부 API 오류·5xx·역직렬화 실패 → PrometheusQueryException으로 변환해 던진다 (CLAUDE.md "외부 API 오류는 별도
  Exception으로 변환").
  - 비즈니스 판단(빈 결과 처리, 0.0 기본값 등)은 하지 않음.
  - 어노테이션: @Component, @RequiredArgsConstructor, @Slf4j.

  2.2 PrometheusQueryService (service)

  - 책임: PromQL 결과를 도메인 관점에서 가공.
    - 단일 스칼라 값(double) 추출
    - data.result가 비어 있으면 0.0 반환
    - status != "success"면 PrometheusQueryException
  - 어노테이션: @Service, @RequiredArgsConstructor.
  - 의존: PrometheusClient만. Repository/Controller 의존 없음.

  2.3 PrometheusResponse (dto)

  - 책임: Prometheus /api/v1/query 응답의 직렬화 모델.
  - 중첩 구조 (Jackson 매핑):
    - PrometheusResponse { String status; Data data; }
    - Data { String resultType; List<Result> result; }
    - Result { Map<String,String> metric; List<Object> value; } — value = [epochSeconds(Number), value(String)]
  형태이므로 List<Object>로 받고 서비스에서 인덱스로 추출.
  - Lombok @Getter + 기본 생성자(Jackson용). Entity 노출 금지 규칙과 무관 (DTO 자체).

  2.4 WebClientConfig (config, 보조)

  - 책임: prometheus.base-url 프로퍼티를 기반으로 WebClient 빈을 단일 생성.
  - 빈 이름: prometheusWebClient (장래 다른 외부 클라이언트와 충돌 방지).

  2.5 PrometheusQueryException (exception, 보조)

  - extends RuntimeException — CLAUDE.md의 "CustomException 상속" 규칙에 따라야 하나 공통 CustomException 부모가 본
  모듈에 아직 없음. 본 Task는 단일 기능 구현 범위라 공통 기반 클래스 도입은 별도 Task로 미룬다는 사유 주석을 클래스
  상단에 명시.
  - 생성자 2종: (String message), (String message, Throwable cause).

  ---
  3. 메서드 목록

  PrometheusClient

  ┌─────────┬────────────────────────────────────┬─────────────────────────────────────────────────────────────────┐
  │ 메서드  │              시그니처              │                              설명                               │
  ├─────────┼────────────────────────────────────┼─────────────────────────────────────────────────────────────────┤
  │ 생성자  │ PrometheusClient(WebClient         │ DI                                                              │
  │         │ prometheusWebClient)               │                                                                 │
  ├─────────┼────────────────────────────────────┼─────────────────────────────────────────────────────────────────┤
  │ 쿼리    │ PrometheusResponse query(String    │ /api/v1/query?query={promql} GET. 동기 반환(block())으로 서비스 │
  │ 호출    │ promql)                            │  사용성 단순화. 4xx/5xx → PrometheusQueryException.             │
  └─────────┴────────────────────────────────────┴─────────────────────────────────────────────────────────────────┘

  PrometheusQueryService

  ┌───────────┬──────────────────────────────────────────────┬─────────────────────────────────────────────────────┐
  │  메서드   │                   시그니처                   │                        설명                         │
  ├───────────┼──────────────────────────────────────────────┼─────────────────────────────────────────────────────┤
  │ 생성자    │ PrometheusQueryService(PrometheusClient      │ DI                                                  │
  │           │ client)                                      │                                                     │
  ├───────────┼──────────────────────────────────────────────┼─────────────────────────────────────────────────────┤
  │ 스칼라    │ double queryScalar(String promql)            │ Acceptance Criteria 충족 메서드. 빈 결과 0.0,       │
  │ 조회      │                                              │ 비-success 응답 시 예외.                            │
  └───────────┴──────────────────────────────────────────────┴─────────────────────────────────────────────────────┘

  PrometheusResponse (+ 중첩)

  - getStatus() / getData()
  - Data#getResultType() / getResult()
  - Result#getMetric() / getValue()

  WebClientConfig

  - WebClient prometheusWebClient(@Value("${prometheus.base-url}") String baseUrl)

  PrometheusQueryException

  - PrometheusQueryException(String message)
  - PrometheusQueryException(String message, Throwable cause)

  ---
  4. 데이터 흐름

  호출자 (테스트 또는 향후 Controller)
     │
     ▼
  PrometheusQueryService.queryScalar(promql)
     │ ① promql 위임
     ▼
  PrometheusClient.query(promql)
     │ ② WebClient GET /api/v1/query?query={promql}
     ▼
  Prometheus 서버
     │ ③ JSON 응답
     ▼
  WebClient → Jackson → PrometheusResponse
     │ ④ 클라이언트가 4xx/5xx·디시리얼라이즈 실패 시 PrometheusQueryException
     ▼
  PrometheusQueryService
     │ ⑤ status != "success"            → PrometheusQueryException
     │ ⑥ data.result 비어있음           → return 0.0
     │ ⑦ result[0].value[1] (String)    → Double.parseDouble → return
     ▼
  호출자 (double)

  핵심 분리 원칙:
  - transport 오류 (HTTP/네트워크/파싱) ⇒ Client 책임
  - 도메인 해석 (빈 결과·status 검사·스칼라 추출) ⇒ Service 책임

  ---
  5. 테스트 전략

  테스트 파일 배치: argus/src/test/java/com/example/argus/... (해당 클래스와 미러).

  5.1 PrometheusClientTest — MockWebServer 기반 통합 단위 테스트

  의존: okhttp3:mockwebserver. WebClient의 baseUrl을 MockWebServer URL로 갈아끼워 검증.

  ┌───────────────────────────────────────┬─────────────────────────────────────────────────────────────────────────┐
  │                케이스                 │                                  검증                                   │
  ├───────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────┤
  │ 정상 응답 (status=success, result     │ PrometheusResponse 매핑이 status/data/result/metric/value를 모두        │
  │ 1건)                                  │ 채우는지                                                                │
  ├───────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────┤
  │ 정상 응답 (result 빈 배열)            │ 예외 없이 빈 result를 그대로 반환 (도메인 판단은 서비스에서)            │
  ├───────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────┤
  │ 5xx 응답                              │ PrometheusQueryException 변환                                           │
  ├───────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────┤
  │ 네트워크 끊김 / 타임아웃              │ PrometheusQueryException 변환, cause 보존                               │
  ├───────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────┤
  │ 잘못된 JSON 본문                      │ PrometheusQueryException 변환                                           │
  └───────────────────────────────────────┴─────────────────────────────────────────────────────────────────────────┘

  5.2 PrometheusQueryServiceTest — 순수 Mockito 단위 테스트 (CLAUDE.md "서비스 레이어: 단위 테스트 필수")

  ┌──────────────────┬───────────────────────────────────────────────────┬────────────────────────────────────────────────────────────────┐
  │      케이스      │                       입력                        │                              기대                              │
  ├──────────────────┼───────────────────────────────────────────────────┼────────────────────────────────────────────────────────────────┤
  │ 단일 결과        │ mock Client가 result 1건 (value=[..,"0.42"]) 반환 │ 0.42                                                           │
  ├──────────────────┼───────────────────────────────────────────────────┼────────────────────────────────────────────────────────────────┤
  │ 빈 결과          │ result []                                         │ 0.0 (Acceptance)                                               │
  ├──────────────────┼───────────────────────────────────────────────────┼────────────────────────────────────────────────────────────────┤
  │ status="error"   │ mock 응답 status=error                            │ PrometheusQueryException                                       │
  ├──────────────────┼───────────────────────────────────────────────────┼────────────────────────────────────────────────────────────────┤
  │ Client 예외 전파 │ mock Client가 PrometheusQueryException 던짐       │ 그대로 전파                                                    │
  ├──────────────────┼───────────────────────────────────────────────────┼────────────────────────────────────────────────────────────────┤
  │ 숫자 파싱 실패   │ value[1]이 숫자 아님                              │ PrometheusQueryException (변환 실패도 외부 응답 이상으로 간주) │
  └──────────────────┴───────────────────────────────────────────────────┴────────────────────────────────────────────────────────────────┘

  5.3 컨트롤러 통합 테스트

  본 Task에는 컨트롤러를 도입하지 않으므로 MockMvc 테스트는 범위 외. 컨트롤러 도입은 후속 Task(예: /api/v1/metrics/...)에서 별도 진행.

  5.4 적용 범위 외 (의도적으로 제외)

  - 인증/TLS 설정 — Task Context에 없음
  - Prometheus /query_range 등 다른 엔드포인트 — Goal 범위 밖
  - 캐싱·재시도·서킷브레이커 — 요구사항 없음, 과도 설계 회피

  ---
  산출 파일 요약 (구현 단계 체크리스트)

  1. argus/build.gradle — webflux + mockwebserver 의존 추가
  2. argus/src/main/resources/application.properties — prometheus.base-url
  3. com.example.argus.config.WebClientConfig
  4. com.example.argus.exception.PrometheusQueryException
  5. com.example.argus.dto.PrometheusResponse (+ Data, Result 중첩)
  6. com.example.argus.client.PrometheusClient
  7. com.example.argus.service.PrometheusQueryService
  8. 위 5·6·7에 대응하는 테스트 클래스 (PrometheusClient/Service)