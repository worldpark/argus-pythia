# 026. Argus PrometheusClient WebClient -> RestClient 전환 Plan

## 구현 목표
Argus 의 Prometheus 외부 호출 클라이언트를 WebClient.block() 기반에서 Spring Boot 3.2+ 정식 동기 API 인 RestClient 기반으로 교체하되, PromQL / DTO / 조립 / Kafka / timeout / 동시성 정책 / 예외 변환 의미는 동등하게 보존한다.

## 영향 범위
- 신규 파일
  - argus/src/main/java/com/example/argus/config/RestClientConfig.java
- 수정 파일
  - argus/src/main/java/com/example/argus/client/PrometheusClient.java (필드/생성자/메서드 본문 RestClient 기반으로 교체)
  - argus/src/test/java/com/example/argus/client/PrometheusClientTest.java (WebClient 의존 제거 및 RestClient 기반 테스트로 갱신)
  - argus/build.gradle (webflux 의존성 제거. 5절 참조 - grep 결과 PrometheusClient 외 사용처 없음을 확인하여 제거 가능)
- 제거 파일
  - argus/src/main/java/com/example/argus/config/WebClientConfig.java (대체 클래스 도입 후 삭제)

> 결정: 동일 이름 유지 대신 RestClientConfig.java 신설 + WebClientConfig.java 삭제.
> 사유: (1) 클래스명이 내부 빈 의미와 일치해 코드 검색/이해성이 높다 (2) "config 이름은 유지하고 내부만 RestClient" 는 차후 혼란 (특히 grep WebClient 가 0 건이 되어야 클린업이 명확해진다).

## 구현 상세

### RestClientConfig.java (신규)
- 역할: Prometheus 호출 전용 RestClient 빈을 등록한다.
- 빈 이름: prometheusRestClient (qualifier 로 사용)
- 사용 컴포넌트
  - java.net.http.HttpClient (JDK HttpClient) - connect timeout 지정
  - org.springframework.http.client.JdkClientHttpRequestFactory - JDK HttpClient 를 ClientHttpRequestFactory 로 어댑팅, read timeout 지정
  - org.springframework.web.client.RestClient
- 시그니처 예시
```
@Configuration
public class RestClientConfig {

  @Bean
  public RestClient prometheusRestClient(@Value("${prometheus.base-url}") String baseUrl) {
    java.net.http.HttpClient jdkHttpClient =
        java.net.http.HttpClient.newBuilder()
            .connectTimeout(Duration.ofMillis(2_000))
            .build();
    JdkClientHttpRequestFactory factory = new JdkClientHttpRequestFactory(jdkHttpClient);
    factory.setReadTimeout(Duration.ofSeconds(5));
    return RestClient.builder()
        .baseUrl(baseUrl)
        .requestFactory(factory)
        .build();
  }
}
```
- timeout 동등성
  - 기존: connect 2_000ms (ChannelOption.CONNECT_TIMEOUT_MILLIS) / response 5s (responseTimeout)
  - 변경: connect 2_000ms (HttpClient.Builder.connectTimeout) / read 5s (JdkClientHttpRequestFactory.setReadTimeout)
  - 의미 동등: connect timeout 은 TCP 핸드셰이크 한도로 동일, response/read timeout 은 양쪽 모두 응답 수신까지의 한도로 의미가 동등하다.
- base URL 처리: 기존 WebClient.builder().baseUrl(...) 와 동등하게 RestClient.builder().baseUrl(...) 사용.
- 새 properties 클래스 신설 안 함. 기존 prometheus.base-url 만 사용. (Task 범위 외 외부화 자제)

### PrometheusClient.java (수정)
- 역할: 동일. PromQL 문자열을 받아 /api/v1/query 호출 후 PrometheusResponse 반환. 외부 노출 API (query(String promql)) 시그니처 무변경.
- 필드/생성자 변경
  - 기존 WebClient prometheusWebClient -> 신규 RestClient prometheusRestClient
  - @Qualifier("prometheusConcurrencyLimiter") ConcurrencyLimiter 그대로 유지
- 메서드 본문 변경 (예시)
```
public PrometheusResponse query(String promql) {
  log.debug("Querying Prometheus with PromQL: {}", promql);
  try {
    return limiter.execute(() ->
        prometheusRestClient
            .get()
            .uri(uriBuilder ->
                uriBuilder.path("/api/v1/query").queryParam("query", "{query}").build(promql))
            .retrieve()
            .body(PrometheusResponse.class));
  } catch (ConcurrencyLimitExceededException e) {
    log.warn("Prometheus query throttled: {}", e.getMessage());
    throw new PrometheusQueryException("Prometheus query throttled: " + e.getMessage(), e);
  } catch (RestClientResponseException e) {
    throw new PrometheusQueryException(
        "Prometheus HTTP error [" + e.getStatusCode() + "]: " + e.getResponseBodyAsString(), e);
  } catch (ResourceAccessException e) {
    throw new PrometheusQueryException("Failed to query Prometheus: " + e.getMessage(), e);
  } catch (RestClientException e) {
    throw new PrometheusQueryException("Failed to query Prometheus: " + e.getMessage(), e);
  } catch (Exception e) {
    throw new PrometheusQueryException("Failed to query Prometheus: " + e.getMessage(), e);
  }
}
```
- ConcurrencyLimiter 결합점: 기존과 동일하게 외부 HTTP 호출 람다를 limiter.execute(...) 로 감싼다. 변경 없음.
- query 파라미터 인코딩: 기존 uriBuilder.queryParam("query", "{query}").build(promql) 패턴 그대로 사용. URI template 변수 치환은 RestClient 의 UriBuilder 에서도 동일하게 동작하며, 특수문자 인코딩 정책 또한 동일하다.
- .block() 제거: RestClient.retrieve().body(T) 가 동기 반환 메서드라 별도 블로킹 호출 불필요.

### PrometheusClientTest.java (수정)
- mock 방식 결정: MockWebServer (okhttp3) 유지로 결정. MockRestServiceServer 미채택.
  - 사유:
    1. MockRestServiceServer 는 RestClient.builder().requestFactory(...) 로 우리 코드가 직접 만든 RequestFactory 를 우회/덮어쓰기 때문에 우리가 검증하려는 connect/read timeout 적용 경로와 다른 객체 그래프가 만들어진다.
    2. 기존 테스트가 이미 MockWebServer (okhttp3) 를 사용 중이며 mockwebserver 의존성이 build.gradle 에 등록되어 있다. 동일 HTTP-level 모킹을 유지하면 회귀 검증이 견고하다.
    3. JSON 직렬화 실패 / 연결 거부 / 5xx 모두 실제 HTTP 동작에 의존해야 RestClientResponseException / ResourceAccessException 분기를 정확히 검증할 수 있다.
  - 결과: 테스트 setUp 에서 MockWebServer 의 base URL 을 받아 production 과 동일 방식으로 RestClient (JDK HttpClient + JdkClientHttpRequestFactory) 를 구성한다.
- setUp 변경 예시
```
// WebClient -> RestClient 로 교체
// baseUrl: mockWebServer.url("/").toString()
java.net.http.HttpClient jdk =
    java.net.http.HttpClient.newBuilder()
        .connectTimeout(Duration.ofMillis(500))
        .build();
JdkClientHttpRequestFactory factory = new JdkClientHttpRequestFactory(jdk);
factory.setReadTimeout(Duration.ofMillis(1_000));
RestClient restClient =
    RestClient.builder()
        .baseUrl(mockWebServer.url("/").toString())
        .requestFactory(factory)
        .build();
client = new PrometheusClient(restClient, realLimiter);
```
- 보존해야 할 케이스 (이름 유지, 동작 동등)
  - query_successWithResult_mapsAllFields
  - query_successWithEmptyResult_returnsEmptyResultList
  - query_promqlWithLabelMatcher_encodesBracesAsQueryValue
  - query_serverError_throwsPrometheusQueryException (HTTP 500 -> RestClientResponseException -> PrometheusQueryException)
  - query_invalidJson_throwsPrometheusQueryException (RestClient 의 HttpMessageConverter 가 RestClientException 계열 throw -> PrometheusQueryException)
  - query_serverUnavailable_throwsPrometheusQueryException (MockWebServer.shutdown() 후 호출 시 ResourceAccessException -> PrometheusQueryException)
  - query_limiterThrottled_wrapsAsPrometheusQueryException (mock limiter 가 ConcurrencyLimitExceededException throw -> PrometheusQueryException, cause = ConcurrencyLimitExceededException)
  - query_limiterPassthrough_normalResponseMapped
- import 변경
  - 제거: org.springframework.web.reactive.function.client.WebClient
  - 추가: org.springframework.web.client.RestClient, org.springframework.http.client.JdkClientHttpRequestFactory

## Spring Boot 컨벤션
- 패키지 구조: 기존 그대로 com.example.argus.config, com.example.argus.client 유지
- 적용할 어노테이션: @Configuration, @Bean, @Value, @Component, @Qualifier
- 예외 처리 방식: PrometheusQueryException (RuntimeException 계열) 로 일원화. catch 블록 순서는 좁은 타입 우선.
  - catch 순서: ConcurrencyLimitExceededException -> RestClientResponseException -> ResourceAccessException -> RestClientException -> Exception
  - 의미 매핑
    - ConcurrencyLimitExceededException -> PrometheusQueryException("Prometheus query throttled: ...", cause) (기존 동일)
    - RestClientResponseException (HTTP 4xx/5xx 응답) -> PrometheusQueryException("Prometheus HTTP error [<status>]: <body>", cause) (기존 WebClientResponseException 매핑과 의미 동등)
    - ResourceAccessException (connect timeout, read timeout, ConnectException 등 I/O 계열) -> PrometheusQueryException("Failed to query Prometheus: ...", cause)
    - RestClientException (HttpMessageConverter 직렬화 실패 등 RestClient 일반 계열) -> PrometheusQueryException("Failed to query Prometheus: ...", cause)
    - 그 외 Exception -> PrometheusQueryException("Failed to query Prometheus: ...", cause)
- 로깅 정책: 기존 log.debug("Querying Prometheus with PromQL: {}", promql) / log.warn("Prometheus query throttled: {}") 패턴 그대로 유지. promql 요약 80자 정책은 기존에 없었으므로 추가하지 않는다.

## 데이터 흐름

### 변경 전
```
Scheduler
  -> Assembler
    -> PrometheusQueryService.queryByMetric / queryScalar
      -> PrometheusClient.query(promql)
        -> limiter.execute(() ->
             prometheusWebClient.get().uri(...).retrieve().bodyToMono(...).block()
           )
```

### 변경 후
```
Scheduler
  -> Assembler
    -> PrometheusQueryService.queryByMetric / queryScalar
      -> PrometheusClient.query(promql)
        -> limiter.execute(() ->
             prometheusRestClient.get().uri(...).retrieve().body(PrometheusResponse.class)
           )
```

- 시그니처 무변경 보장 호출자: PrometheusQueryService, MetricSnapshotAssembler 계열, MetricPointMapper, Scheduler/Publisher 모두 PrometheusClient/Service 의 public API 만 사용 -> 수정 0건.
- 가상 스레드 호환성: Task 025 에서 spring.threads.virtual.enabled: true 로 가상 스레드가 활성화되어 있고, JDK HttpClient 는 가상 스레드 친화적 (블로킹 I/O 가 carrier thread 를 점유하지 않고 park) 으로 동작. 별도 옵션 불필요.

## 완료 조건
- [ ] RestClientConfig.java 신규 생성, prometheusRestClient 빈 등록 (base URL = ${prometheus.base-url}, connect 2_000ms, read 5s)
- [ ] WebClientConfig.java 삭제
- [ ] PrometheusClient.java 가 RestClient 를 주입받고 .block() 없이 동작, 예외 catch 블록이 위 순서/매핑대로 구현
- [ ] PrometheusClientTest.java 가 RestClient + MockWebServer 기반으로 모든 기존 케이스 PASS
- [ ] build.gradle 에서 spring-boot-starter-webflux 제거 (다른 사용처 없음 확인됨, 5절 참조)
- [ ] ./gradlew :argus:test 전체 PASS
- [ ] PrometheusQueryServiceTest, *AssemblerTest, *PublisherTest, MetricSnapshotSchedulerTest 코드 변경 없이 PASS (회귀)
- [ ] WebClient, WebClientResponseException, reactor.netty, ChannelOption import 가 src/main 에 0건 (grep 검증)

---

## 1. 설계 방식 및 이유

### RestClient 선택 근거
- Spring Boot 3.2+ 정식 동기 HTTP 클라이언트 API. 기존 RestTemplate 의 후속 정식 API 이자, fluent builder 패턴으로 WebClient 와 가독성이 유사.
- 본 코드의 외부 HTTP 호출은 이미 .block() 으로 동기 사용 중 -> reactive 모델 손실 없음.
- Java 21 가상 스레드 활성화 환경에서 JDK HttpClient + RestClient 조합은 blocking I/O 가 carrier thread 를 점유하지 않고 park 되어 가상 스레드 친화적.

### ClientHttpRequestFactory 선택
- 채택: JdkClientHttpRequestFactory (org.springframework.http.client.JdkClientHttpRequestFactory)
  - 장점:
    - 추가 의존성 0 (JDK 21 표준의 java.net.http.HttpClient 활용)
    - 가상 스레드 친화 (JDK HttpClient 는 blocking 호출 시 가상 스레드 park 지원)
    - HTTP/2 기본 지원
    - connect/read timeout 모두 표준 API 로 설정 가능
  - 단점:
    - HTTPS 인증서 / proxy 설정 시 JDK HttpClient API 학습 필요 (현재 Task 에서는 사용 안 함)
- 미채택 대안
  - HttpComponentsClientHttpRequestFactory (Apache HttpClient 5): 추가 의존성 필요, 가상 스레드 친화성은 양호하나 본 Task 범위에서 의존성 추가 부담을 피한다.
  - SimpleClientHttpRequestFactory (HttpURLConnection 기반): 구식 API, HTTP/2 미지원, 가상 스레드 친화성 미검증.
  - ReactorClientHttpRequestFactory: 다시 reactor-netty 의존을 가져오므로 webflux 제거 목표와 충돌.

### timeout 설정 위치
- connect timeout: HttpClient.Builder.connectTimeout(Duration.ofMillis(2_000)) - 기존 ChannelOption.CONNECT_TIMEOUT_MILLIS=2_000 과 동등
- read timeout: JdkClientHttpRequestFactory.setReadTimeout(Duration.ofSeconds(5)) - 기존 HttpClient.responseTimeout(Duration.ofSeconds(5)) 와 동등

### ConcurrencyLimiter 결합
- 기존 limiter.execute(Callable) 호출 구조 그대로 유지. 내부 람다에서 호출하는 HTTP 클라이언트만 WebClient -> RestClient 로 교체.
- ConcurrencyLimiter.execute 가 Callable<T> 를 받고 결과를 반환하는 시그니처이므로, RestClient.retrieve().body(...) 의 동기 반환과 호환된다.

### 예외 매핑 변경
| 기존 (WebClient)                                | 변경 (RestClient)                  | 변환                                                                |
| ----------------------------------------------- | ---------------------------------- | ------------------------------------------------------------------- |
| WebClientResponseException (HTTP 4xx/5xx)       | RestClientResponseException        | PrometheusQueryException("Prometheus HTTP error [<status>]: ...")   |
| Exception (timeout, 네트워크, 직렬화 실패 등)   | ResourceAccessException            | PrometheusQueryException("Failed to query Prometheus: ...")         |
| 동상                                            | RestClientException (직렬화 등)    | PrometheusQueryException("Failed to query Prometheus: ...")         |
| ConcurrencyLimitExceededException               | 동일                               | PrometheusQueryException("Prometheus query throttled: ...")         |
| 그 외 Exception                                 | 그 외 Exception                    | PrometheusQueryException("Failed to query Prometheus: ...")         |

## 2. 구성 요소
- 신규: RestClientConfig.java (prometheusRestClient 빈)
- 대체/수정: PrometheusClient.java (필드/생성자/메서드/catch 블록)
- 수정: PrometheusClientTest.java
- 제거: WebClientConfig.java
- 의존성 변경: build.gradle 에서 spring-boot-starter-webflux 제거 (5절 근거)
- 신규 ConfigurationProperties: 신설하지 않음. 기존 prometheus.base-url 그대로 재사용. timeout 값은 코드 상수로 유지 (기존 WebClientConfig 도 동일하게 코드 상수였음 -> 동등성 유지, 외부화는 Task 범위 외).

## 3. 데이터 흐름
- 위 "데이터 흐름" 절 참조.
- 가상 스레드 fan-out 환경 (Task 025) 에서 RestClient + JDK HttpClient 가 가상 스레드 친화적으로 park 되며, ConcurrencyLimiter 의 Semaphore acquire 도 가상 스레드와 호환되어 동시성 제한 의미가 유지된다.

## 4. 예외 처리 전략
- catch 블록 순서 (좁은 타입 우선)
  1. ConcurrencyLimitExceededException -> PrometheusQueryException("Prometheus query throttled: " + msg, cause)
  2. RestClientResponseException -> PrometheusQueryException("Prometheus HTTP error [" + statusCode + "]: " + body, cause)
  3. ResourceAccessException (connect/read timeout, ConnectException, SocketTimeoutException, HttpTimeoutException 등 I/O 계열) -> PrometheusQueryException("Failed to query Prometheus: " + msg, cause)
  4. RestClientException (직렬화 실패 등 일반 RestClient 계열) -> PrometheusQueryException("Failed to query Prometheus: " + msg, cause)
  5. Exception -> PrometheusQueryException("Failed to query Prometheus: " + msg, cause)
- 메시지에 status code 포함 (HTTP 오류 분기에서 e.getStatusCode() 사용)
- 응답 JSON 파싱 실패: RestClient 의 HttpMessageConverter 가 변환 실패 시 RestClientException 계열을 throw -> 4번 catch 분기에서 처리. (필요 시 "invalid JSON" 문구 추가 검토 가능하나, 기존 메시지 정책 유지 우선)
- 로깅: 기존 log.warn("Prometheus query throttled: {}") 패턴 유지. 추가 로깅은 본 Task 범위 외.

## 5. 검증 방법 (테스트)

### mock 방식 결정
- 채택: MockWebServer (okhttp3) 유지. (기존 테스트와 동일)
- 미채택 대안
  - MockRestServiceServer: 우리 코드가 만든 JdkClientHttpRequestFactory 를 우회/덮어쓴다. timeout/JDK HttpClient 적용 경로 검증이 어렵다.
  - WireMock: 새 의존성 도입 부담. mockwebserver 와 기능 차별점 없음.
- 사유: HTTP 레벨에서 실제 응답을 받게 해야 RestClientResponseException, ResourceAccessException, JSON 파싱 실패 분기가 production 동일 코드 경로에서 검증된다. 또한 기존 테스트 인프라 재사용 가능.

### 검증 케이스 (Task 4절 매핑)
| 케이스                                         | 기존 테스트                                              | 변경 사항                          | 기대 결과                                                          |
| ---------------------------------------------- | -------------------------------------------------------- | ---------------------------------- | ------------------------------------------------------------------ |
| 성공 응답 매핑                                 | query_successWithResult_mapsAllFields                    | RestClient 로 setUp 교체           | PrometheusResponse 필드 매핑 동일                                  |
| 빈 결과 매핑                                   | query_successWithEmptyResult_returnsEmptyResultList      | RestClient 로 setUp 교체           | empty list                                                         |
| query parameter 인코딩 (label matcher)         | query_promqlWithLabelMatcher_encodesBracesAsQueryValue   | RestClient 로 setUp 교체           | RecordedRequest.queryParameter("query") 가 원본 PromQL 과 동일     |
| HTTP 5xx -> PrometheusQueryException           | query_serverError_throwsPrometheusQueryException         | RestClient 로 setUp 교체           | PrometheusQueryException, cause = RestClientResponseException      |
| invalid JSON -> PrometheusQueryException       | query_invalidJson_throwsPrometheusQueryException         | RestClient 로 setUp 교체           | PrometheusQueryException, cause = RestClientException 계열         |
| 서버 미가용 -> PrometheusQueryException        | query_serverUnavailable_throwsPrometheusQueryException   | RestClient 로 setUp 교체           | PrometheusQueryException, cause = ResourceAccessException          |
| limiter throttle -> PrometheusQueryException   | query_limiterThrottled_wrapsAsPrometheusQueryException   | mock 대상이 RestClient 로 바뀜만   | cause = ConcurrencyLimitExceededException                          |
| limiter passthrough (회귀)                     | query_limiterPassthrough_normalResponseMapped            | RestClient 로 setUp 교체           | 정상 응답 매핑                                                     |

### 회귀 보증
- PrometheusQueryServiceTest: PrometheusClient.query 호출 시그니처 무변경 -> 테스트 변경 없이 PASS
- MetricSnapshotAssembler*Test, *PublisherTest, MetricSnapshotSchedulerTest: 의존성 그래프 무변경 -> PASS
- 전체 ./gradlew :argus:test PASS

### webflux 제거 검증 (build.gradle)
- grep 결과: argus 모듈 내 WebClient / webflux / reactive 사용처는 다음 4건뿐
  - src/main/java/com/example/argus/client/PrometheusClient.java (본 Task 에서 제거)
  - src/main/java/com/example/argus/config/WebClientConfig.java (본 Task 에서 삭제)
  - src/test/java/com/example/argus/client/PrometheusClientTest.java (본 Task 에서 갱신)
  - build.gradle (본 Task 에서 의존성 제거)
- -> 본 Task 적용 후 spring-boot-starter-webflux 의존성은 제거 가능. 제거한다.
- 추가 검증 단계: 의존성 제거 후 ./gradlew :argus:build PASS 로 transitive 누락 없음을 확인.

## 6. 트레이드오프

### RestClient 의 단점
- 비동기 모델 자체가 사라짐 -> 본 코드는 어차피 .block() 사용이라 손실 없음.
- reactive 백프레셔 / streaming 응답 미지원 -> Prometheus 단발 query 응답에는 영향 없음.

### JdkClientHttpRequestFactory 의 단점
- HTTPS 클라이언트 인증서 / proxy / connection pool 세밀 튜닝 시 JDK HttpClient API 학습 필요. 현재 Task 범위 외.
- 향후 인증/proxy 추가 시 RestClient.builder().requestInterceptor(...) 또는 HttpClient.Builder.authenticator(...) 로 확장 가능.

### webflux 의존성 제거 시
- 다른 곳에서 WebClient 가 쓰이지 않는지 확인 완료 (위 grep). Spring Actuator / Security 의 transitive 의존도 webflux starter 가 없으면 자동 webflux 자동설정이 비활성화되므로 안전.
- 빌드 사이즈 감소, 자동설정 비용 감소.

### 채택하지 않은 대안
- Apache HttpClient 5: 의존성 추가 부담, 현재 시점 이득 미미.
- OkHttp: 새 의존성 도입.
- Resilience4j Retry/Circuit: 본 Task 범위 외 (이미 ConcurrencyLimiter 적용, 추가 정책은 별도 Task).

### 향후 확장 여지
- JDK HttpClient 는 HTTP/2 기본 지원 -> Prometheus 가 HTTP/2 지원 시 자동 활용.
- 인증/proxy 추가 시 RestClient builder 에 requestInterceptor 추가로 점진적 확장 가능.
- 외부화 요구 발생 시 prometheus.connect-timeout, prometheus.read-timeout 키와 @ConfigurationProperties 도입 (현재 Task 범위 외).
