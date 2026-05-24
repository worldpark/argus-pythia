package com.example.argus.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.example.argus.common.concurrency.ConcurrencyLimiter;
import com.example.argus.dto.PrometheusResponse;
import com.example.argus.exception.ConcurrencyLimitExceededException;
import com.example.argus.exception.PrometheusQueryException;
import java.io.IOException;
import java.net.http.HttpClient;
import java.time.Duration;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

class PrometheusClientTest {

    private MockWebServer mockWebServer;
    private PrometheusClient client;
    // 실제 Limiter: permit 충분, 빠른 timeout — 기존 케이스 회귀 보장
    private ConcurrencyLimiter realLimiter;

    @BeforeEach
    void setUp() throws IOException {
        mockWebServer = new MockWebServer();
        mockWebServer.start();
        HttpClient jdkHttpClient =
            HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(500))
                .build();
        JdkClientHttpRequestFactory factory = new JdkClientHttpRequestFactory(jdkHttpClient);
        factory.setReadTimeout(Duration.ofMillis(1_000));
        RestClient restClient =
            RestClient.builder()
                .baseUrl(mockWebServer.url("/").toString())
                .requestFactory(factory)
                .build();
        realLimiter = new ConcurrencyLimiter("prometheus-test", 10, Duration.ofMillis(50), 1);
        client = new PrometheusClient(restClient, realLimiter);
    }

    @AfterEach
    void tearDown() {
        try {
            mockWebServer.shutdown();
        } catch (IOException ignored) {
        }
    }

    @Test
    void query_successWithResult_mapsAllFields() {
        mockWebServer.enqueue(
            new MockResponse()
                .setBody(
                    """
                    {
                      "status": "success",
                      "data": {
                        "resultType": "vector",
                        "result": [
                          {
                            "metric": {"__name__": "up"},
                            "value": [1714000000, "0.42"]
                          }
                        ]
                      }
                    }""")
                .addHeader("Content-Type", "application/json"));

        PrometheusResponse response = client.query("up");

        assertThat(response.getStatus()).isEqualTo("success");
        assertThat(response.getData().getResultType()).isEqualTo("vector");
        assertThat(response.getData().getResult()).hasSize(1);
        assertThat(response.getData().getResult().get(0).getMetric()).containsEntry("__name__", "up");
        assertThat(response.getData().getResult().get(0).getValue().get(1)).isEqualTo("0.42");
    }

    @Test
    void query_successWithEmptyResult_returnsEmptyResultList() {
        mockWebServer.enqueue(
            new MockResponse()
                .setBody(
                    """
                    {
                      "status": "success",
                      "data": {
                        "resultType": "vector",
                        "result": []
                      }
                    }""")
                .addHeader("Content-Type", "application/json"));

        PrometheusResponse response = client.query("up");

        assertThat(response.getStatus()).isEqualTo("success");
        assertThat(response.getData().getResult()).isEmpty();
    }

    @Test
    void query_promqlWithLabelMatcher_encodesBracesAsQueryValue() throws InterruptedException {
        mockWebServer.enqueue(
            new MockResponse()
                .setBody(
                    """
                    {
                      "status": "success",
                      "data": {
                        "resultType": "vector",
                        "result": []
                      }
                    }""")
                .addHeader("Content-Type", "application/json"));

        String promql = "jvm_memory_used_bytes{area=\"heap\"}";

        PrometheusResponse response = client.query(promql);

        RecordedRequest request = mockWebServer.takeRequest();
        assertThat(response.getStatus()).isEqualTo("success");
        assertThat(request.getRequestUrl().queryParameter("query")).isEqualTo(promql);
    }

    @Test
    void query_serverError_throwsPrometheusQueryException() {
        mockWebServer.enqueue(new MockResponse().setResponseCode(500).setBody("Internal Server Error"));

        assertThatThrownBy(() -> client.query("up"))
            .isInstanceOf(PrometheusQueryException.class)
            .satisfies(ex -> assertThat(ex.getCause())
                .isInstanceOf(org.springframework.web.client.RestClientResponseException.class));
    }

    @Test
    void query_invalidJson_throwsPrometheusQueryException() {
        mockWebServer.enqueue(
            new MockResponse()
                .setBody("not-valid-json")
                .addHeader("Content-Type", "application/json"));

        assertThatThrownBy(() -> client.query("up"))
            .isInstanceOf(PrometheusQueryException.class);
    }

    @Test
    void query_serverUnavailable_throwsPrometheusQueryException() throws IOException {
        mockWebServer.shutdown();

        assertThatThrownBy(() -> client.query("up"))
            .isInstanceOf(PrometheusQueryException.class)
            .satisfies(ex -> assertThat(ex.getCause())
                .isInstanceOf(org.springframework.web.client.ResourceAccessException.class));
    }

    @Test
    @DisplayName("limiter 가 ConcurrencyLimitExceededException 을 던질 때 PrometheusQueryException 으로 변환되고 cause 가 ConcurrencyLimitExceededException 이다")
    void query_limiterThrottled_wrapsAsPrometheusQueryException() throws Exception {
        ConcurrencyLimiter mockLimiter = mock(ConcurrencyLimiter.class);
        HttpClient jdkHttpClient =
            HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(500))
                .build();
        JdkClientHttpRequestFactory factory = new JdkClientHttpRequestFactory(jdkHttpClient);
        factory.setReadTimeout(Duration.ofMillis(1_000));
        RestClient restClient =
            RestClient.builder()
                .baseUrl(mockWebServer.url("/").toString())
                .requestFactory(factory)
                .build();
        PrometheusClient throttledClient = new PrometheusClient(restClient, mockLimiter);

        ConcurrencyLimitExceededException throttleEx =
            new ConcurrencyLimitExceededException("prometheus", 3, 600L);
        when(mockLimiter.execute(any())).thenThrow(throttleEx);

        assertThatThrownBy(() -> throttledClient.query("up"))
            .isInstanceOf(PrometheusQueryException.class)
            .satisfies(ex -> {
                assertThat(ex.getCause()).isInstanceOf(ConcurrencyLimitExceededException.class);
                assertThat(ex.getMessage()).contains("throttled");
            });
    }

    @Test
    @DisplayName("정상 query: limiter 가 action 을 정상 invoke 하면 기존 응답 매핑이 정상 동작한다 (회귀)")
    void query_limiterPassthrough_normalResponseMapped() throws Exception {
        mockWebServer.enqueue(
            new MockResponse()
                .setBody(
                    """
                    {
                      "status": "success",
                      "data": {
                        "resultType": "vector",
                        "result": []
                      }
                    }""")
                .addHeader("Content-Type", "application/json"));

        // realLimiter 는 permit 충분 -> 정상 통과
        PrometheusResponse response = client.query("up");
        assertThat(response.getStatus()).isEqualTo("success");
    }
}
