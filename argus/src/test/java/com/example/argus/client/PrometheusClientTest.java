package com.example.argus.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.example.argus.dto.PrometheusResponse;
import com.example.argus.exception.PrometheusQueryException;
import java.io.IOException;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.web.reactive.function.client.WebClient;

class PrometheusClientTest {

  private MockWebServer mockWebServer;
  private PrometheusClient client;

  @BeforeEach
  void setUp() throws IOException {
    mockWebServer = new MockWebServer();
    mockWebServer.start();
    WebClient webClient =
        WebClient.builder().baseUrl(mockWebServer.url("/").toString()).build();
    client = new PrometheusClient(webClient);
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

    assertThatThrownBy(() -> client.query("up")).isInstanceOf(PrometheusQueryException.class);
  }

  @Test
  void query_invalidJson_throwsPrometheusQueryException() {
    mockWebServer.enqueue(
        new MockResponse()
            .setBody("not-valid-json")
            .addHeader("Content-Type", "application/json"));

    assertThatThrownBy(() -> client.query("up")).isInstanceOf(PrometheusQueryException.class);
  }

  @Test
  void query_serverUnavailable_throwsPrometheusQueryException() throws IOException {
    mockWebServer.shutdown();

    assertThatThrownBy(() -> client.query("up")).isInstanceOf(PrometheusQueryException.class);
  }
}
