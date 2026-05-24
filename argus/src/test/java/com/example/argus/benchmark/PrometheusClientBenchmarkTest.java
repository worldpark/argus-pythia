package com.example.argus.benchmark;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.argus.client.PrometheusClient;
import com.example.argus.common.concurrency.ConcurrencyLimiter;
import java.io.IOException;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

class PrometheusClientBenchmarkTest {

  private static final String SUCCESS_BODY =
      """
      {
        "status": "success",
        "data": {
          "resultType": "vector",
          "result": [
            {
              "metric": {"application": "argus", "instance": "localhost:8080"},
              "value": [1714000000, "0.42"]
            }
          ]
        }
      }""";

  private MockWebServer mockWebServer;
  private PrometheusClient client;

  @BeforeAll
  static void requireOptIn() {
    BenchmarkSupport.requireOptIn();
  }

  @BeforeEach
  void setUp() throws IOException {
    mockWebServer = new MockWebServer();
    mockWebServer.start();

    HttpClient jdkHttpClient =
        HttpClient.newBuilder().connectTimeout(Duration.ofMillis(500)).build();
    JdkClientHttpRequestFactory requestFactory = new JdkClientHttpRequestFactory(jdkHttpClient);
    requestFactory.setReadTimeout(Duration.ofSeconds(2));

    RestClient restClient =
        RestClient.builder()
            .baseUrl(mockWebServer.url("/").toString())
            .requestFactory(requestFactory)
            .build();

    ConcurrencyLimiter limiter =
        new ConcurrencyLimiter("prometheus-benchmark", 256, Duration.ofMillis(100), 1);
    client = new PrometheusClient(restClient, limiter);
  }

  @AfterEach
  void tearDown() throws IOException {
    mockWebServer.shutdown();
  }

  @Test
  @DisplayName("PrometheusClient benchmark: 100ms 지연 응답에서 platform thread vs virtual thread 비교")
  void benchmark_queryWith100msDelay_platformVsVirtual() throws Exception {
    warmUp(10, 20);

    BenchmarkSupport.BenchmarkResult platform =
        runScenario("prometheus-client-100ms", "platform-fixed-20", 60, 20, 100);
    BenchmarkSupport.BenchmarkResult virtual =
        runScenario("prometheus-client-100ms", "virtual-per-task-20", 60, 20, 100);

    BenchmarkSupport.printResult(platform);
    BenchmarkSupport.printResult(virtual);
    BenchmarkSupport.printComparison(platform, virtual);

    assertThat(platform.failures()).isZero();
    assertThat(virtual.failures()).isZero();
  }

  @Test
  @DisplayName("PrometheusClient benchmark: 300ms 지연 응답에서 platform thread vs virtual thread 비교")
  void benchmark_queryWith300msDelay_platformVsVirtual() throws Exception {
    warmUp(10, 50);

    BenchmarkSupport.BenchmarkResult platform =
        runScenario("prometheus-client-300ms", "platform-fixed-50", 100, 50, 300);
    BenchmarkSupport.BenchmarkResult virtual =
        runScenario("prometheus-client-300ms", "virtual-per-task-50", 100, 50, 300);

    BenchmarkSupport.printResult(platform);
    BenchmarkSupport.printResult(virtual);
    BenchmarkSupport.printComparison(platform, virtual);

    assertThat(platform.failures()).isZero();
    assertThat(virtual.failures()).isZero();
  }

  private void warmUp(int taskCount, long delayMs) throws Exception {
    enqueueSuccessResponses(taskCount, delayMs);
    BenchmarkSupport.runScenario(
        "prometheus-client-warmup",
        "platform-fixed-5",
        taskCount,
        5,
        () -> Executors.newFixedThreadPool(5),
        () -> client.query("up"));
  }

  private BenchmarkSupport.BenchmarkResult runScenario(
      String scenario,
      String executionModel,
      int taskCount,
      int concurrency,
      long delayMs)
      throws Exception {

    enqueueSuccessResponses(taskCount, delayMs);

    return BenchmarkSupport.runScenario(
        scenario,
        executionModel,
        taskCount,
        concurrency,
        () ->
            executionModel.startsWith("virtual")
                ? Executors.newVirtualThreadPerTaskExecutor()
                : Executors.newFixedThreadPool(concurrency),
        () -> client.query("up"));
  }

  private void enqueueSuccessResponses(int count, long delayMs) {
    for (int i = 0; i < count; i++) {
      mockWebServer.enqueue(
          new MockResponse()
              .setBody(SUCCESS_BODY)
              .setBodyDelay(delayMs, TimeUnit.MILLISECONDS)
              .addHeader("Content-Type", "application/json"));
    }
  }
}
