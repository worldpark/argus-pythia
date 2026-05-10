package com.example.argus.client;

import com.example.argus.dto.PrometheusResponse;
import com.example.argus.exception.PrometheusQueryException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;

@Component
@RequiredArgsConstructor
@Slf4j
public class PrometheusClient {

  private final WebClient prometheusWebClient;

  public PrometheusResponse query(String promql) {
    log.debug("Querying Prometheus with PromQL: {}", promql);
    try {
      return prometheusWebClient
          .get()
          .uri(uriBuilder ->
              uriBuilder.path("/api/v1/query").queryParam("query", "{query}").build(promql))
          .retrieve()
          .bodyToMono(PrometheusResponse.class)
          .block();
    } catch (WebClientResponseException e) {
      throw new PrometheusQueryException(
          "Prometheus HTTP error [" + e.getStatusCode() + "]: " + e.getResponseBodyAsString(), e);
    } catch (Exception e) {
      throw new PrometheusQueryException("Failed to query Prometheus: " + e.getMessage(), e);
    }
  }
}
