package com.example.argus.client;

import com.example.argus.common.concurrency.ConcurrencyLimiter;
import com.example.argus.dto.PrometheusResponse;
import com.example.argus.exception.ConcurrencyLimitExceededException;
import com.example.argus.exception.PrometheusQueryException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestClientResponseException;

@Component
@Slf4j
public class PrometheusClient {

    private final RestClient prometheusRestClient;
    private final ConcurrencyLimiter limiter;

    public PrometheusClient(
        @Qualifier("prometheusRestClient") RestClient prometheusRestClient,
        @Qualifier("prometheusConcurrencyLimiter") ConcurrencyLimiter limiter) {
        this.prometheusRestClient = prometheusRestClient;
        this.limiter = limiter;
    }

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
}
