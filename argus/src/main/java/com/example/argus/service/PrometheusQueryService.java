package com.example.argus.service;

import com.example.argus.client.PrometheusClient;
import com.example.argus.dto.PrometheusResponse;
import com.example.argus.exception.PrometheusQueryException;
import com.example.argus.service.metric.MetricType;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class PrometheusQueryService {

  private final PrometheusClient client;

  public PrometheusResponse queryByMetric(MetricType type) {
    PrometheusResponse response = client.query(type.getPromql());
    if (!"success".equals(response.getStatus())) {
      throw new PrometheusQueryException(
          "Prometheus query returned non-success status: " + response.getStatus());
    }
    return response;
  }

  public double queryScalar(String promql) {
    PrometheusResponse response = client.query(promql);

    if (!"success".equals(response.getStatus())) {
      throw new PrometheusQueryException(
          "Prometheus query returned non-success status: " + response.getStatus());
    }

    if (response.getData() == null) {
      return 0.0;
    }

    List<PrometheusResponse.Result> results = response.getData().getResult();
    if (results == null || results.isEmpty()) {
      return 0.0;
    }

    try {
      List<Object> value = results.get(0).getValue();
      if (value == null || value.size() < 2) {
        throw new PrometheusQueryException("Prometheus result value is missing or incomplete");
      }
      return Double.parseDouble(value.get(1).toString());
    } catch (NumberFormatException | NullPointerException e) {
      throw new PrometheusQueryException(
          "Failed to parse scalar value from Prometheus response", e);
    }
  }
}
