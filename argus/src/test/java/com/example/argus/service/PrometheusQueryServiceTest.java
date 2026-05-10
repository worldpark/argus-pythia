package com.example.argus.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.example.argus.client.PrometheusClient;
import com.example.argus.dto.PrometheusResponse;
import com.example.argus.exception.PrometheusQueryException;
import com.example.argus.service.metric.MetricType;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PrometheusQueryServiceTest {

  @Mock private PrometheusClient client;

  @InjectMocks private PrometheusQueryService service;

  @Test
  void queryScalar_singleResult_returnsValue() {
    when(client.query("up")).thenReturn(buildSuccessResponse(List.of(buildResult("0.42"))));

    assertThat(service.queryScalar("up")).isEqualTo(0.42);
  }

  @Test
  void queryScalar_emptyResult_returnsZero() {
    when(client.query("up")).thenReturn(buildSuccessResponse(Collections.emptyList()));

    assertThat(service.queryScalar("up")).isEqualTo(0.0);
  }

  @Test
  void queryScalar_statusError_throwsPrometheusQueryException() {
    when(client.query("up")).thenReturn(buildResponse("error", Collections.emptyList()));

    assertThatThrownBy(() -> service.queryScalar("up"))
        .isInstanceOf(PrometheusQueryException.class);
  }

  @Test
  void queryScalar_clientThrows_propagatesException() {
    when(client.query("up")).thenThrow(new PrometheusQueryException("network error"));

    assertThatThrownBy(() -> service.queryScalar("up"))
        .isInstanceOf(PrometheusQueryException.class)
        .hasMessage("network error");
  }

  @Test
  void queryScalar_nonNumericValue_throwsPrometheusQueryException() {
    when(client.query("up")).thenReturn(buildSuccessResponse(List.of(buildResult("not-a-number"))));

    assertThatThrownBy(() -> service.queryScalar("up"))
        .isInstanceOf(PrometheusQueryException.class);
  }

  @Test
  void queryScalar_dataNullInResponse_returnsZero() {
    PrometheusResponse response = new PrometheusResponse();
    response.setStatus("success");
    response.setData(null);
    when(client.query("up")).thenReturn(response);

    assertThat(service.queryScalar("up")).isEqualTo(0.0);
  }

  @Test
  void queryScalar_valueTooShort_throwsPrometheusQueryException() {
    PrometheusResponse.Result result = new PrometheusResponse.Result();
    result.setMetric(Map.of());
    result.setValue(List.of(1714000000L));
    when(client.query("up")).thenReturn(buildSuccessResponse(List.of(result)));

    assertThatThrownBy(() -> service.queryScalar("up"))
        .isInstanceOf(PrometheusQueryException.class);
  }

  @Test
  void queryByMetric_delegatesPromqlToClient() {
    MetricType type = MetricType.CPU_USAGE;
    ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
    when(client.query(any())).thenReturn(buildSuccessResponse(Collections.emptyList()));

    service.queryByMetric(type);

    verify(client).query(captor.capture());
    assertThat(captor.getValue()).isEqualTo(type.getPromql());
  }

  @Test
  void queryByMetric_successStatus_returnsSameResponse() {
    MetricType type = MetricType.HEAP_USAGE;
    PrometheusResponse expected = buildSuccessResponse(List.of(buildResult("72.5")));
    when(client.query(type.getPromql())).thenReturn(expected);

    PrometheusResponse actual = service.queryByMetric(type);

    assertThat(actual).isSameAs(expected);
  }

  @Test
  void queryByMetric_errorStatus_throwsPrometheusQueryException() {
    MetricType type = MetricType.ACTIVE_THREADS;
    when(client.query(type.getPromql()))
        .thenReturn(buildResponse("error", Collections.emptyList()));

    assertThatThrownBy(() -> service.queryByMetric(type))
        .isInstanceOf(PrometheusQueryException.class)
        .hasMessageContaining("non-success status");
  }

  @Test
  void queryByMetric_clientThrows_propagatesException() {
    MetricType type = MetricType.HTTP_RPS;
    when(client.query(type.getPromql()))
        .thenThrow(new PrometheusQueryException("network error"));

    assertThatThrownBy(() -> service.queryByMetric(type))
        .isInstanceOf(PrometheusQueryException.class)
        .hasMessage("network error");
  }

  // --- helpers ---

  private PrometheusResponse buildSuccessResponse(List<PrometheusResponse.Result> results) {
    return buildResponse("success", results);
  }

  private PrometheusResponse buildResponse(String status, List<PrometheusResponse.Result> results) {
    PrometheusResponse.Datas data = new PrometheusResponse.Datas();
    data.setResultType("vector");
    data.setResult(results);

    PrometheusResponse response = new PrometheusResponse();
    response.setStatus(status);
    response.setData(data);
    return response;
  }

  private PrometheusResponse.Result buildResult(String value) {
    PrometheusResponse.Result result = new PrometheusResponse.Result();
    result.setMetric(Map.of());
    result.setValue(List.of(1714000000L, value));
    return result;
  }
}
