package com.example.argus.service.metric.snapshot;

import com.example.argus.dto.metric.http.HttpMetricResult;
import com.example.argus.dto.metric.http.HttpMetricSnapshotDto;
import com.example.argus.dto.metric.http.HttpResponseTimeDto;
import com.example.argus.dto.metric.http.HttpThroughputDto;
import com.example.argus.dto.metric.http.HttpErrorRateDto;
import com.example.argus.dto.metric.SnapshotStatus;
import com.example.argus.exception.PrometheusQueryException;
import com.example.argus.service.PrometheusQueryService;
import com.example.argus.service.metric.MetricType;
import com.example.argus.service.metric.mapper.MetricPointMapper;
import com.example.argus.service.metric.mapper.MetricPointMapper.LabeledPoint;
import com.example.argus.service.metric.mapper.MetricPointMapper.MultiMappingResult;
import java.time.Clock;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class HttpMetricSnapshotAssembler {

  private static final String LABEL_URI = "uri";

  private static final Logger log = LoggerFactory.getLogger(HttpMetricSnapshotAssembler.class);

  private final PrometheusQueryService queryService;
  private final Clock clock;

  public HttpMetricSnapshotDto assemble() {
    LabelAccumulator labels = new LabelAccumulator();
    OffsetDateTime collectedAt = OffsetDateTime.now(clock);

    HttpResponseTimeDto p99 = resolve(MetricType.HTTP_P99_RESPONSE_TIME,
        withLabels(labels, HttpResponseTimeDto::from),
        HttpResponseTimeDto::queryFailed,
        HttpResponseTimeDto::parseFailed);
    // HTTP_RPS PromQL 은 sum by (uri) 라 application/instance 라벨이 없음 → 라벨 누적 대상 아님
    HttpThroughputDto rps = resolve(MetricType.HTTP_RPS,
        HttpThroughputDto::from,
        HttpThroughputDto::queryFailed,
        HttpThroughputDto::parseFailed);
    HttpErrorRateDto errorRate = resolve(MetricType.HTTP_ERROR_RATE,
        withLabels(labels, HttpErrorRateDto::from),
        HttpErrorRateDto::queryFailed,
        HttpErrorRateDto::parseFailed);

    SnapshotStatus snapshotStatus =
        SnapshotStatus.from(List.of(p99.status(), rps.status(), errorRate.status()));

    return new HttpMetricSnapshotDto(
        labels.application(), labels.instance(), collectedAt, p99, rps, errorRate, snapshotStatus);
  }

  private <T extends HttpMetricResult> T resolve(
      MetricType type,
      Function<MultiMappingResult, T> ok,
      Function<String, T> queryFailed,
      Function<String, T> parseFailed) {
    try {
      return ok.apply(map(type));
    } catch (PrometheusQueryException e) {
      return queryFailed.apply(e.getMessage());
    } catch (Exception e) {
      log.error("Unexpected error while querying {}", type, e);
      return parseFailed.apply(e.getMessage());
    }
  }

  private <T extends HttpMetricResult> Function<MultiMappingResult, T> withLabels(
      LabelAccumulator labels, Function<MultiMappingResult, T> mapper) {
    return result -> {
      if (result instanceof MultiMappingResult.Success s) {
        s.points().forEach(labels::accept);
      }
      return mapper.apply(result);
    };
  }

  private MultiMappingResult map(MetricType type) {
    return MetricPointMapper.toPoints(queryService.queryByMetric(type), type, LABEL_URI);
  }

  private static class LabelAccumulator {
    private String application;
    private String instance;

    void accept(LabeledPoint point) {
      if (application == null && point.application() != null) {
        application = point.application();
      }
      if (instance == null && point.instance() != null) {
        instance = point.instance();
      }
    }

    String application() {
      return application;
    }

    String instance() {
      return instance;
    }
  }
}
