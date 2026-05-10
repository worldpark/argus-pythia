package com.example.argus.service.metric.mapper;

import com.example.argus.dto.PrometheusResponse;
import com.example.argus.dto.metric.jvm.MetricPointDto;
import com.example.argus.service.metric.MetricType;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class MetricPointMapper {

  static final String LABEL_APPLICATION = "application";
  static final String LABEL_INSTANCE = "instance";

  private static final Logger log = LoggerFactory.getLogger(MetricPointMapper.class);

  private MetricPointMapper() {}

  public sealed interface MappingResult {
    record Success(MetricPointDto point) implements MappingResult {}

    record Empty() implements MappingResult {}

    record ParseFailed(String reason) implements MappingResult {}

    record QueryFailed(String reason) implements MappingResult {}
  }

  public sealed interface MultiMappingResult {
    record Success(List<LabeledPoint> points) implements MultiMappingResult {}

    record Empty() implements MultiMappingResult {}

    record ParseFailed(String reason) implements MultiMappingResult {}

    record QueryFailed(String reason) implements MultiMappingResult {}
  }

  // identifierLabel 값과 단일 시계열의 application/instance/timestamp/value 를 보유하는 중간 모델.
  // HTTP는 identifier="uri", HikariCP는 identifier="pool" 라벨로 매핑된다.
  public record LabeledPoint(
      String application,
      String instance,
      String identifier,
      OffsetDateTime timestamp,
      BigDecimal value) {}

  public static MappingResult toPoint(PrometheusResponse response, MetricType type) {
    if (response.getData() == null) {
      return new MappingResult.Empty();
    }

    List<PrometheusResponse.Result> results = response.getData().getResult();
    if (results == null || results.isEmpty()) {
      return new MappingResult.Empty();
    }

    if (results.size() > 1) {
      log.warn("MetricType {} returned {} results; using first only", type, results.size());
    }

    PrometheusResponse.Result first = results.get(0);
    List<Object> value = first.getValue();

    if (value == null || value.size() < 2) {
      return new MappingResult.ParseFailed("value array is missing or too short");
    }

    try {
      OffsetDateTime timestamp = parseTimestamp(value.get(0));

      String rawValue = value.get(1).toString();
      if (isNonFinite(rawValue)) {
        return new MappingResult.ParseFailed("non-finite value: " + rawValue);
      }

      BigDecimal parsedValue = new BigDecimal(rawValue);

      Map<String, String> labels = first.getMetric() != null ? first.getMetric() : Map.of();
      String application = labels.get(LABEL_APPLICATION);
      String instance = labels.get(LABEL_INSTANCE);

      return new MappingResult.Success(new MetricPointDto(application, instance, timestamp, parsedValue));
    } catch (NumberFormatException | ArithmeticException | ClassCastException e) {
      return new MappingResult.ParseFailed("failed to parse value: " + e.getMessage());
    }
  }

  public static MultiMappingResult toPoints(
      PrometheusResponse response, MetricType type, String identifierLabel) {
    if (response.getData() == null) {
      return new MultiMappingResult.Empty();
    }

    List<PrometheusResponse.Result> results = response.getData().getResult();
    if (results == null || results.isEmpty()) {
      return new MultiMappingResult.Empty();
    }

    List<LabeledPoint> points = new ArrayList<>(results.size());
    int skipped = 0;

    for (PrometheusResponse.Result raw : results) {
      List<Object> value = raw.getValue();
      if (value == null || value.size() < 2) {
        skipped++;
        continue;
      }

      try {
        OffsetDateTime timestamp = parseTimestamp(value.get(0));

        String rawValue = value.get(1).toString();
        if (isNonFinite(rawValue)) {
          skipped++;
          continue;
        }

        BigDecimal parsedValue = new BigDecimal(rawValue);

        Map<String, String> labels = raw.getMetric() != null ? raw.getMetric() : Map.of();
        String identifier = labels.get(identifierLabel);
        if (identifier == null) {
          log.warn("MetricType {} result missing label '{}'; skipping", type, identifierLabel);
          skipped++;
          continue;
        }

        String application = labels.get(LABEL_APPLICATION);
        String instance = labels.get(LABEL_INSTANCE);

        points.add(new LabeledPoint(application, instance, identifier, timestamp, parsedValue));
      } catch (NumberFormatException | ArithmeticException | ClassCastException e) {
        skipped++;
      }
    }

    if (points.isEmpty()) {
      if (skipped > 0) {
        return new MultiMappingResult.ParseFailed(
            "all " + skipped + " result(s) failed to parse or missing '" + identifierLabel + "' label");
      }
      return new MultiMappingResult.Empty();
    }

    return new MultiMappingResult.Success(points);
  }

  private static OffsetDateTime parseTimestamp(Object rawTimestamp) {
    BigDecimal epochBd = new BigDecimal(rawTimestamp.toString());
    long sec = epochBd.longValue();
    long nanos = epochBd.subtract(BigDecimal.valueOf(sec))
        .movePointRight(9).longValueExact();
    Instant instant = Instant.ofEpochSecond(sec, nanos);
    return OffsetDateTime.ofInstant(instant, ZoneId.of("Asia/Seoul"));
  }

  private static boolean isNonFinite(String rawValue) {
    return "NaN".equals(rawValue) || "Inf".equals(rawValue) || "-Inf".equals(rawValue);
  }
}
