package com.example.argus.service.metric.snapshot;

import com.example.argus.dto.metric.hikari.HikariActiveDto;
import com.example.argus.dto.metric.hikari.HikariMetricResult;
import com.example.argus.dto.metric.hikari.HikariMetricSnapshotDto;
import com.example.argus.dto.metric.hikari.HikariPendingDto;
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
public class HikariMetricSnapshotAssembler {

  private static final String LABEL_POOL = "pool";

  private static final Logger log = LoggerFactory.getLogger(HikariMetricSnapshotAssembler.class);

  private final PrometheusQueryService queryService;
  private final Clock clock;

  public HikariMetricSnapshotDto assemble() {
    LabelAccumulator labels = new LabelAccumulator();
    OffsetDateTime collectedAt = OffsetDateTime.now(clock);

    HikariActiveDto active = resolveActive(labels);
    HikariPendingDto pending = resolve(MetricType.HIKARI_PENDING_CONNECTIONS,
        withLabels(labels, HikariPendingDto::from),
        HikariPendingDto::queryFailed,
        HikariPendingDto::parseFailed);

    SnapshotStatus snapshotStatus = SnapshotStatus.from(List.of(active.status(), pending.status()));

    return new HikariMetricSnapshotDto(
        labels.application(), labels.instance(), collectedAt, active, pending, snapshotStatus);
  }

  private <T extends HikariMetricResult> T resolve(
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

  private HikariActiveDto resolveActive(LabelAccumulator labels) {
    MultiMappingResult activeResult = mapWithQueryFailFallback(MetricType.HIKARI_ACTIVE_CONNECTIONS, labels);
    MultiMappingResult usageRatioResult = mapWithQueryFailFallback(MetricType.HIKARI_USAGE_RATIO, labels);
    return HikariActiveDto.from(activeResult, usageRatioResult);
  }

  private MultiMappingResult mapWithQueryFailFallback(MetricType type, LabelAccumulator labels) {
    try {
      MultiMappingResult result = map(type);
      if (result instanceof MultiMappingResult.Success s) {
        s.points().forEach(labels::accept);
      }
      return result;
    } catch (PrometheusQueryException e) {
      return new MultiMappingResult.QueryFailed(e.getMessage());
    } catch (Exception e) {
      log.error("Unexpected error while querying {}", type, e);
      return new MultiMappingResult.ParseFailed(e.getMessage());
    }
  }

  private <T extends HikariMetricResult> Function<MultiMappingResult, T> withLabels(
      LabelAccumulator labels, Function<MultiMappingResult, T> mapper) {
    return result -> {
      if (result instanceof MultiMappingResult.Success s) {
        s.points().forEach(labels::accept);
      }
      return mapper.apply(result);
    };
  }

  private MultiMappingResult map(MetricType type) {
    return MetricPointMapper.toPoints(queryService.queryByMetric(type), type, LABEL_POOL);
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
