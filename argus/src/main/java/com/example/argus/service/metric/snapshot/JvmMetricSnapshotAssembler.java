package com.example.argus.service.metric.snapshot;

import com.example.argus.dto.metric.jvm.CpuUsageDto;
import com.example.argus.dto.metric.jvm.GcMetricDto;
import com.example.argus.dto.metric.jvm.JvmMetricResult;
import com.example.argus.dto.metric.jvm.JvmMetricSnapshotDto;
import com.example.argus.dto.metric.jvm.MemoryUsageDto;
import com.example.argus.dto.metric.jvm.MetricPointDto;
import com.example.argus.dto.metric.SnapshotStatus;
import com.example.argus.dto.metric.jvm.ThreadMetricDto;
import com.example.argus.exception.PrometheusQueryException;
import com.example.argus.service.PrometheusQueryService;
import com.example.argus.service.metric.MetricType;
import com.example.argus.service.metric.mapper.MetricPointMapper;
import com.example.argus.service.metric.mapper.MetricPointMapper.MappingResult;
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
public class JvmMetricSnapshotAssembler {

  private static final Logger log = LoggerFactory.getLogger(JvmMetricSnapshotAssembler.class);

  private final PrometheusQueryService queryService;
  private final Clock clock;

  public JvmMetricSnapshotDto assemble() {
    LabelAccumulator labels = new LabelAccumulator();
    OffsetDateTime collectedAt = OffsetDateTime.now(clock);

    CpuUsageDto cpu = resolve(MetricType.CPU_USAGE,
        withLabels(labels, CpuUsageDto::from),
        CpuUsageDto::queryFailed,
        CpuUsageDto::parseFailed);
    MemoryUsageDto memory = resolveMemory(labels);
    GcMetricDto gc = resolveGc(labels);
    ThreadMetricDto thread = resolveThread(labels);

    SnapshotStatus snapshotStatus =
        SnapshotStatus.from(List.of(cpu.status(), memory.status(), gc.status(), thread.status()));

    return new JvmMetricSnapshotDto(
        labels.application(), labels.instance(), collectedAt, cpu, memory, gc, thread, snapshotStatus);
  }

  private <T extends JvmMetricResult> T resolve(
      MetricType type,
      Function<MappingResult, T> ok,
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

  private GcMetricDto resolveGc(LabelAccumulator labels) {
    MappingResult durationResult = mapWithQueryFailFallback(MetricType.GC_AVG_DURATION, labels);
    MappingResult countResult = mapWithQueryFailFallback(MetricType.GC_COUNT, labels);
    return GcMetricDto.from(durationResult, countResult);
  }

  private MemoryUsageDto resolveMemory(LabelAccumulator labels) {
    MappingResult heapResult = mapWithQueryFailFallback(MetricType.HEAP_USAGE, labels);
    MappingResult oldGenResult = mapWithQueryFailFallback(MetricType.HEAP_OLD_GEN_USAGE, labels);
    return MemoryUsageDto.from(heapResult, oldGenResult);
  }

  private ThreadMetricDto resolveThread(LabelAccumulator labels) {
    MappingResult activeResult = mapWithQueryFailFallback(MetricType.ACTIVE_THREADS, labels);
    MappingResult peakResult = mapWithQueryFailFallback(MetricType.PEAK_THREADS, labels);
    MappingResult daemonResult = mapWithQueryFailFallback(MetricType.DAEMON_THREADS, labels);
    return ThreadMetricDto.from(activeResult, peakResult, daemonResult);
  }

  private MappingResult mapWithQueryFailFallback(MetricType type, LabelAccumulator labels) {
    try {
      MappingResult result = map(type);
      if (result instanceof MappingResult.Success s) {
        labels.accept(s.point());
      }
      return result;
    } catch (PrometheusQueryException e) {
      return new MappingResult.QueryFailed(e.getMessage());
    } catch (Exception e) {
      log.error("Unexpected error while querying {}", type, e);
      return new MappingResult.ParseFailed(e.getMessage());
    }
  }

  private <T extends JvmMetricResult> Function<MappingResult, T> withLabels(
      LabelAccumulator labels, Function<MappingResult, T> mapper) {
    return result -> {
      if (result instanceof MappingResult.Success s) {
        labels.accept(s.point());
      }
      return mapper.apply(result);
    };
  }

  private MappingResult map(MetricType type) {
    return MetricPointMapper.toPoint(queryService.queryByMetric(type), type);
  }

  private static class LabelAccumulator {
    private String application;
    private String instance;

    void accept(MetricPointDto point) {
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
