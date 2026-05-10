package com.example.argus.dto.metric.jvm;

import com.example.argus.dto.metric.MetricStatus;
import com.example.argus.service.metric.mapper.MetricPointMapper.MappingResult;
import java.time.OffsetDateTime;

public record ThreadMetricDto(
    Integer activeCount,
    Integer peakCount,
    Integer daemonCount,
    OffsetDateTime measuredAt,
    MetricStatus status,
    String missingReason)
    implements JvmMetricResult {

  public static ThreadMetricDto from(MappingResult result) {
    return JvmMetricResult.from(result,
        (value, ts) -> {
          try {
            return ThreadMetricDto.success(value.intValueExact(), null, null, ts);
          } catch (ArithmeticException e) {
            return ThreadMetricDto.parseFailed("cannot convert to int: " + e.getMessage());
          }
        },
        ThreadMetricDto::empty,
        ThreadMetricDto::parseFailed,
        ThreadMetricDto::queryFailed);
  }

  public static ThreadMetricDto from(
      MappingResult activeResult, MappingResult peakResult, MappingResult daemonResult) {
    Integer active = valueOrNull(activeResult);
    Integer peak = valueOrNull(peakResult);
    Integer daemon = valueOrNull(daemonResult);
    OffsetDateTime measuredAt = measuredAt(activeResult, peakResult, daemonResult);

    if (active != null && peak != null && daemon != null) {
      return success(active, peak, daemon, measuredAt);
    }
    if (active != null || peak != null || daemon != null) {
      return partial(active, peak, daemon, measuredAt, joinReasons(
          prefixedReason("ACTIVE_THREADS", activeResult),
          prefixedReason("PEAK_THREADS", peakResult),
          prefixedReason("DAEMON_THREADS", daemonResult)));
    }
    String combined = joinReasons(
        prefixedReason("ACTIVE_THREADS", activeResult),
        prefixedReason("PEAK_THREADS", peakResult),
        prefixedReason("DAEMON_THREADS", daemonResult));
    if (activeResult instanceof MappingResult.QueryFailed
        || peakResult instanceof MappingResult.QueryFailed
        || daemonResult instanceof MappingResult.QueryFailed) {
      return queryFailed(combined);
    }
    if (activeResult instanceof MappingResult.ParseFailed
        || peakResult instanceof MappingResult.ParseFailed
        || daemonResult instanceof MappingResult.ParseFailed
        || hasConversionFailure(activeResult, peakResult, daemonResult)) {
      return parseFailed(combined);
    }
    return emptyResult(combined);
  }

  public static ThreadMetricDto success(
      Integer activeCount, Integer peakCount, Integer daemonCount, OffsetDateTime measuredAt) {
    return new ThreadMetricDto(
        activeCount, peakCount, daemonCount, measuredAt, MetricStatus.SUCCESS, null);
  }

  public static ThreadMetricDto partial(
      Integer activeCount,
      Integer peakCount,
      Integer daemonCount,
      OffsetDateTime measuredAt,
      String missingReason) {
    return new ThreadMetricDto(
        activeCount, peakCount, daemonCount, measuredAt, MetricStatus.PARTIAL, missingReason);
  }

  public static ThreadMetricDto empty() {
    return emptyResult("empty result");
  }

  public static ThreadMetricDto emptyResult(String reason) {
    return new ThreadMetricDto(null, null, null, null, MetricStatus.EMPTY_RESULT, reason);
  }

  public static ThreadMetricDto queryFailed(String reason) {
    return new ThreadMetricDto(null, null, null, null, MetricStatus.QUERY_FAILED, reason);
  }

  public static ThreadMetricDto parseFailed(String reason) {
    return new ThreadMetricDto(null, null, null, null, MetricStatus.PARSE_FAILED, reason);
  }

  private static Integer valueOrNull(MappingResult result) {
    if (result instanceof MappingResult.Success s) {
      try {
        return s.point().value().intValueExact();
      } catch (ArithmeticException e) {
        return null;
      }
    }
    return null;
  }

  private static OffsetDateTime measuredAt(MappingResult... results) {
    for (MappingResult result : results) {
      if (result instanceof MappingResult.Success s) {
        return s.point().timestamp();
      }
    }
    return null;
  }

  private static String prefixedReason(String prefix, MappingResult result) {
    if (result instanceof MappingResult.Success s) {
      try {
        s.point().value().intValueExact();
        return null;
      } catch (ArithmeticException e) {
        return prefix + ": cannot convert to int: " + e.getMessage();
      }
    }
    if (result instanceof MappingResult.Empty) return prefix + ": empty result";
    if (result instanceof MappingResult.ParseFailed pf) return prefix + ": " + pf.reason();
    if (result instanceof MappingResult.QueryFailed qf) return prefix + ": " + qf.reason();
    return prefix + ": unknown";
  }

  private static boolean hasConversionFailure(MappingResult... results) {
    for (MappingResult result : results) {
      if (result instanceof MappingResult.Success s) {
        try {
          s.point().value().intValueExact();
        } catch (ArithmeticException e) {
          return true;
        }
      }
    }
    return false;
  }

  private static String joinReasons(String... reasons) {
    String joined = null;
    for (String reason : reasons) {
      if (reason == null) {
        continue;
      }
      joined = joined == null ? reason : joined + "; " + reason;
    }
    return joined;
  }
}
