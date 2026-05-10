package com.example.argus.service.metric;

public enum MetricType {
  CPU_USAGE(
      "avg by (application, instance) (avg_over_time(process_cpu_usage[1m]))"),
  HEAP_USAGE(
      "sum by (application, instance) (jvm_memory_used_bytes{area=\"heap\"})"
          + " / sum by (application, instance) (jvm_memory_max_bytes{area=\"heap\"}) * 100"),
  HEAP_OLD_GEN_USAGE(
      "sum by (application, instance) (jvm_memory_used_bytes{area=\"heap\", id=~\".*Old Gen|.*Tenured Gen\"})"
          + " / sum by (application, instance)"
          + " (jvm_memory_max_bytes{area=\"heap\", id=~\".*Old Gen|.*Tenured Gen\"}) * 100"),
  GC_AVG_DURATION(
      "sum by (application, instance) (increase(jvm_gc_pause_seconds_sum[1m]))"
          + " / clamp_min(sum by (application, instance)"
          + " (increase(jvm_gc_pause_seconds_count[1m])), 1)"),
  GC_COUNT(
      "sum by (application, instance) (increase(jvm_gc_pause_seconds_count[1m]))"),
  ACTIVE_THREADS(
      "avg by (application, instance) (jvm_threads_live_threads)"),
  PEAK_THREADS(
      "avg by (application, instance) (jvm_threads_peak_threads)"),
  DAEMON_THREADS(
      "avg by (application, instance) (jvm_threads_daemon_threads)"),
  HTTP_P99_RESPONSE_TIME(
      "histogram_quantile(0.99,"
          + " sum by (application, instance, uri, le)"
          + " (rate(http_server_requests_seconds_bucket[1m])))"),
  HTTP_RPS(
      "sum by (uri) (rate(http_server_requests_seconds_count[1m]))"),
  HTTP_ERROR_RATE(
      "sum by (application, instance, uri)"
          + " (rate(http_server_requests_seconds_count{status=~\"5..\"}[1m]))"
          + " / clamp_min(sum by (application, instance, uri)"
          + " (rate(http_server_requests_seconds_count[1m])), 1)"),
  HIKARI_ACTIVE_CONNECTIONS(
      "avg_over_time(hikaricp_connections_active[1m])"),
  HIKARI_USAGE_RATIO(
      "hikaricp_connections_active / clamp_min(hikaricp_connections_max, 1)"),
  HIKARI_PENDING_CONNECTIONS(
      "avg_over_time(hikaricp_connections_pending[1m])");

  private final String promql;

  MetricType(String promql) {
    this.promql = promql;
  }

  public String getPromql() {
    return promql;
  }
}
