package com.example.argus.service.metric;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class MetricTypeTest {

  @ParameterizedTest
  @EnumSource(MetricType.class)
  void getPromql_allTypes_returnsNonBlank(MetricType type) {
    assertThat(type.getPromql()).isNotBlank();
  }

  @Test
  void gcQueries_useOneMinuteWindowAndSnapshotLabels() {
    assertThat(MetricType.GC_AVG_DURATION.getPromql())
        .contains("sum by (application, instance)")
        .contains("increase(jvm_gc_pause_seconds_sum[1m])")
        .contains("increase(jvm_gc_pause_seconds_count[1m])");
    assertThat(MetricType.GC_COUNT.getPromql())
        .contains("sum by (application, instance)")
        .contains("increase(jvm_gc_pause_seconds_count[1m])");
  }

  @Test
  void activeThreadsQuery_usesCurrentGaugeValue() {
    assertThat(MetricType.ACTIVE_THREADS.getPromql())
        .isEqualTo("avg by (application, instance) (jvm_threads_live_threads)");
  }
}
