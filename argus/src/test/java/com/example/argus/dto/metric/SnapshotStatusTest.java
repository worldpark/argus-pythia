package com.example.argus.dto.metric;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

class SnapshotStatusTest {

  @Test
  void from_allSuccess_returnsComplete() {
    assertThat(SnapshotStatus.from(List.of(
            MetricStatus.SUCCESS, MetricStatus.SUCCESS,
            MetricStatus.SUCCESS, MetricStatus.SUCCESS)))
        .isEqualTo(SnapshotStatus.COMPLETE);
  }

  @Test
  void from_allHardFailed_returnsFailed() {
    assertThat(SnapshotStatus.from(List.of(
            MetricStatus.QUERY_FAILED, MetricStatus.EMPTY_RESULT,
            MetricStatus.PARSE_FAILED, MetricStatus.QUERY_FAILED)))
        .isEqualTo(SnapshotStatus.FAILED);
  }

  @Test
  void from_mixedSuccessAndHardFailed_returnsPartial() {
    assertThat(SnapshotStatus.from(List.of(
            MetricStatus.SUCCESS, MetricStatus.QUERY_FAILED,
            MetricStatus.SUCCESS, MetricStatus.SUCCESS)))
        .isEqualTo(SnapshotStatus.PARTIAL);
  }

  @Test
  void from_successWithPartialAtEnd_returnsPartial() {
    assertThat(SnapshotStatus.from(List.of(
            MetricStatus.SUCCESS, MetricStatus.SUCCESS,
            MetricStatus.SUCCESS, MetricStatus.PARTIAL)))
        .isEqualTo(SnapshotStatus.PARTIAL);
  }

  @Test
  void from_gcPartialWithOthersSuccess_returnsPartial() {
    assertThat(SnapshotStatus.from(List.of(
            MetricStatus.SUCCESS, MetricStatus.SUCCESS,
            MetricStatus.PARTIAL, MetricStatus.SUCCESS)))
        .isEqualTo(SnapshotStatus.PARTIAL);
  }

  @Test
  void from_successPartialAndHardFail_returnsPartial() {
    assertThat(SnapshotStatus.from(List.of(
            MetricStatus.SUCCESS, MetricStatus.SUCCESS,
            MetricStatus.PARTIAL, MetricStatus.QUERY_FAILED)))
        .isEqualTo(SnapshotStatus.PARTIAL);
  }

  // PARTIAL은 일부 데이터가 있는 상태이므로 전부 PARTIAL이어도 FAILED가 아닌 PARTIAL
  @Test
  void from_allPartial_returnsPartial() {
    assertThat(SnapshotStatus.from(List.of(
            MetricStatus.PARTIAL, MetricStatus.PARTIAL,
            MetricStatus.PARTIAL, MetricStatus.PARTIAL)))
        .isEqualTo(SnapshotStatus.PARTIAL);
  }

  @Test
  void from_partialWithHardFails_returnsPartial() {
    assertThat(SnapshotStatus.from(List.of(
            MetricStatus.PARTIAL, MetricStatus.QUERY_FAILED,
            MetricStatus.QUERY_FAILED, MetricStatus.QUERY_FAILED)))
        .isEqualTo(SnapshotStatus.PARTIAL);
  }
}
