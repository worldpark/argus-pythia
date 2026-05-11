package com.example.pythia.kafka.consumer;

import static org.mockito.Mockito.verify;

import com.example.pythia.kafka.dto.MetricStatus;
import com.example.pythia.kafka.dto.SnapshotStatus;
import com.example.pythia.kafka.dto.hikari.HikariActiveDto;
import com.example.pythia.kafka.dto.hikari.HikariMetricSnapshotDto;
import com.example.pythia.kafka.dto.hikari.HikariPendingDto;
import java.time.OffsetDateTime;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class HikariMetricSnapshotConsumerTest {

  @Mock
  private HikariMetricSnapshotHandler handler;

  @InjectMocks
  private HikariMetricSnapshotConsumer consumer;

  @Test
  @DisplayName("정상 DTO 수신 시 handler.handle()이 호출된다")
  void 정상_DTO_수신시_handler_handle이_호출된다() {
    HikariMetricSnapshotDto snapshot = new HikariMetricSnapshotDto(
        "argus",
        "localhost:8080",
        OffsetDateTime.now(),
        new HikariActiveDto(List.of(),List.of(), OffsetDateTime.now(), MetricStatus.SUCCESS, null),
        new HikariPendingDto(List.of(), OffsetDateTime.now(), MetricStatus.SUCCESS, null),
        SnapshotStatus.COMPLETE
    );

    consumer.consume(snapshot, "argus:localhost:8080", "hikari.metrics.raw");

    verify(handler).handle(snapshot);
  }

  @Test
  @DisplayName("빈 결과(FAILED) DTO 수신 시에도 handler.handle()이 위임된다")
  void 빈결과_FAILED_DTO_수신시에도_handler_handle이_위임된다() {
    HikariMetricSnapshotDto snapshot = new HikariMetricSnapshotDto(
        "argus",
        "localhost:8080",
        OffsetDateTime.now(),
        null, null,
        SnapshotStatus.FAILED
    );

    consumer.consume(snapshot, "argus:localhost:8080", "hikari.metrics.raw");

    verify(handler).handle(snapshot);
  }
}
