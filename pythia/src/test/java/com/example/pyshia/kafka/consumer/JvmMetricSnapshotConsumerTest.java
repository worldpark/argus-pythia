package com.example.pyshia.kafka.consumer;

import static org.mockito.Mockito.verify;

import com.example.pyshia.kafka.dto.MetricStatus;
import com.example.pyshia.kafka.dto.SnapshotStatus;
import com.example.pyshia.kafka.dto.jvm.CpuUsageDto;
import com.example.pyshia.kafka.dto.jvm.GcMetricDto;
import com.example.pyshia.kafka.dto.jvm.JvmMetricSnapshotDto;
import com.example.pyshia.kafka.dto.jvm.MemoryUsageDto;
import com.example.pyshia.kafka.dto.jvm.ThreadMetricDto;
import java.math.BigDecimal;
import java.time.OffsetDateTime;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class JvmMetricSnapshotConsumerTest {

  @Mock
  private JvmMetricSnapshotHandler handler;

  @InjectMocks
  private JvmMetricSnapshotConsumer consumer;

  @Test
  @DisplayName("정상 DTO 수신 시 handler.handle()이 호출된다")
  void 정상_DTO_수신시_handler_handle이_호출된다() {
    JvmMetricSnapshotDto snapshot = new JvmMetricSnapshotDto(
        "argus",
        "localhost:8080",
        OffsetDateTime.now(),
        new CpuUsageDto(BigDecimal.valueOf(50.0), OffsetDateTime.now(), MetricStatus.SUCCESS, null),
        new MemoryUsageDto(BigDecimal.valueOf(60.0),BigDecimal.valueOf(60.0), OffsetDateTime.now(), MetricStatus.SUCCESS, null),
        new GcMetricDto(BigDecimal.valueOf(0.01), BigDecimal.valueOf(5), OffsetDateTime.now(), MetricStatus.SUCCESS, null),
        new ThreadMetricDto(20,20,20, OffsetDateTime.now(), MetricStatus.SUCCESS, null),
        SnapshotStatus.COMPLETE
    );

    consumer.consume(snapshot, "argus:localhost:8080", "jvm.metrics.raw");

    verify(handler).handle(snapshot);
  }

  @Test
  @DisplayName("빈 결과(FAILED) DTO 수신 시에도 handler.handle()이 위임된다")
  void 빈결과_FAILED_DTO_수신시에도_handler_handle이_위임된다() {
    JvmMetricSnapshotDto snapshot = new JvmMetricSnapshotDto(
        "argus",
        "localhost:8080",
        OffsetDateTime.now(),
        null, null, null, null,
        SnapshotStatus.FAILED
    );

    consumer.consume(snapshot, "argus:localhost:8080", "jvm.metrics.raw");

    verify(handler).handle(snapshot);
  }
}
