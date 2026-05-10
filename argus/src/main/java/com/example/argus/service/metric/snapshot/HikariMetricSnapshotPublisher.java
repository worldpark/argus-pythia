package com.example.argus.service.metric.snapshot;

import com.example.argus.dto.metric.hikari.HikariMetricSnapshotDto;
import com.example.argus.messaging.HikariMetricSnapshotProducer;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class HikariMetricSnapshotPublisher {

  private final HikariMetricSnapshotAssembler assembler;
  private final HikariMetricSnapshotProducer producer;

  public CompletableFuture<SendResult<String, HikariMetricSnapshotDto>> publish() {
    HikariMetricSnapshotDto snapshot = assembler.assemble();
    String serviceId = snapshot.application() != null ? snapshot.application() : "unknown";
    return producer.send(serviceId, snapshot);
  }
}
