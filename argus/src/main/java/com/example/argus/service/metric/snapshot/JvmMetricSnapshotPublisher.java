package com.example.argus.service.metric.snapshot;

import com.example.argus.dto.metric.jvm.JvmMetricSnapshotDto;
import com.example.argus.messaging.JvmMetricSnapshotProducer;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class JvmMetricSnapshotPublisher {

  private final JvmMetricSnapshotAssembler assembler;
  private final JvmMetricSnapshotProducer producer;

  public CompletableFuture<SendResult<String, JvmMetricSnapshotDto>> publish() {
    JvmMetricSnapshotDto snapshot = assembler.assemble();
    String serviceId = snapshot.application() != null ? snapshot.application() : "unknown";
    return producer.send(serviceId, snapshot);
  }
}
