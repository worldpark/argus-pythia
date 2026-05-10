package com.example.argus.service.metric.snapshot;

import com.example.argus.dto.metric.http.HttpMetricSnapshotDto;
import com.example.argus.messaging.HttpMetricSnapshotProducer;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class HttpMetricSnapshotPublisher {

  private final HttpMetricSnapshotAssembler assembler;
  private final HttpMetricSnapshotProducer producer;

  public CompletableFuture<SendResult<String, HttpMetricSnapshotDto>> publish() {
    HttpMetricSnapshotDto snapshot = assembler.assemble();
    String serviceId = snapshot.application() != null ? snapshot.application() : "unknown";
    return producer.send(serviceId, snapshot);
  }
}
