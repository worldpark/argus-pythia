package com.example.argus.service.metric.buffer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "argus.scheduling.enabled", havingValue = "true", matchIfMissing = true)
@RequiredArgsConstructor
@Slf4j
public class MetricBufferDrainScheduler {

    private final MetricBufferDrainService drainService;

    @Scheduled(fixedDelayString = "${argus.buffer.drain-interval-ms}")
    public void drain() {
        log.debug("metric-buffer: starting drain cycle");
        drainService.drainAll();
    }
}
