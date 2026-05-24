package com.example.argus.service.metric.snapshot;

import com.example.argus.dto.metric.http.HttpErrorRateDto;
import com.example.argus.dto.metric.http.HttpMetricSnapshotDto;
import com.example.argus.dto.metric.http.HttpResponseTimeDto;
import com.example.argus.dto.metric.http.HttpThroughputDto;
import com.example.argus.dto.metric.SnapshotStatus;
import com.example.argus.exception.PrometheusQueryException;
import com.example.argus.service.PrometheusQueryService;
import com.example.argus.service.metric.MetricType;
import com.example.argus.service.metric.mapper.MetricPointMapper;
import com.example.argus.service.metric.mapper.MetricPointMapper.LabeledPoint;
import com.example.argus.service.metric.mapper.MetricPointMapper.MultiMappingResult;
import java.time.Clock;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Service
public class HttpMetricSnapshotAssembler {

    private static final String LABEL_URI = "uri";

    private static final Logger log = LoggerFactory.getLogger(HttpMetricSnapshotAssembler.class);

    private final PrometheusQueryService queryService;
    private final Clock clock;
    private final Executor fanoutExecutor;

    public HttpMetricSnapshotAssembler(
            PrometheusQueryService queryService,
            Clock clock,
            @Qualifier("metricFanoutExecutor") Executor fanoutExecutor) {
        this.queryService = queryService;
        this.clock = clock;
        this.fanoutExecutor = fanoutExecutor;
    }

    public HttpMetricSnapshotDto assemble() {
        long start = System.nanoTime();

        OffsetDateTime collectedAt = OffsetDateTime.now(clock);

        CompletableFuture<MultiMappingResult> fP99 =
                CompletableFuture.supplyAsync(() -> safeQuery(MetricType.HTTP_P99_RESPONSE_TIME), fanoutExecutor);
        CompletableFuture<MultiMappingResult> fRps =
                CompletableFuture.supplyAsync(() -> safeQuery(MetricType.HTTP_RPS), fanoutExecutor);
        CompletableFuture<MultiMappingResult> fErrorRate =
                CompletableFuture.supplyAsync(() -> safeQuery(MetricType.HTTP_ERROR_RATE), fanoutExecutor);

        CompletableFuture.allOf(fP99, fRps, fErrorRate).join();

        LabelAccumulator labels = new LabelAccumulator();

        // HTTP_P99_RESPONSE_TIME — application/instance 라벨 있음
        MultiMappingResult p99Result = fP99.join();
        if (p99Result instanceof MultiMappingResult.Success s) {
            s.points().forEach(labels::accept);
        }
        HttpResponseTimeDto p99 = HttpResponseTimeDto.from(p99Result);

        // HTTP_RPS — sum by (uri) 라 application/instance 라벨 없음 → 라벨 누적 대상 아님
        MultiMappingResult rpsResult = fRps.join();
        HttpThroughputDto rps = HttpThroughputDto.from(rpsResult);

        MultiMappingResult errorRateResult = fErrorRate.join();
        if (errorRateResult instanceof MultiMappingResult.Success s) {
            s.points().forEach(labels::accept);
        }
        HttpErrorRateDto errorRate = HttpErrorRateDto.from(errorRateResult);

        SnapshotStatus snapshotStatus =
                SnapshotStatus.from(List.of(p99.status(), rps.status(), errorRate.status()));

        log.info("metric-assemble: type=HTTP elapsedMs={}", (System.nanoTime() - start) / 1_000_000);

        return new HttpMetricSnapshotDto(
                labels.application(), labels.instance(), collectedAt, p99, rps, errorRate, snapshotStatus);
    }

    private MultiMappingResult safeQuery(MetricType type) {
        try {
            return MetricPointMapper.toPoints(queryService.queryByMetric(type), type, LABEL_URI);
        } catch (PrometheusQueryException e) {
            return new MultiMappingResult.QueryFailed(e.getMessage());
        } catch (Exception e) {
            log.error("Unexpected error while querying {}", type, e);
            return new MultiMappingResult.ParseFailed(e.getMessage());
        }
    }

    private static class LabelAccumulator {
        private String application;
        private String instance;

        void accept(LabeledPoint point) {
            if (application == null && point.application() != null) {
                application = point.application();
            }
            if (instance == null && point.instance() != null) {
                instance = point.instance();
            }
        }

        String application() {
            return application;
        }

        String instance() {
            return instance;
        }
    }
}
