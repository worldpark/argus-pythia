package com.example.argus.service.metric.snapshot;

import com.example.argus.dto.metric.jvm.CpuUsageDto;
import com.example.argus.dto.metric.jvm.GcMetricDto;
import com.example.argus.dto.metric.jvm.JvmMetricSnapshotDto;
import com.example.argus.dto.metric.jvm.MemoryUsageDto;
import com.example.argus.dto.metric.jvm.MetricPointDto;
import com.example.argus.dto.metric.SnapshotStatus;
import com.example.argus.dto.metric.jvm.ThreadMetricDto;
import com.example.argus.exception.PrometheusQueryException;
import com.example.argus.service.PrometheusQueryService;
import com.example.argus.service.metric.MetricType;
import com.example.argus.service.metric.mapper.MetricPointMapper;
import com.example.argus.service.metric.mapper.MetricPointMapper.MappingResult;
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
public class JvmMetricSnapshotAssembler {

    private static final Logger log = LoggerFactory.getLogger(JvmMetricSnapshotAssembler.class);

    private final PrometheusQueryService queryService;
    private final Clock clock;
    private final Executor fanoutExecutor;

    public JvmMetricSnapshotAssembler(
            PrometheusQueryService queryService,
            Clock clock,
            @Qualifier("metricFanoutExecutor") Executor fanoutExecutor) {
        this.queryService = queryService;
        this.clock = clock;
        this.fanoutExecutor = fanoutExecutor;
    }

    public JvmMetricSnapshotDto assemble() {
        long start = System.nanoTime();

        OffsetDateTime collectedAt = OffsetDateTime.now(clock);

        CompletableFuture<MappingResult> fCpu =
                CompletableFuture.supplyAsync(() -> safeQuery(MetricType.CPU_USAGE), fanoutExecutor);
        CompletableFuture<MappingResult> fHeap =
                CompletableFuture.supplyAsync(() -> safeQuery(MetricType.HEAP_USAGE), fanoutExecutor);
        CompletableFuture<MappingResult> fOld =
                CompletableFuture.supplyAsync(() -> safeQuery(MetricType.HEAP_OLD_GEN_USAGE), fanoutExecutor);
        CompletableFuture<MappingResult> fGcDur =
                CompletableFuture.supplyAsync(() -> safeQuery(MetricType.GC_AVG_DURATION), fanoutExecutor);
        CompletableFuture<MappingResult> fGcCnt =
                CompletableFuture.supplyAsync(() -> safeQuery(MetricType.GC_COUNT), fanoutExecutor);
        CompletableFuture<MappingResult> fActive =
                CompletableFuture.supplyAsync(() -> safeQuery(MetricType.ACTIVE_THREADS), fanoutExecutor);
        CompletableFuture<MappingResult> fPeak =
                CompletableFuture.supplyAsync(() -> safeQuery(MetricType.PEAK_THREADS), fanoutExecutor);
        CompletableFuture<MappingResult> fDaemon =
                CompletableFuture.supplyAsync(() -> safeQuery(MetricType.DAEMON_THREADS), fanoutExecutor);

        CompletableFuture.allOf(fCpu, fHeap, fOld, fGcDur, fGcCnt, fActive, fPeak, fDaemon).join();

        LabelAccumulator labels = new LabelAccumulator();

        MappingResult cpuResult = fCpu.join();
        labels.accept(cpuResult);
        CpuUsageDto cpu = CpuUsageDto.from(cpuResult);

        MappingResult heapResult = fHeap.join();
        labels.accept(heapResult);
        MappingResult oldResult = fOld.join();
        labels.accept(oldResult);
        MemoryUsageDto memory = MemoryUsageDto.from(heapResult, oldResult);

        MappingResult gcDurResult = fGcDur.join();
        labels.accept(gcDurResult);
        MappingResult gcCntResult = fGcCnt.join();
        labels.accept(gcCntResult);
        GcMetricDto gc = GcMetricDto.from(gcDurResult, gcCntResult);

        MappingResult activeResult = fActive.join();
        labels.accept(activeResult);
        MappingResult peakResult = fPeak.join();
        labels.accept(peakResult);
        MappingResult daemonResult = fDaemon.join();
        labels.accept(daemonResult);
        ThreadMetricDto thread = ThreadMetricDto.from(activeResult, peakResult, daemonResult);

        SnapshotStatus snapshotStatus =
                SnapshotStatus.from(List.of(cpu.status(), memory.status(), gc.status(), thread.status()));

        log.info("metric-assemble: type=JVM elapsedMs={}", (System.nanoTime() - start) / 1_000_000);

        return new JvmMetricSnapshotDto(
                labels.application(), labels.instance(), collectedAt, cpu, memory, gc, thread, snapshotStatus);
    }

    private MappingResult safeQuery(MetricType type) {
        try {
            return MetricPointMapper.toPoint(queryService.queryByMetric(type), type);
        } catch (PrometheusQueryException e) {
            return new MappingResult.QueryFailed(e.getMessage());
        } catch (Exception e) {
            log.error("Unexpected error while querying {}", type, e);
            return new MappingResult.ParseFailed(e.getMessage());
        }
    }

    private static class LabelAccumulator {
        private String application;
        private String instance;

        void accept(MappingResult result) {
            if (result instanceof MappingResult.Success s) {
                accept(s.point());
            }
        }

        void accept(MetricPointDto point) {
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
