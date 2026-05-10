package com.example.pyshia.kafka.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.example.pyshia.alert.service.ThresholdEvaluator;
import com.example.pyshia.kafka.dto.MetricStatus;
import com.example.pyshia.kafka.dto.SnapshotStatus;
import com.example.pyshia.kafka.dto.http.HttpErrorRateDto;
import com.example.pyshia.kafka.dto.http.HttpMetricSnapshotDto;
import com.example.pyshia.kafka.dto.http.HttpResponseTimeDto;
import com.example.pyshia.kafka.dto.http.HttpThroughputDto;
import com.example.pyshia.metric.exception.MetricStoreErrorCode;
import com.example.pyshia.metric.exception.MetricStoreException;
import com.example.pyshia.metric.service.MetricStoreService;
import java.time.OffsetDateTime;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.LoggerFactory;

@ExtendWith(MockitoExtension.class)
class HttpMetricSnapshotHandlerTest {

    @Mock
    private ThresholdEvaluator evaluator;

    @Mock
    private MetricStoreService metricStoreService;

    @InjectMocks
    private HttpMetricSnapshotHandler handler;

    private ListAppender<ILoggingEvent> listAppender;
    private Logger logger;

    @BeforeEach
    void setUp() {
        logger = (Logger) LoggerFactory.getLogger(HttpMetricSnapshotHandler.class);
        listAppender = new ListAppender<>();
        listAppender.start();
        logger.addAppender(listAppender);
        logger.setLevel(Level.DEBUG);
    }

    @AfterEach
    void tearDown() {
        logger.detachAppender(listAppender);
    }

    @Test
    @DisplayName("정상 DTO 수신 시 evaluator.evaluateHttp()이 호출된다")
    void 정상_DTO_수신시_evaluator_evaluateHttp이_호출된다() {
        HttpMetricSnapshotDto snapshot = normalSnapshot();
        handler.handle(snapshot);
        verify(evaluator).evaluateHttp(snapshot);
    }

    @Test
    @DisplayName("정상 DTO 수신 시 metricStoreService.save()가 호출된다")
    void 정상_DTO_수신시_metricStoreService_save가_호출된다() {
        HttpMetricSnapshotDto snapshot = normalSnapshot();
        handler.handle(snapshot);
        verify(metricStoreService).save(snapshot);
    }

    @Test
    @DisplayName("MetricStoreException 발생 시 listener 예외 propagate 안 됨 (assertThatNoException)")
    void MetricStoreException_발생시_listener_예외_propagate_안됨() {
        HttpMetricSnapshotDto snapshot = normalSnapshot();
        doThrow(new MetricStoreException(MetricStoreErrorCode.METRIC_PERSIST_FAILED))
            .when(metricStoreService).save(any(HttpMetricSnapshotDto.class));

        assertThatNoException().isThrownBy(() -> handler.handle(snapshot));
    }

    @Test
    @DisplayName("MetricStoreException 발생 시 log.error가 발생한다")
    void MetricStoreException_발생시_log_error가_발생한다() {
        HttpMetricSnapshotDto snapshot = normalSnapshot();
        doThrow(new MetricStoreException(MetricStoreErrorCode.METRIC_PERSIST_FAILED))
            .when(metricStoreService).save(any(HttpMetricSnapshotDto.class));

        handler.handle(snapshot);

        assertThat(listAppender.list)
            .anyMatch(e -> e.getLevel() == Level.ERROR
                && e.getFormattedMessage().contains("metric persist failed"));
    }

    @Test
    @DisplayName("FAILED 상태 DTO 수신 시 evaluator가 호출되지 않는다")
    void FAILED_상태_DTO_수신시_evaluator_미호출() {
        HttpMetricSnapshotDto snapshot = failedSnapshot();
        handler.handle(snapshot);
        verify(evaluator, never()).evaluateHttp(any());
    }

    @Test
    @DisplayName("FAILED 상태 DTO 수신 시 metricStoreService.save()가 호출되지 않는다")
    void FAILED_상태_DTO_수신시_store_미호출() {
        HttpMetricSnapshotDto snapshot = failedSnapshot();
        handler.handle(snapshot);
        verify(metricStoreService, never()).save(any(HttpMetricSnapshotDto.class));
    }

    @Test
    @DisplayName("FAILED 상태 DTO 수신 시 warn 로그를 남긴다")
    void FAILED_상태_DTO_수신시_warn_로그를_남긴다() {
        handler.handle(failedSnapshot());
        assertThat(listAppender.list)
            .anyMatch(e -> e.getLevel() == Level.WARN && e.getFormattedMessage().contains("FAILED"));
    }

    private HttpMetricSnapshotDto normalSnapshot() {
        return new HttpMetricSnapshotDto(
            "argus", "localhost:8080", OffsetDateTime.now(),
            new HttpResponseTimeDto(List.of(), OffsetDateTime.now(), MetricStatus.SUCCESS, null),
            new HttpThroughputDto(List.of(), OffsetDateTime.now(), MetricStatus.SUCCESS, null),
            new HttpErrorRateDto(List.of(), OffsetDateTime.now(), MetricStatus.SUCCESS, null),
            SnapshotStatus.COMPLETE);
    }

    private HttpMetricSnapshotDto failedSnapshot() {
        return new HttpMetricSnapshotDto(
            "argus", "localhost:8080", OffsetDateTime.now(),
            null, null, null, SnapshotStatus.FAILED);
    }
}
