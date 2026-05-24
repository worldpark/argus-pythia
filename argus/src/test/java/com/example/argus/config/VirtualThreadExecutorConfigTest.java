package com.example.argus.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest(webEnvironment = WebEnvironment.NONE)
@ActiveProfiles("test")
class VirtualThreadExecutorConfigTest {

    @Autowired
    @Qualifier("metricFanoutExecutor")
    private ExecutorService metricFanoutExecutor;

    @Test
    void metricFanoutExecutor_beanIsRegistered() {
        assertThat(metricFanoutExecutor).isNotNull();
    }

    @Test
    void metricFanoutExecutor_isAutoCloseable() {
        assertThat(metricFanoutExecutor).isInstanceOf(AutoCloseable.class);
    }

    @Test
    void metricFanoutExecutor_taskRunsOnVirtualThread() throws Exception {
        AtomicBoolean isVirtual = new AtomicBoolean(false);
        AtomicReference<Throwable> error = new AtomicReference<>();

        metricFanoutExecutor.submit(() -> {
            try {
                isVirtual.set(Thread.currentThread().isVirtual());
            } catch (Throwable t) {
                error.set(t);
            }
        }).get();

        assertThat(error.get()).isNull();
        assertThat(isVirtual.get()).isTrue();
    }
}
