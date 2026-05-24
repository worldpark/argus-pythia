package com.example.argus.config;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class VirtualThreadExecutorConfig {

    @Bean(name = "metricFanoutExecutor", destroyMethod = "close")
    public ExecutorService metricFanoutExecutor() {
        return Executors.newVirtualThreadPerTaskExecutor();
    }
}
