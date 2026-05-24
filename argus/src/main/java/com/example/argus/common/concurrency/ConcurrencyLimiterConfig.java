package com.example.argus.common.concurrency;

import com.example.argus.config.ConcurrencyLimitProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(ConcurrencyLimitProperties.class)
public class ConcurrencyLimiterConfig {

    @Bean("prometheusConcurrencyLimiter")
    public ConcurrencyLimiter prometheusConcurrencyLimiter(ConcurrencyLimitProperties p) {
        ConcurrencyLimitProperties.LimiterSpec s = p.getPrometheus();
        return new ConcurrencyLimiter("prometheus", s.getPermits(), s.getAcquireTimeout(), s.getMaxAttempts());
    }

    @Bean("kafkaPublishConcurrencyLimiter")
    public ConcurrencyLimiter kafkaPublishConcurrencyLimiter(ConcurrencyLimitProperties p) {
        ConcurrencyLimitProperties.LimiterSpec s = p.getKafkaPublish();
        return new ConcurrencyLimiter("kafka-publish", s.getPermits(), s.getAcquireTimeout(), s.getMaxAttempts());
    }
}
