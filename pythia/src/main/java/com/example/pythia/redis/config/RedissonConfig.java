package com.example.pythia.redis.config;

import java.time.Duration;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.redisson.config.SingleServerConfig;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

@Configuration
@EnableConfigurationProperties(PythiaRedisProperties.class)
public class RedissonConfig {

  @Bean(destroyMethod = "shutdown")
  @ConditionalOnMissingBean(RedissonClient.class)
  public RedissonClient redissonClient(PythiaRedisProperties redisProperties) {
    Config config = new Config();
    SingleServerConfig singleServerConfig = config.useSingleServer();

    singleServerConfig.setAddress(buildAddress(redisProperties));
    singleServerConfig.setDatabase(redisProperties.getDatabase());

    if (StringUtils.hasText(redisProperties.getUsername())) {
      singleServerConfig.setUsername(redisProperties.getUsername());
    }
    if (StringUtils.hasText(redisProperties.getPassword())) {
      singleServerConfig.setPassword(redisProperties.getPassword());
    }
    if (StringUtils.hasText(redisProperties.getClientName())) {
      singleServerConfig.setClientName(redisProperties.getClientName());
    }
    if (redisProperties.getTimeout() != null) {
      singleServerConfig.setTimeout(toMillis(redisProperties.getTimeout()));
    }
    if (redisProperties.getConnectTimeout() != null) {
      singleServerConfig.setConnectTimeout(toMillis(redisProperties.getConnectTimeout()));
    }

    return Redisson.create(config);
  }

  private String buildAddress(PythiaRedisProperties redisProperties) {
    if (StringUtils.hasText(redisProperties.getUrl())) {
      return redisProperties.getUrl();
    }

    String protocol = redisProperties.getSsl().isEnabled() ? "rediss" : "redis";
    return protocol + "://" + redisProperties.getHost() + ":" + redisProperties.getPort();
  }

  private int toMillis(Duration duration) {
    return Math.toIntExact(duration.toMillis());
  }
}
