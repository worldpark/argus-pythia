package com.example.pythia.redis.config;

import java.time.Duration;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "spring.data.redis")
@Getter
@Setter
public class PythiaRedisProperties {

  private int database;
  private String url;
  private String host = "localhost";
  private String username;
  private String password;
  private int port = 6379;
  private Duration timeout;
  private Duration connectTimeout;
  private String clientName;
  private final Ssl ssl = new Ssl();

  @Getter
  @Setter
  public static class Ssl {

    private boolean enabled;
  }
}
