package com.example.pythia.alert.config;

import java.time.Duration;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "pythia.alert.violation-state")
@Getter
@Setter
public class ViolationStateProperties {

  private Duration ttl = Duration.ofHours(1);
  private String keyPrefix = "pythia:alert:violation:";
  private String lockKeyPrefix = "pythia:alert:violation:lock:";
  private Duration lockWait = Duration.ofMillis(200);
  private Duration lockLease = Duration.ofMillis(3000);
}
