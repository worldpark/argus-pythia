package com.example.pythia.kafka.config;

import java.time.Duration;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "pythia.kafka.dedup")
@Getter
@Setter
public class KafkaDedupProperties {

  private Duration ttl = Duration.ofHours(24);
}
