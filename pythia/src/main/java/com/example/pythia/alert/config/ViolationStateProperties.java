package com.example.pythia.alert.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "pythia.alert.violation-state")
public class ViolationStateProperties {

  private Duration ttl = Duration.ofHours(1);
  private String keyPrefix = "pythia:alert:violation:";
  private String lockKeyPrefix = "pythia:alert:violation:lock:";
  private Duration lockWait = Duration.ofMillis(200);
  private Duration lockLease = Duration.ofMillis(3000);

  public Duration getTtl() {
    return ttl;
  }

  public void setTtl(Duration ttl) {
    this.ttl = ttl;
  }

  public String getKeyPrefix() {
    return keyPrefix;
  }

  public void setKeyPrefix(String keyPrefix) {
    this.keyPrefix = keyPrefix;
  }

  public String getLockKeyPrefix() {
    return lockKeyPrefix;
  }

  public void setLockKeyPrefix(String lockKeyPrefix) {
    this.lockKeyPrefix = lockKeyPrefix;
  }

  public Duration getLockWait() {
    return lockWait;
  }

  public void setLockWait(Duration lockWait) {
    this.lockWait = lockWait;
  }

  public Duration getLockLease() {
    return lockLease;
  }

  public void setLockLease(Duration lockLease) {
    this.lockLease = lockLease;
  }
}
