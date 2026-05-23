package com.example.pythia.alert.exception;

import com.example.pythia.common.exception.ErrorCode;

public enum ViolationStateErrorCode implements ErrorCode {

  REDIS_ACCESS_FAILED("ALERT-STATE-001", "Failed to access Redis for violation state"),
  LOCK_ACQUISITION_FAILED("ALERT-STATE-002", "Failed to acquire violation state lock"),
  LOCK_INTERRUPTED("ALERT-STATE-003", "Interrupted while acquiring violation state lock");

  private final String code;
  private final String defaultMessage;

  ViolationStateErrorCode(String code, String defaultMessage) {
    this.code = code;
    this.defaultMessage = defaultMessage;
  }

  @Override
  public String code() {
    return code;
  }

  @Override
  public String defaultMessage() {
    return defaultMessage;
  }
}
