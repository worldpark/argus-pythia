package com.example.pythia.alert.exception;

import com.example.pythia.common.exception.ErrorCode;

public enum ThresholdConfigErrorCode implements ErrorCode {

  NON_MONOTONIC("THRESHOLD_001", "warning must be less than critical"),
  MISSING_THRESHOLD("THRESHOLD_002", "required threshold configuration is missing");

  private final String code;
  private final String defaultMessage;

  ThresholdConfigErrorCode(String code, String defaultMessage) {
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
