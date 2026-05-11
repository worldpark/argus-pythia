package com.example.pythia.alert.exception;

import com.example.pythia.common.exception.CustomException;

public class ThresholdConfigException extends CustomException {

  public ThresholdConfigException(ThresholdConfigErrorCode errorCode, String message) {
    super(errorCode, message);
  }
}
