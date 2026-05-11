package com.example.pyshia.alert.exception;

import com.example.pyshia.common.exception.CustomException;

public class ThresholdConfigException extends CustomException {

  public ThresholdConfigException(ThresholdConfigErrorCode errorCode, String message) {
    super(errorCode, message);
  }
}
