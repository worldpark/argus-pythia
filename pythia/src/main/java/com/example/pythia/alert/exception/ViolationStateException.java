package com.example.pythia.alert.exception;

import com.example.pythia.common.exception.CustomException;

public class ViolationStateException extends CustomException {

  public ViolationStateException(ViolationStateErrorCode errorCode) {
    super(errorCode, errorCode.defaultMessage());
  }

  public ViolationStateException(ViolationStateErrorCode errorCode, Throwable cause) {
    super(errorCode, errorCode.defaultMessage(), cause);
  }
}
