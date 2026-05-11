package com.example.pyshia.common.exception;

public abstract class CustomException extends RuntimeException {

  private final ErrorCode errorCode;

  protected CustomException(ErrorCode errorCode, String message) {
    super(message);
    this.errorCode = errorCode;
  }

  protected CustomException(ErrorCode errorCode, String message, Throwable cause) {
    super(message, cause);
    this.errorCode = errorCode;
  }

  public ErrorCode getErrorCode() {
    return errorCode;
  }
}
