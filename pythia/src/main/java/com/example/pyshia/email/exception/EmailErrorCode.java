package com.example.pyshia.email.exception;

import com.example.pyshia.common.exception.ErrorCode;

public enum EmailErrorCode implements ErrorCode {

  INVALID_RECIPIENT("EMAIL_001", "Recipient list must not be empty"),
  INVALID_PAYLOAD("EMAIL_002", "Email subject or body must not be blank"),
  SMTP_FAILURE("EMAIL_003", "SMTP server communication failed");

  private final String code;
  private final String defaultMessage;

  EmailErrorCode(String code, String defaultMessage) {
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
