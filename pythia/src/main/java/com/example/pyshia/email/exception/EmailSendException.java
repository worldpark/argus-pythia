package com.example.pyshia.email.exception;

import com.example.pyshia.common.exception.CustomException;

public class EmailSendException extends CustomException {

  public EmailSendException(EmailErrorCode errorCode, String message) {
    super(errorCode, message);
  }

  public EmailSendException(EmailErrorCode errorCode, String message, Throwable cause) {
    super(errorCode, message, cause);
  }
}
