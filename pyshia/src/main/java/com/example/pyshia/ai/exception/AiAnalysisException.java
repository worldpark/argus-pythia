package com.example.pyshia.ai.exception;

import com.example.pyshia.common.exception.CustomException;

public class AiAnalysisException extends CustomException {

  public AiAnalysisException(AiErrorCode errorCode, String message) {
    super(errorCode, message);
  }

  public AiAnalysisException(AiErrorCode errorCode, String message, Throwable cause) {
    super(errorCode, message, cause);
  }
}
