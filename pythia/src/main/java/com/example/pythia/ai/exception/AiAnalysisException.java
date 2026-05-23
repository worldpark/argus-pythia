package com.example.pythia.ai.exception;

import com.example.pythia.common.exception.CustomException;

public class AiAnalysisException extends CustomException {

  public AiAnalysisException(AiErrorCode errorCode) {
    super(errorCode, errorCode.defaultMessage());
  }

  public AiAnalysisException(AiErrorCode errorCode, String message) {
    super(errorCode, message);
  }

  public AiAnalysisException(AiErrorCode errorCode, String message, Throwable cause) {
    super(errorCode, message, cause);
  }
}
