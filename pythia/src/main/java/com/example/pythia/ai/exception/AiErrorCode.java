package com.example.pythia.ai.exception;

import com.example.pythia.common.exception.ErrorCode;

public enum AiErrorCode implements ErrorCode {

  INVALID_REQUEST("AI_001", "Analysis request payload is invalid"),
  LLM_CALL_FAILURE("AI_002", "LLM call failed"),
  EMPTY_RESPONSE("AI_003", "LLM returned empty response");

  private final String code;
  private final String defaultMessage;

  AiErrorCode(String code, String defaultMessage) {
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
