package com.example.argus.exception;

/*
 * 설계 이탈 사유: 현재 모듈에 공통 CustomException 상위 클래스가 없으므로 RuntimeException을 직접 상속한다.
 * 공통 기반 클래스 도입은 별도 Task에서 진행한다.
 */
public class PrometheusQueryException extends RuntimeException {

  public PrometheusQueryException(String message) {
    super(message);
  }

  public PrometheusQueryException(String message, Throwable cause) {
    super(message, cause);
  }
}
