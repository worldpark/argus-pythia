package com.example.pythia.alert.exception;

/**
 * Resilience4j Retry 트리거 전용 sentinel 예외.
 *
 * <p>ViolationStateStore.tryAcquireLock 에서 Redisson lock 획득에 실패했을 때(tryLock false 반환)
 * throw 되며, Resilience4j 가 이를 감지하여 재시도를 수행한다. 외부로 노출되지 않으며,
 * retry 모두 소진 시 ViolationStateException(LOCK_ACQUISITION_FAILED)으로 변환된다.
 */
public class LockAcquisitionRetryException extends RuntimeException {

  public LockAcquisitionRetryException() {
    super("Lock acquisition returned false");
  }
}
