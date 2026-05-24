package com.example.argus.common.concurrency;

import com.example.argus.exception.ConcurrencyLimitExceededException;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ConcurrencyLimiter {

    private final String name;
    private final Semaphore semaphore;
    private final Duration acquireTimeout;
    private final int maxAttempts;

    public ConcurrencyLimiter(String name, int permits, Duration acquireTimeout, int maxAttempts) {
        this.name = name;
        this.semaphore = new Semaphore(permits, true);
        this.acquireTimeout = acquireTimeout;
        this.maxAttempts = maxAttempts;
    }

    /**
     * permit 을 획득한 뒤 action 을 실행하고 finally 에서 release 한다. (동기 경로 전용)
     */
    public <T> T execute(Callable<T> action) {
        acquireInternal();
        try {
            return action.call();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("Callable action failed with checked exception", e);
        } finally {
            release();
        }
    }

    /**
     * 비동기 release 시나리오용: permit 획득만 수행. 실패 시 ConcurrencyLimitExceededException.
     */
    public void acquire() {
        acquireInternal();
    }

    /**
     * acquire 와 짝을 이루는 release. 예외 완료된 future 의 whenComplete 에서도 호출 안전.
     */
    public void release() {
        semaphore.release();
    }

    public int availablePermits() {
        return semaphore.availablePermits();
    }

    private void acquireInternal() {
        long acquireTimeoutMs = acquireTimeout.toMillis();
        long totalWaited = 0L;

        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                boolean acquired = semaphore.tryAcquire(acquireTimeoutMs, TimeUnit.MILLISECONDS);
                if (acquired) {
                    return;
                }
                totalWaited += acquireTimeoutMs;
                log.debug(
                    "concurrency-limit: acquire attempt {}/{} failed on [{}], available={}",
                    attempt, maxAttempts, name, semaphore.availablePermits());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn(
                    "concurrency-limit: interrupted while acquiring permit on [{}] after {} attempt(s) ({}ms waited)",
                    name, attempt, totalWaited);
                throw new ConcurrencyLimitExceededException(name, attempt, totalWaited, e);
            }
        }

        log.warn(
            "concurrency-limit: failed to acquire permit on [{}] after {} attempts ({}ms total), available={}",
            name, maxAttempts, totalWaited, semaphore.availablePermits());
        throw new ConcurrencyLimitExceededException(name, maxAttempts, totalWaited);
    }
}
