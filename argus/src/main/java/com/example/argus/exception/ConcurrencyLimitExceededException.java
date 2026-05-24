package com.example.argus.exception;

/*
 * 설계 이탈 사유: 현재 모듈에 공통 CustomException 상위 클래스가 없으므로 RuntimeException을 직접 상속한다.
 * PrometheusQueryException 과 동일 정책.
 */
public class ConcurrencyLimitExceededException extends RuntimeException {

    private final String limiterName;
    private final int attempts;
    private final long totalWaitedMillis;

    public ConcurrencyLimitExceededException(String limiterName, int attempts, long totalWaitedMillis) {
        super(String.format(
            "concurrency-limit: failed to acquire permit on [%s] after %d attempts (%dms)",
            limiterName, attempts, totalWaitedMillis));
        this.limiterName = limiterName;
        this.attempts = attempts;
        this.totalWaitedMillis = totalWaitedMillis;
    }

    public ConcurrencyLimitExceededException(
        String limiterName, int attempts, long totalWaitedMillis, Throwable cause) {
        super(
            String.format(
                "concurrency-limit: failed to acquire permit on [%s] after %d attempts (%dms)",
                limiterName, attempts, totalWaitedMillis),
            cause);
        this.limiterName = limiterName;
        this.attempts = attempts;
        this.totalWaitedMillis = totalWaitedMillis;
    }

    public String getLimiterName() {
        return limiterName;
    }

    public int getAttempts() {
        return attempts;
    }

    public long getTotalWaitedMillis() {
        return totalWaitedMillis;
    }
}
