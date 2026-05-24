package com.example.argus.common.concurrency;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.example.argus.exception.ConcurrencyLimitExceededException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ConcurrencyLimiterTest {

    @Test
    @DisplayName("permit 정상 획득: action 실행 후 release 되어 availablePermits 가 원상복구된다")
    void execute_permitAcquired_releasedAfterAction() throws Exception {
        ConcurrencyLimiter limiter = new ConcurrencyLimiter("test", 1, Duration.ofMillis(50), 1);

        String result = limiter.execute(() -> "ok");

        assertThat(result).isEqualTo("ok");
        assertThat(limiter.availablePermits()).isEqualTo(1);
    }

    @Test
    @DisplayName("다른 스레드가 permit 점유 중일 때 재시도하여 획득 성공한다")
    void execute_permitTemporarilyHeld_retriesAndSucceeds() throws Exception {
        ConcurrencyLimiter limiter = new ConcurrencyLimiter("test", 1, Duration.ofMillis(200), 5);
        CountDownLatch holdLatch = new CountDownLatch(1);
        CountDownLatch releaseLatch = new CountDownLatch(1);

        // 다른 스레드에서 permit 점유 후 잠시 대기
        Thread holder = new Thread(() -> {
            limiter.acquire();
            holdLatch.countDown();
            try {
                releaseLatch.await(3, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                limiter.release();
            }
        });
        holder.start();
        holdLatch.await(2, TimeUnit.SECONDS);

        // 점유 스레드가 잡은 뒤 150ms 후 release
        Thread.ofVirtual().start(() -> {
            try {
                Thread.sleep(150);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            releaseLatch.countDown();
        });

        // 메인 스레드에서 재시도 중 획득 성공해야 한다
        String result = limiter.execute(() -> "ok-after-retry");
        assertThat(result).isEqualTo("ok-after-retry");
        holder.join(2000);
    }

    @Test
    @DisplayName("재시도 소진 시 ConcurrencyLimitExceededException 이 발생하고 메시지에 limiter 이름/시도횟수/누적대기ms 포함")
    void execute_allAttemptsExhausted_throwsConcurrencyLimitExceededException() {
        // permit=0 상태를 만들기 위해 외부에서 획득만 하고 release 하지 않음
        ConcurrencyLimiter limiter = new ConcurrencyLimiter("test-limiter", 1, Duration.ofMillis(10), 3);
        limiter.acquire(); // permit 고갈

        assertThatThrownBy(() -> limiter.execute(() -> "never"))
            .isInstanceOf(ConcurrencyLimitExceededException.class)
            .satisfies(ex -> {
                ConcurrencyLimitExceededException e = (ConcurrencyLimitExceededException) ex;
                assertThat(e.getLimiterName()).isEqualTo("test-limiter");
                assertThat(e.getAttempts()).isEqualTo(3);
                assertThat(e.getTotalWaitedMillis()).isGreaterThanOrEqualTo(30L);
                assertThat(e.getMessage()).contains("test-limiter");
                assertThat(e.getMessage()).contains("3");
            });
    }

    @Test
    @DisplayName("action 이 RuntimeException 을 던져도 permit 이 release 되어 availablePermits 가 초기치와 동일하다")
    void execute_actionThrowsRuntimeException_permitReleased() {
        ConcurrencyLimiter limiter = new ConcurrencyLimiter("test", 2, Duration.ofMillis(50), 1);

        assertThatThrownBy(() -> limiter.execute(() -> {
            throw new IllegalStateException("boom");
        })).isInstanceOf(IllegalStateException.class);

        assertThat(limiter.availablePermits()).isEqualTo(2);
    }

    @Test
    @DisplayName("InterruptedException 발생 시 Thread interrupt 상태 복원 후 ConcurrencyLimitExceededException 으로 변환")
    void acquire_interrupted_throwsConcurrencyLimitExceededExceptionAndRestoresInterruptFlag() {
        // permit=0 상태로 tryAcquire 가 blocking 되도록 설정 (긴 timeout)
        ConcurrencyLimiter limiter = new ConcurrencyLimiter("test-interrupted", 1, Duration.ofSeconds(10), 3);
        limiter.acquire(); // permit 고갈

        Thread[] resultHolder = new Thread[1];
        ConcurrencyLimitExceededException[] exceptionHolder = new ConcurrencyLimitExceededException[1];

        Thread testThread = new Thread(() -> {
            try {
                limiter.acquire();
            } catch (ConcurrencyLimitExceededException e) {
                exceptionHolder[0] = e;
            }
            resultHolder[0] = Thread.currentThread();
        });
        testThread.start();

        // tryAcquire 가 blocking 중일 때 interrupt
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        testThread.interrupt();

        try {
            testThread.join(3000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        assertThat(exceptionHolder[0]).isNotNull();
        assertThat(exceptionHolder[0]).isInstanceOf(ConcurrencyLimitExceededException.class);
        assertThat(exceptionHolder[0].getCause()).isInstanceOf(InterruptedException.class);
        // interrupt 상태가 복원되었는지 확인
        assertThat(testThread.isInterrupted()).isTrue();
    }

    @Test
    @DisplayName("동시 acquire/release 시 permit 카운트 invariant 유지: 종료 후 availablePermits 가 초기치와 동일하다")
    void concurrentAcquireRelease_permitCountInvariantMaintained() throws InterruptedException {
        int permits = 3;
        int threadCount = 10;
        ConcurrencyLimiter limiter = new ConcurrencyLimiter("test-concurrent", permits, Duration.ofMillis(200), 5);
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        List<Future<?>> futures = new ArrayList<>();

        for (int i = 0; i < threadCount; i++) {
            futures.add(executor.submit(() -> {
                try {
                    startLatch.await();
                    limiter.execute(() -> {
                        Thread.sleep(10);
                        return null;
                    });
                } catch (ConcurrencyLimitExceededException ignored) {
                    // 일부는 실패할 수 있음
                } catch (Exception e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            }));
        }

        startLatch.countDown();
        assertThat(doneLatch.await(10, TimeUnit.SECONDS)).isTrue();
        executor.shutdown();

        assertThat(limiter.availablePermits()).isEqualTo(permits);
    }

    @Test
    @DisplayName("가상 스레드 8개에서 동시 execute 시 permit(=2) 이 상한으로 동작하고, 종료 후 availablePermits 가 초기치와 동일하다")
    void acquireFromVirtualThreads_doesNotPin_andRespectsPermit() throws InterruptedException {
        int permits = 2;
        int virtualThreadCount = 8;
        ConcurrencyLimiter limiter = new ConcurrencyLimiter(
            "test-virtual", permits, Duration.ofMillis(500), 10);

        java.util.concurrent.atomic.AtomicInteger inFlight = new java.util.concurrent.atomic.AtomicInteger(0);
        java.util.concurrent.atomic.AtomicInteger peakInFlight = new java.util.concurrent.atomic.AtomicInteger(0);

        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(virtualThreadCount);
        ExecutorService virtualExecutor = Executors.newVirtualThreadPerTaskExecutor();

        for (int i = 0; i < virtualThreadCount; i++) {
            virtualExecutor.submit(() -> {
                try {
                    startLatch.await();
                    limiter.execute(() -> {
                        int current = inFlight.incrementAndGet();
                        int peak = peakInFlight.get();
                        while (current > peak && !peakInFlight.compareAndSet(peak, current)) {
                            peak = peakInFlight.get();
                        }
                        Thread.sleep(10);
                        inFlight.decrementAndGet();
                        return null;
                    });
                } catch (ConcurrencyLimitExceededException ignored) {
                    // 일부는 실패할 수 있음
                } catch (Exception e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertThat(doneLatch.await(10, TimeUnit.SECONDS)).isTrue();
        virtualExecutor.close();

        assertThat(peakInFlight.get()).isLessThanOrEqualTo(permits);
        assertThat(limiter.availablePermits()).isEqualTo(permits);
    }
}
