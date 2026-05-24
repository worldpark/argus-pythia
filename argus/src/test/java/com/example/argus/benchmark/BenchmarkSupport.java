package com.example.argus.benchmark;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

final class BenchmarkSupport {

  private static final long MAX_WAIT_MINUTES = 5L;

  private BenchmarkSupport() {}

  static void requireOptIn() {
    assumeTrue(
        Boolean.getBoolean("benchmarkTests"),
        "Manual benchmark skipped. Re-run with -DbenchmarkTests=true");
  }

  static BenchmarkResult runScenario(
      String scenario,
      String executionModel,
      int taskCount,
      int concurrency,
      Supplier<ExecutorService> executorSupplier,
      ThrowingRunnable task)
      throws InterruptedException {

    List<Long> durationsNanos = Collections.synchronizedList(new ArrayList<>());
    AtomicInteger failureCount = new AtomicInteger();
    AtomicInteger timeoutCount = new AtomicInteger();
    Semaphore gate = new Semaphore(concurrency);
    java.util.concurrent.CountDownLatch done = new java.util.concurrent.CountDownLatch(taskCount);

    long totalStart = System.nanoTime();

    try (ExecutorService executor = executorSupplier.get()) {
      for (int i = 0; i < taskCount; i++) {
        executor.submit(
            () -> {
              try {
                gate.acquire();

                long taskStart = System.nanoTime();
                try {
                  task.run();
                } catch (Exception e) {
                  failureCount.incrementAndGet();
                  if (isTimeout(e)) {
                    timeoutCount.incrementAndGet();
                  }
                } finally {
                  durationsNanos.add(System.nanoTime() - taskStart);
                  gate.release();
                }
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                failureCount.incrementAndGet();
              } finally {
                done.countDown();
              }
            });
      }

      boolean completed = done.await(MAX_WAIT_MINUTES, TimeUnit.MINUTES);
      if (!completed) {
        throw new IllegalStateException("Benchmark scenario timed out: " + scenario);
      }
    }

    long totalElapsed = System.nanoTime() - totalStart;
    return BenchmarkResult.from(
        scenario,
        executionModel,
        taskCount,
        concurrency,
        totalElapsed,
        durationsNanos,
        failureCount.get(),
        timeoutCount.get());
  }

  static void printResult(BenchmarkResult result) {
    System.out.println(result.toSummaryLine());
  }

  static void printComparison(BenchmarkResult left, BenchmarkResult right) {
    System.out.println(
        "comparison scenario="
            + left.scenario()
            + " left="
            + left.executionModel()
            + " totalMs="
            + left.totalMs()
            + " right="
            + right.executionModel()
            + " totalMs="
            + right.totalMs()
            + " deltaMs="
            + (right.totalMs() - left.totalMs()));
  }

  private static boolean isTimeout(Throwable throwable) {
    Throwable current = throwable;
    while (current != null) {
      String name = current.getClass().getName();
      if (name.contains("Timeout")) {
        return true;
      }
      current = current.getCause();
    }
    return false;
  }

  @FunctionalInterface
  interface ThrowingRunnable {
    void run() throws Exception;
  }

  record BenchmarkResult(
      String scenario,
      String executionModel,
      int taskCount,
      int concurrency,
      long totalMs,
      double avgMs,
      long minMs,
      long p95Ms,
      long p99Ms,
      long maxMs,
      int failures,
      int timeouts) {

    static BenchmarkResult from(
        String scenario,
        String executionModel,
        int taskCount,
        int concurrency,
        long totalElapsedNanos,
        List<Long> durationsNanos,
        int failures,
        int timeouts) {

      List<Long> millis = new ArrayList<>(durationsNanos.size());
      for (Long nanos : durationsNanos) {
        millis.add(TimeUnit.NANOSECONDS.toMillis(nanos));
      }
      Collections.sort(millis);

      long totalMs = TimeUnit.NANOSECONDS.toMillis(totalElapsedNanos);
      double avgMs =
          millis.stream().mapToLong(Long::longValue).average().orElse(0.0d);

      long minMs = millis.isEmpty() ? 0L : millis.get(0);
      long p95Ms = percentile(millis, 95);
      long p99Ms = percentile(millis, 99);
      long maxMs = millis.isEmpty() ? 0L : millis.get(millis.size() - 1);

      return new BenchmarkResult(
          scenario,
          executionModel,
          taskCount,
          concurrency,
          totalMs,
          avgMs,
          minMs,
          p95Ms,
          p99Ms,
          maxMs,
          failures,
          timeouts);
    }

    String toSummaryLine() {
      return "scenario="
          + scenario
          + " executor="
          + executionModel
          + " tasks="
          + taskCount
          + " concurrency="
          + concurrency
          + " totalMs="
          + totalMs
          + " avgMs="
          + Math.round(avgMs)
          + " minMs="
          + minMs
          + " p95Ms="
          + p95Ms
          + " p99Ms="
          + p99Ms
          + " maxMs="
          + maxMs
          + " failures="
          + failures
          + " timeouts="
          + timeouts;
    }

    private static long percentile(List<Long> values, int percentile) {
      if (values.isEmpty()) {
        return 0L;
      }
      int index = (int) Math.ceil((percentile / 100.0d) * values.size()) - 1;
      index = Math.max(0, Math.min(index, values.size() - 1));
      return values.get(index);
    }
  }
}
