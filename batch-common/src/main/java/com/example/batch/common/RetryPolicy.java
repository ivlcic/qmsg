package com.example.batch.common;

import lombok.Builder;
import lombok.Getter;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * @author Nikola Ivacic <nikola.ivacic@dropchop.com> on 05. 11. 2026.
 */
@Getter
public final class RetryPolicy {
  private static final int DEFAULT_MAX_RETRIES = 7;
  private static final List<Long> DEFAULT_DELAYS_MS =
      List.of(5000L, 15000L, 45000L, 120000L, 300000L, 900000L, 3600000L);

  private final int maxRetries;
  private final List<Long> delaysMs;

  @Builder()
  public RetryPolicy(int maxRetries, List<Long> delaysMs) {
    if (maxRetries < 0 || delaysMs == null || delaysMs.isEmpty()) {
      throw new IllegalArgumentException("Retry configuration requires a non-negative limit and positive delays");
    }
    for (Long delay : delaysMs) {
      if (delay == null || delay <= 0) {
        throw new IllegalArgumentException("Retry delays must be positive");
      }
    }
    this.maxRetries = maxRetries;
    this.delaysMs = List.copyOf(delaysMs);
  }

  @SuppressWarnings({"unused", "FieldMayBeFinal"})
  public static class RetryPolicyBuilder {
    private int maxRetries = DEFAULT_MAX_RETRIES;
    private List<Long> delaysMs = DEFAULT_DELAYS_MS;

    public RetryPolicyBuilder delays(Duration... delays) {
      if (delays == null || Arrays.stream(delays).anyMatch(Objects::isNull)) {
        throw new IllegalArgumentException("Retry delays are required");
      }
      return delaysMs(Arrays.stream(delays).map(Duration::toMillis).toList());
    }
  }

  public static final RetryPolicy DEFAULT = RetryPolicy.builder().build();

  public long delayMs(int retryNumber, long requestedDelayMs) {
    if (retryNumber <= 0 || requestedDelayMs < 0) {
      throw new IllegalArgumentException("Invalid retry number or delay");
    }
    return requestedDelayMs > 0 ? requestedDelayMs : delaysMs.get(Math.min(retryNumber - 1, delaysMs.size() - 1));
  }
}
