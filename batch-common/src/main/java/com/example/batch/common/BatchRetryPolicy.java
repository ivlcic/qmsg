package com.example.batch.common;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.List;

@ApplicationScoped
public class BatchRetryPolicy {
  private final int maxRetries;
  private final List<Long> delaysMs;

  @Inject
  public BatchRetryPolicy(
      @ConfigProperty(name = "batch.retry.max-retries", defaultValue = "7") int maxRetries,
      @ConfigProperty(name = "batch.retry.delays-ms", defaultValue = "5000,15000,45000,120000,300000,900000,3600000")
      List<Long> delaysMs) {
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

  public int getMaxRetries() {
    return maxRetries;
  }

  public long delayMs(int retryNumber, long requestedDelayMs) {
    if (retryNumber <= 0 || requestedDelayMs < 0) {
      throw new IllegalArgumentException("Invalid retry number or delay");
    }
    return requestedDelayMs > 0 ? requestedDelayMs : delaysMs.get(Math.min(retryNumber - 1, delaysMs.size() - 1));
  }
}
