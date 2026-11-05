package com.example.batch.common;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class BatchRetryPolicyTest {
  @Test
  void configuredScheduleUsesTheFinalDelayForAdditionalAllowedRetries() {
    BatchRetryPolicy policy = new BatchRetryPolicy(5, List.of(5000L, 15000L, 45000L));
    assertEquals(5, policy.getMaxRetries());
    assertEquals(5000, policy.delayMs(1, 0));
    assertEquals(15000, policy.delayMs(2, 0));
    assertEquals(45000, policy.delayMs(3, 0));
    assertEquals(45000, policy.delayMs(5, 0));
  }

  @Test
  void explicitStepDelayIsUsedExactly() {
    BatchRetryPolicy policy = new BatchRetryPolicy(3, List.of(5000L));
    assertEquals(1234, policy.delayMs(2, 1234));
  }

  @Test
  void invalidConfigurationAndNegativeDelaysAreRejected() {
    assertThrows(IllegalArgumentException.class, () -> new BatchRetryPolicy(-1, List.of(5000L)));
    assertThrows(IllegalArgumentException.class, () -> new BatchRetryPolicy(1, List.of()));
    assertThrows(IllegalArgumentException.class, () -> new BatchRetryPolicy(1, List.of(0L)));
    BatchRetryPolicy policy = new BatchRetryPolicy(0, List.of(5000L));
    assertEquals(0, policy.getMaxRetries());
    assertThrows(IllegalArgumentException.class, () -> policy.delayMs(0, 5000));
    assertThrows(IllegalArgumentException.class, () -> policy.delayMs(1, -1));
  }
}
