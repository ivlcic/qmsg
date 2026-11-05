package com.example.batch.common;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static java.time.Duration.ofHours;
import static java.time.Duration.ofMinutes;
import static java.time.Duration.ofSeconds;
import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Nikola Ivacic <nikola.ivacic@dropchop.com> on 05. 11. 2026.
 */
class RetryPolicyTest {
  @Test
  void configuredScheduleUsesTheFinalDelayForAdditionalAllowedRetries() {
    RetryPolicy policy = new RetryPolicy(5, List.of(5000L, 15000L, 45000L));
    assertEquals(5, policy.getMaxRetries());
    assertEquals(5000, policy.delayMs(1, 0));
    assertEquals(15000, policy.delayMs(2, 0));
    assertEquals(45000, policy.delayMs(3, 0));
    assertEquals(45000, policy.delayMs(5, 0));
  }

  @Test
  void explicitStepDelayIsUsedExactly() {
    RetryPolicy policy = new RetryPolicy(3, List.of(5000L));
    assertEquals(1234, policy.delayMs(2, 1234));
  }

  @Test
  void invalidConfigurationAndNegativeDelaysAreRejected() {
    assertThrows(IllegalArgumentException.class, () -> new RetryPolicy(-1, List.of(5000L)));
    assertThrows(IllegalArgumentException.class, () -> new RetryPolicy(1, List.of()));
    assertThrows(IllegalArgumentException.class, () -> new RetryPolicy(1, List.of(0L)));
    RetryPolicy policy = new RetryPolicy(0, List.of(5000L));
    assertEquals(0, policy.getMaxRetries());
    assertThrows(IllegalArgumentException.class, () -> policy.delayMs(0, 5000));
    assertThrows(IllegalArgumentException.class, () -> policy.delayMs(1, -1));
  }

  @Test
  void namedBuilderAcceptsHumanReadableDurations() {
    RetryPolicy policy = RetryPolicy.builder().maxRetries(3)
        .delays(ofSeconds(5), ofMinutes(2), ofHours(1)).build();
    assertEquals(3, policy.getMaxRetries());
    assertEquals(List.of(5000L, 120000L, 3600000L), policy.getDelaysMs());
    assertEquals(3600000, policy.delayMs(3, 0));
  }

  @Test
  void builderDefaultsPreserveTheExistingRetryPolicy() {
    RetryPolicy policy = RetryPolicy.builder().build();
    assertEquals(7, policy.getMaxRetries());
    assertEquals(List.of(5000L, 15000L, 45000L, 120000L, 300000L, 900000L, 3600000L), policy.getDelaysMs());
    assertEquals(policy.getDelaysMs(), RetryPolicy.DEFAULT.getDelaysMs());
    assertEquals(7, RetryPolicy.DEFAULT.getMaxRetries());
  }

  @Test
  void maxRetriesAndDelaysCanBeCustomizedIndependently() {
    RetryPolicy noRetries = RetryPolicy.builder().maxRetries(0).build();
    assertEquals(0, noRetries.getMaxRetries());
    assertEquals(RetryPolicy.DEFAULT.getDelaysMs(), noRetries.getDelaysMs());
    RetryPolicy customDelays = RetryPolicy.builder().delays(ofSeconds(2)).build();
    assertEquals(7, customDelays.getMaxRetries());
    assertEquals(2000, customDelays.delayMs(1, 0));
  }

  @Test
  void schedulesAreDefensivelyCopiedAndImmutable() {
    List<Long> delays = new ArrayList<>(List.of(100L, 200L));
    RetryPolicy policy = RetryPolicy.builder().maxRetries(2).delaysMs(delays).build();
    delays.set(0, 999L);
    assertEquals(List.of(100L, 200L), policy.getDelaysMs());
    //noinspection DataFlowIssue
    assertThrows(UnsupportedOperationException.class, () -> policy.getDelaysMs().add(300L));
  }

  @Test
  void reusingABuilderDoesNotMutateAlreadyBuiltPolicies() {
    RetryPolicy.RetryPolicyBuilder builder = RetryPolicy.builder().maxRetries(2).delays(ofSeconds(5));
    RetryPolicy original = builder.build();
    RetryPolicy updated = builder.maxRetries(4).delays(ofMinutes(1)).build();
    assertEquals(2, original.getMaxRetries());
    assertEquals(List.of(5000L), original.getDelaysMs());
    assertEquals(4, updated.getMaxRetries());
    assertEquals(List.of(60000L), updated.getDelaysMs());
  }

  @Test
  void builderRejectsInvalidLimitsAndSchedules() {
    assertThrows(IllegalArgumentException.class, () -> RetryPolicy.builder().maxRetries(-1).build());
    assertThrows(IllegalArgumentException.class, () -> RetryPolicy.builder().delays().build());
    assertThrows(IllegalArgumentException.class, () -> RetryPolicy.builder().delays(Duration.ZERO).build());
    assertThrows(IllegalArgumentException.class, () -> RetryPolicy.builder().delays(ofSeconds(-1)).build());
    assertThrows(IllegalArgumentException.class, () -> RetryPolicy.builder().delays(Duration.ofNanos(1)).build());
    assertThrows(IllegalArgumentException.class, () -> RetryPolicy.builder().delays((Duration[]) null));
    assertThrows(IllegalArgumentException.class, () -> RetryPolicy.builder().delays((Duration) null));
    assertThrows(ArithmeticException.class, () -> RetryPolicy.builder().delays(ofSeconds(Long.MAX_VALUE)));
  }
}
