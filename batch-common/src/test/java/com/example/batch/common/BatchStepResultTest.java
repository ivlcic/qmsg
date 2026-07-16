package com.example.batch.common;

import org.junit.jupiter.api.Test;
import org.slf4j.event.Level;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Nikola Ivačič <nikola.ivacic@dropchop.com> on 16. 07. 2026.
 */
class BatchStepResultTest {

  @Test
  void proceedHasContinueSemanticsAndNoRetry() {
    BatchStep.Result result = BatchStep.proceed();
    assertEquals(BatchStep.Result.Process.CONTINUE, result.getProcess());
    assertFalse(result.isMessageRetry());
    assertEquals(0L, result.getMessageDeferMs());
    assertNull(result.getLogMessage());
    assertNull(result.getCause());
  }

  @Test
  void proceedWithMessageDefaultsToInfoLevel() {
    BatchStep.Result result = BatchStep.proceed("done [{}]", 42);
    assertEquals(BatchStep.Result.Process.CONTINUE, result.getProcess());
    assertEquals(Level.INFO, result.getLogLevel());
    assertEquals("done [{}]", result.getLogMessage());
    assertArrayEquals(new Object[]{42}, result.getLogArgs());
  }

  @Test
  void stopHasStopSemanticsWithoutRetry() {
    BatchStep.Result result = BatchStep.stop();
    assertEquals(BatchStep.Result.Process.STOP, result.getProcess());
    assertFalse(result.isMessageRetry());
  }

  @Test
  void stopWithMessageDefaultsToWarnLevel() {
    BatchStep.Result result = BatchStep.stop("halted");
    assertEquals(Level.WARN, result.getLogLevel());
    assertEquals("halted", result.getLogMessage());
  }

  @Test
  void retryHasStopSemanticsWithRetry() {
    BatchStep.Result result = BatchStep.retry();
    assertEquals(BatchStep.Result.Process.STOP, result.getProcess());
    assertTrue(result.isMessageRetry());
  }

  @Test
  void retryWithMessageDefaultsToWarnLevel() {
    BatchStep.Result result = BatchStep.retry("try again [{}]", "later");
    assertEquals(Level.WARN, result.getLogLevel());
    assertTrue(result.isMessageRetry());
  }

  @Test
  void trailingThrowableIsExtractedAsCauseAndStrippedFromArgs() {
    IllegalStateException failure = new IllegalStateException("boom");
    BatchStep.Result result = BatchStep.retry("failed [{}]", "item-1", failure);
    assertSame(failure, result.getCause());
    assertArrayEquals(new Object[]{"item-1"}, result.getLogArgs());
  }

  @Test
  void argsWithoutTrailingThrowableAreKeptIntactWithNullCause() {
    BatchStep.Result result = BatchStep.proceed("ok [{}] [{}]", "a", "b");
    assertNull(result.getCause());
    assertArrayEquals(new Object[]{"a", "b"}, result.getLogArgs());
  }

  @Test
  void builderAllowsExplicitLevelAndDefer() {
    BatchStep.Result result = BatchStep.retryer(Level.ERROR, "defer me")
        .messageDeferMs(5000L)
        .build();
    assertEquals(Level.ERROR, result.getLogLevel());
    assertEquals(5000L, result.getMessageDeferMs());
    assertTrue(result.isMessageRetry());
  }
}
