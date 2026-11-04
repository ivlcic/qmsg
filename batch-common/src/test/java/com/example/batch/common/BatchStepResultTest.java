package com.example.batch.common;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;
import org.slf4j.event.Level;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Kristijan Sečan <kristijan.secan@dropchop.com> on 16. 07. 2026.
 */
@QuarkusTest
class BatchStepResultTest {

  @Test
  void proceedHasContinueSemantics() {
    BatchStep.Result result = BatchStep.proceed();
    assertEquals(BatchStep.Result.Process.CONTINUE, result.getProcess());
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
  }

  @Test
  void stopWithMessageDefaultsToWarnLevel() {
    BatchStep.Result result = BatchStep.stop("halted");
    assertEquals(Level.WARN, result.getLogLevel());
    assertEquals("halted", result.getLogMessage());
  }

  @Test
  void retryHasExplicitRetrySemantics() {
    BatchStep.Result result = BatchStep.retry();
    assertEquals(BatchStep.Result.Process.RETRY, result.getProcess());
  }

  @Test
  void retryWithMessageDefaultsToWarnLevel() {
    BatchStep.Result result = BatchStep.retry("try again [{}]", "later");
    assertEquals(Level.WARN, result.getLogLevel());
    assertEquals(BatchStep.Result.Process.RETRY, result.getProcess());
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
    assertEquals(BatchStep.Result.Process.RETRY, result.getProcess());
  }

  @Test
  void failWithoutCauseHasExplicitFailSemantics() {
    BatchStep.Result result = BatchStep.fail();
    assertEquals(BatchStep.Result.Process.FAIL, result.getProcess());
    assertEquals(Level.ERROR, result.getLogLevel());
    assertNull(result.getCause());
  }

  @Test
  void failExtractsTrailingCauseAndPreservesFormattingArguments() {
    IllegalArgumentException failure = new IllegalArgumentException("invalid payload");
    BatchStep.Result result = BatchStep.fail("Invalid payload [{}]", "item-1", failure);
    assertEquals(BatchStep.Result.Process.FAIL, result.getProcess());
    assertEquals(Level.ERROR, result.getLogLevel());
    assertSame(failure, result.getCause());
    assertArrayEquals(new Object[]{"item-1"}, result.getLogArgs());
  }

  @Test
  void failerAllowsCustomizationBeforeBuilding() {
    IllegalArgumentException failure = new IllegalArgumentException("invalid payload");
    BatchStep.Result result = BatchStep.failer("Invalid payload [{}]", "item-1", failure)
        .logLevel(Level.WARN)
        .build();
    assertEquals(BatchStep.Result.Process.FAIL, result.getProcess());
    assertEquals(Level.WARN, result.getLogLevel());
    assertSame(failure, result.getCause());
    assertArrayEquals(new Object[]{"item-1"}, result.getLogArgs());
  }

  @Test
  void stopCauseDoesNotChangeItsOutcome() {
    BatchStep.Result result = BatchStep.stop("Stopping", new IllegalStateException("diagnostic"));
    assertEquals(BatchStep.Result.Process.STOP, result.getProcess());
    assertNotNull(result.getCause());
  }
}
