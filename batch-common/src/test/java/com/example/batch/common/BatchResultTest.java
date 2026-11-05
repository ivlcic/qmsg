package com.example.batch.common;

import org.junit.jupiter.api.Test;
import org.slf4j.event.Level;

import java.util.List;

import static com.example.batch.common.BatchResult.failer;
import static com.example.batch.common.BatchResult.proceeder;
import static com.example.batch.common.BatchResult.retryer;
import static com.example.batch.common.BatchResult.stopper;
import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Kristijan Sečan <kristijan.secan@dropchop.com> on 16. 07. 2026.
 */
class BatchResultTest {

  @Test
  void proceedHasContinueSemantics() {
    BatchResult result = BatchStep.proceed();
    assertEquals(BatchResult.Process.CONTINUE, result.getProcess());
    assertEquals(0L, result.getMessageDeferMs());
    assertNull(result.getLogMessage());
    assertNull(result.getCause());
  }

  @Test
  void proceedWithMessageDefaultsToInfoLevel() {
    BatchResult result = BatchStep.proceed("done [{}]", 42);
    assertEquals(BatchResult.Process.CONTINUE, result.getProcess());
    assertEquals(Level.INFO, result.getLogLevel());
    assertEquals("done [{}]", result.getLogMessage());
    assertArrayEquals(new Object[]{42}, result.getLogArgs());
  }

  @Test
  void stopHasStopSemanticsWithoutRetry() {
    BatchResult result = BatchStep.stop();
    assertEquals(BatchResult.Process.STOP, result.getProcess());
  }

  @Test
  void stopWithMessageDefaultsToWarnLevel() {
    BatchResult result = BatchStep.stop("halted");
    assertEquals(Level.WARN, result.getLogLevel());
    assertEquals("halted", result.getLogMessage());
  }

  @Test
  void retryHasExplicitRetrySemantics() {
    BatchResult result = BatchStep.retry();
    assertEquals(BatchResult.Process.RETRY, result.getProcess());
  }

  @Test
  void retryWithMessageDefaultsToWarnLevel() {
    BatchResult result = BatchStep.retry("try again [{}]", "later");
    assertEquals(Level.WARN, result.getLogLevel());
    assertEquals(BatchResult.Process.RETRY, result.getProcess());
  }

  @Test
  void trailingThrowableIsExtractedAsCauseAndStrippedFromArgs() {
    IllegalStateException failure = new IllegalStateException("boom");
    BatchResult result = BatchStep.retry("failed [{}]", "item-1", failure);
    assertSame(failure, result.getCause());
    assertArrayEquals(new Object[]{"item-1"}, result.getLogArgs());
  }

  @Test
  void argsWithoutTrailingThrowableAreKeptIntactWithNullCause() {
    BatchResult result = BatchStep.proceed("ok [{}] [{}]", "a", "b");
    assertNull(result.getCause());
    assertArrayEquals(new Object[]{"a", "b"}, result.getLogArgs());
  }

  @Test
  void builderAllowsExplicitLevelAndDefer() {
    BatchResult result = retryer(Level.ERROR, "defer me")
        .messageDeferMs(5000L)
        .build();
    assertEquals(Level.ERROR, result.getLogLevel());
    assertEquals(5000L, result.getMessageDeferMs());
    assertEquals(BatchResult.Process.RETRY, result.getProcess());
  }

  @Test
  void failWithoutCauseHasExplicitFailSemantics() {
    BatchResult result = BatchStep.fail();
    assertEquals(BatchResult.Process.FAIL, result.getProcess());
    assertEquals(Level.ERROR, result.getLogLevel());
    assertNull(result.getCause());
  }

  @Test
  void failExtractsTrailingCauseAndPreservesFormattingArguments() {
    IllegalArgumentException failure = new IllegalArgumentException("invalid payload");
    BatchResult result = BatchStep.fail("Invalid payload [{}]", "item-1", failure);
    assertEquals(BatchResult.Process.FAIL, result.getProcess());
    assertEquals(Level.ERROR, result.getLogLevel());
    assertSame(failure, result.getCause());
    assertArrayEquals(new Object[]{"item-1"}, result.getLogArgs());
  }

  @Test
  void failerAllowsCustomizationBeforeBuilding() {
    IllegalArgumentException failure = new IllegalArgumentException("invalid payload");
    BatchResult result = failer("Invalid payload [{}]", "item-1", failure)
        .logLevel(Level.WARN)
        .build();
    assertEquals(BatchResult.Process.FAIL, result.getProcess());
    assertEquals(Level.WARN, result.getLogLevel());
    assertSame(failure, result.getCause());
    assertArrayEquals(new Object[]{"item-1"}, result.getLogArgs());
  }

  @Test
  void stopCauseDoesNotChangeItsOutcome() {
    BatchResult result = BatchStep.stop("Stopping", new IllegalStateException("diagnostic"));
    assertEquals(BatchResult.Process.STOP, result.getProcess());
    assertNotNull(result.getCause());
  }

  @Test
  void directBuilderRetainsContinueAndZeroDeferDefaults() {
    BatchResult result = BatchResult.builder().build();
    assertEquals(BatchResult.Process.CONTINUE, result.getProcess());
    assertEquals(0, result.getMessageDeferMs());
    assertNull(result.getCause());
  }

  @Test
  void builderShortcutsHaveTheSameDefaultOutcomesAndLevelsAsStepShortcuts() {
    List<BatchResult> results = List.of(proceeder().build(), stopper().build(), retryer().build(), failer().build());
    assertEquals(List.of(BatchResult.Process.CONTINUE, BatchResult.Process.STOP,
            BatchResult.Process.RETRY, BatchResult.Process.FAIL),
        results.stream().map(BatchResult::getProcess).toList());
    assertEquals(List.of(Level.INFO, Level.WARN, Level.WARN, Level.ERROR),
        results.stream().map(BatchResult::getLogLevel).toList());
  }

  @Test
  void everyBuilderShortcutSupportsCustomLoggingAndPreservesCallerArguments() {
    IllegalArgumentException failure = new IllegalArgumentException("invalid");
    Object[] args = {"item-1", failure};
    List<BatchResult> results = List.of(
        proceeder(Level.DEBUG, "item [{}]", args).build(),
        stopper(Level.DEBUG, "item [{}]", args).build(),
        retryer(Level.DEBUG, "item [{}]", args).build(),
        failer(Level.DEBUG, "item [{}]", args).build());
    for (BatchResult result : results) {
      assertEquals(Level.DEBUG, result.getLogLevel());
      assertEquals("item [{}]", result.getLogMessage());
      assertSame(failure, result.getCause());
      assertArrayEquals(new Object[]{"item-1"}, result.getLogArgs());
    }
    assertEquals(2, args.length);
    assertSame(failure, args[1]);
  }

  @Test
  void builderShortcutAcceptsNullFormattingArguments() {
    BatchResult result = failer("invalid", (Object[]) null).build();
    assertNull(result.getLogArgs());
    assertNull(result.getCause());
  }

  @Test
  void nonTrailingThrowableRemainsAFormattingArgument() {
    IllegalStateException diagnostic = new IllegalStateException("diagnostic");
    BatchResult result = proceeder("arguments [{}] [{}]", diagnostic, "last").build();
    assertNull(result.getCause());
    assertArrayEquals(new Object[]{diagnostic, "last"}, result.getLogArgs());
  }
}
