package com.example.batch.common;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.example.batch.common.BatchService.*;
import static com.example.batch.common.RetryPolicy.DEFAULT;
import static java.time.Duration.ofMillis;
import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Kristijan Sečan <kristijan.secan@dropchop.com> on 16. 07. 2026.
 */
@QuarkusTest
class ActionsTest {

  static class PayloadA {}

  static class StepOne implements BatchStep<PayloadA> {
    @Override
    public BatchResult execute(BatchContext<PayloadA> context) {
      return BatchStep.proceed();
    }
  }

  static class StepTwo implements BatchStep<PayloadA> {
    @Override
    public BatchResult execute(BatchContext<PayloadA> context) {
      return BatchStep.proceed();
    }
  }

  @Test
  void actionRequiresNameAndPayloadTypeAndSteps() {
    assertThrows(IllegalArgumentException.class,
        () -> new Action<>(null, PayloadA.class, List.of(StepOne.class)));
    assertThrows(IllegalArgumentException.class,
        () -> new Action<>("  ", PayloadA.class, List.of(StepOne.class)));
    assertThrows(IllegalArgumentException.class,
        () -> new Action<>("x", null, List.of(StepOne.class)));
    assertThrows(IllegalArgumentException.class,
        () -> new Action<>("x", PayloadA.class, List.of()));
  }

  @Test
  void actionIsEmptyUntilStepsAreResolved() {
    Action<PayloadA> action = new Action<>("x", PayloadA.class, List.of(StepOne.class));
    assertTrue(action.isEmpty());
    action.addStep(new StepOne());
    assertFalse(action.isEmpty());
  }

  @Test
  void actionIteratesResolvedStepsInOrder() {
    Action<PayloadA> action = new Action<>("x", PayloadA.class, List.of(StepOne.class, StepTwo.class));
    StepOne one = new StepOne();
    StepTwo two = new StepTwo();
    action.addStep(one);
    action.addStep(two);
    List<BatchStep<PayloadA>> resolved = List.of(one, two);
    int i = 0;
    for (BatchStep<PayloadA> step : action) {
      assertSame(resolved.get(i++), step);
    }
    assertEquals(2, i);
  }

  @Test
  void builderRequiresPayloadTypeAndAtLeastOneStep() {
    assertThrows(IllegalArgumentException.class, () -> with(null));
    assertThrows(IllegalArgumentException.class, () -> with(PayloadA.class).execute(List.of()));
  }

  @Test
  void byDefaultRegistersUnderInternalDefaultKey() {
    Actions actions = byDefault(with(PayloadA.class).execute(StepOne.class));
    assertEquals(1, actions.size());
    assertNotNull(actions.get(DEFAULT_ACTION));
    assertEquals(DEFAULT_ACTION, actions.get(DEFAULT_ACTION).getName());
  }

  @Test
  void onRegistersNamedActionsAndPreservesInsertionOrder() {
    Actions actions = byDefault(with(PayloadA.class).execute(StepOne.class))
        .on("archive", with(PayloadA.class).execute(StepOne.class, StepTwo.class))
        .on("delete", with(PayloadA.class).execute(StepTwo.class));
    assertEquals(List.of(DEFAULT_ACTION, "archive", "delete"), List.copyOf(actions.keySet()));
    assertEquals(PayloadA.class, actions.get("archive").getPayloadType());
    assertEquals(
        List.of(StepOne.class, StepTwo.class),
        actions.get("archive").getStepTypes()
    );
  }

  @Test
  void existingBuildersRetainTheDefaultRetrySettings() {
    Actions actions = byDefault(with(PayloadA.class).execute(StepOne.class));
    assertSame(DEFAULT, actions.getRetryPolicy());
    assertEquals(7, actions.getRetryPolicy().getMaxRetries());
    assertEquals(List.of(5000L, 15000L, 45000L, 120000L, 300000L, 900000L, 3600000L),
        actions.getRetryPolicy().getDelaysMs());
  }

  @Test
  void retryDefaultsCanStartAFluentActionDefinition() {
    RetryPolicy policy = RetryPolicy.builder().maxRetries(3).delays(ofMillis(100), ofMillis(200)).build();
    Actions actions = byDefault(with(PayloadA.class).execute(StepOne.class), policy)
        .on("archive", with(PayloadA.class).execute(StepTwo.class));
    assertSame(policy, actions.getRetryPolicy());
    assertEquals(3, actions.getRetryPolicy().getMaxRetries());
    assertEquals(List.of(100L, 200L), actions.getRetryPolicy().getDelaysMs());
    assertEquals(List.of(DEFAULT_ACTION, "archive"), List.copyOf(actions.keySet()));
  }

  @Test
  void retryDefaultsCanBeUpdatedWithoutReplacingActions() {
    Actions actions = byDefault(with(PayloadA.class).execute(StepOne.class));
    Action<?> original = actions.get(DEFAULT_ACTION);
    assertSame(actions, actions.withRetryPolicy(RetryPolicy.builder().maxRetries(0).build()));
    assertEquals(0, actions.getRetryPolicy().getMaxRetries());
    assertSame(original, actions.get(DEFAULT_ACTION));
    assertEquals(1, actions.size());
  }

  @Test
  void retryPolicyCanBeReusedAcrossActionDefinitions() {
    RetryPolicy policy = RetryPolicy.builder().maxRetries(2).delays(ofMillis(100), ofMillis(200)).build();
    Actions first = byDefault(with(PayloadA.class).execute(StepOne.class), policy);
    Actions second = byDefault(with(PayloadA.class).execute(StepTwo.class), policy);
    assertSame(policy, first.getRetryPolicy());
    assertSame(policy, second.getRetryPolicy());
    //noinspection DataFlowIssue
    assertThrows(UnsupportedOperationException.class, () -> policy.getDelaysMs().add(300L));
  }

  @Test
  void namedActionDefinitionsCanAlsoSetRetryDefaults() {
    Actions actions = on("archive", with(PayloadA.class).execute(StepOne.class))
        .withRetryPolicy(RetryPolicy.builder().maxRetries(1).delays(ofMillis(250)).build());
    assertEquals(1, actions.getRetryPolicy().getMaxRetries());
    assertEquals(List.of(250L), actions.getRetryPolicy().getDelaysMs());
    assertEquals(List.of("archive"), List.copyOf(actions.keySet()));
  }
}
