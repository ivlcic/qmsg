package com.example.batch.common;

import org.junit.jupiter.api.Test;

import java.util.List;

import static com.example.batch.common.BatchService.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Nikola Ivačič <nikola.ivacic@dropchop.com> on 16. 07. 2026.
 */
class ActionsTest {

  static class PayloadA {}

  static class StepOne implements BatchStep<PayloadA> {
    @Override
    public Result execute(BatchContext<PayloadA> context) {
      return BatchStep.proceed();
    }
  }

  static class StepTwo implements BatchStep<PayloadA> {
    @Override
    public Result execute(BatchContext<PayloadA> context) {
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
        () -> new Action<PayloadA>("x", null, List.of(StepOne.class)));
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
}
