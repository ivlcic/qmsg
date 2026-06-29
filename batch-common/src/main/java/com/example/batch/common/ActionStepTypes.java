package com.example.batch.common;

import static com.example.batch.common.AbstractBatchService.DEFAULT_ACTION;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ActionStepTypes<P> extends LinkedHashMap<String, StepTypes<P>> {

  public static <X> ActionStepTypes<X> of(String action, List<Class<? extends BatchStep<X>>> steps) {
    ActionStepTypes<X> types = new ActionStepTypes<>();
    types.put(action, new StepTypes<>(steps));
    return types;
  }

  @SafeVarargs
  public static <X> ActionStepTypes<X> of(String action, Class<? extends BatchStep<X>>... steps) {
    return of(action, Arrays.asList(steps));
  }

  @SafeVarargs
  public static <X> ActionStepTypes<X> of(Map.Entry<String, List<Class<? extends BatchStep<X>>>>... entries) {
    ActionStepTypes<X> types = new ActionStepTypes<>();
    for (Map.Entry<String, List<Class<? extends BatchStep<X>>>> entry : entries) {
      types.put(entry.getKey(), new StepTypes<>(entry.getValue()));
    }
    return types;
  }

  public ActionStepTypes<P> on(String action, List<Class<? extends BatchStep<P>>> steps) {
    put(action, new StepTypes<>(steps));
    return this;
  }

  public ActionStepTypes<P> onDefault(List<Class<? extends BatchStep<P>>> steps) {
    return on(DEFAULT_ACTION, steps);
  }

  @SafeVarargs
  public final ActionStepTypes<P> on(String action, Class<? extends BatchStep<P>>... steps) {
    return on(action, Arrays.asList(steps));
  }

  @SafeVarargs
  public final ActionStepTypes<P> onDefault(Class<? extends BatchStep<P>>... steps) {
    return on(DEFAULT_ACTION, Arrays.asList(steps));
  }
}
