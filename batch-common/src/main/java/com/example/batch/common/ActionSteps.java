package com.example.batch.common;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;

import static com.example.batch.common.AbstractBatchService.DEFAULT_ACTION;

/**
 * @author Nikola Ivačič <nikola.ivacic@dropchop.com> on 03. 06. 2026.
 */
public class ActionSteps extends LinkedHashMap<String, Steps<?>> {

  @SuppressWarnings("unused")
  public static <X> ActionSteps of(String action, List<Class<? extends BatchStep<X>>> types) {
    ActionSteps actionSteps = new ActionSteps();
    actionSteps.on(action, types);
    return actionSteps;
  }

  public <X> ActionSteps on(String action, List<Class<? extends BatchStep<X>>> types) {
    put(action, new Steps<>(types));
    return this;
  }

  @SafeVarargs
  public final <X> ActionSteps on(String action, Class<? extends BatchStep<X>>... types) {
    put(action, new Steps<>(Arrays.asList(types)));
    return this;
  }

  @SuppressWarnings("unused")
  public <X> ActionSteps onDefault(List<Class<? extends BatchStep<X>>> types) {
    return on(DEFAULT_ACTION, types);
  }

  @SafeVarargs
  public final <X> ActionSteps onDefault(Class<? extends BatchStep<X>>... types) {
    return on(DEFAULT_ACTION, Arrays.asList(types));
  }
}
