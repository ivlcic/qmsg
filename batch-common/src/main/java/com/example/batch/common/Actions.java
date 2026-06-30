package com.example.batch.common;

import static com.example.batch.common.BatchService.DEFAULT_ACTION;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;

public class Actions extends LinkedHashMap<String, Action<?>> {

  public <P> Actions byDefault(
      Class<P> payloadType,
      List<Class<? extends BatchStep<P>>> stepTypes) {
    return on(DEFAULT_ACTION, payloadType, stepTypes);
  }

  @SafeVarargs
  public final <P> Actions byDefault(
      Class<P> payloadType,
      Class<? extends BatchStep<P>>... stepTypes) {
    return byDefault(payloadType, Arrays.asList(stepTypes));
  }

  public <P> Actions on(
      String name,
      Class<P> payloadType,
      List<Class<? extends BatchStep<P>>> stepTypes) {
    Action<P> action = new Action<>(name, payloadType, stepTypes);
    put(action.name(), action);
    return this;
  }

  @SafeVarargs
  public final <P> Actions on(
      String name,
      Class<P> payloadType,
      Class<? extends BatchStep<P>>... stepTypes) {
    return on(name, payloadType, Arrays.asList(stepTypes));
  }
}
