package com.example.batch.common;

import static com.example.batch.common.AbstractBatchService.DEFAULT_ACTION;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;

public class Actions extends LinkedHashMap<String, Action<?>> {

  public static <P> Actions with(Action<P> action) {
    return new Actions().add(action);
  }

  public static <P> Actions on(
      String name,
      Class<P> payloadType,
      List<Class<? extends BatchStep<P>>> stepTypes) {
    return new Actions().add(new Action<>(name, payloadType, stepTypes));
  }

  @SafeVarargs
  public static <P> Actions on(
      String name,
      Class<P> payloadType,
      Class<? extends BatchStep<P>>... stepTypes) {
    return on(name, payloadType, Arrays.asList(stepTypes));
  }

  public static <P> Actions onDefault(
      Class<P> payloadType,
      List<Class<? extends BatchStep<P>>> stepTypes) {
    return on(DEFAULT_ACTION, payloadType, stepTypes);
  }

  @SafeVarargs
  public static <P> Actions onDefault(
      Class<P> payloadType,
      Class<? extends BatchStep<P>>... stepTypes) {
    return onDefault(payloadType, Arrays.asList(stepTypes));
  }

  public <P> Actions add(Action<P> action) {
    put(action.name(), action);
    return this;
  }

  public <P> Actions then(
      String name,
      Class<P> payloadType,
      List<Class<? extends BatchStep<P>>> stepTypes) {
    return add(new Action<>(name, payloadType, stepTypes));
  }

  @SafeVarargs
  public final <P> Actions then(
      String name,
      Class<P> payloadType,
      Class<? extends BatchStep<P>>... stepTypes) {
    return then(name, payloadType, Arrays.asList(stepTypes));
  }

  public <P> Actions thenDefault(
      Class<P> payloadType,
      List<Class<? extends BatchStep<P>>> stepTypes) {
    return then(DEFAULT_ACTION, payloadType, stepTypes);
  }

  @SafeVarargs
  public final <P> Actions thenDefault(
      Class<P> payloadType,
      Class<? extends BatchStep<P>>... stepTypes) {
    return thenDefault(payloadType, Arrays.asList(stepTypes));
  }
}
