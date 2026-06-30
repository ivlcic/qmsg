package com.example.batch.common;

import java.util.Arrays;
import java.util.List;

public interface BatchService {
  String DEFAULT_ACTION = "<<default>>";

  static <P> Actions byDefault(Class<P> payloadType, List<Class<? extends BatchStep<P>>> stepTypes) {
    return new Actions().byDefault(payloadType, stepTypes);
  }

  @SafeVarargs
  static <P> Actions byDefault(Class<P> payloadType, Class<? extends BatchStep<P>>... stepTypes) {
    return byDefault(payloadType, Arrays.asList(stepTypes));
  }

  static <P> Actions on(String name, Class<P> payloadType, List<Class<? extends BatchStep<P>>> stepTypes) {
    return new Actions().on(name, payloadType, stepTypes);
  }

  @SafeVarargs
  static <P> Actions on(String name, Class<P> payloadType, Class<? extends BatchStep<P>>... stepTypes) {
    return on(name, payloadType, Arrays.asList(stepTypes));
  }

  String getName();

  BatchStatus start();

  BatchStatus stop();

  BatchStatus status();
}
