package com.example.batch.common;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * @author Nikola Ivačič <nikola.ivacic@dropchop.com> on 30. 06. 2026.
 */
public interface BatchService {
  String DEFAULT_ACTION = "<<default>>";

  class ActionBuilder<P> {
    private final Class<P> payloadType;

    ActionBuilder(Class<P> payloadType) {
      if (payloadType == null) {
        throw new IllegalArgumentException("Payload type is required");
      }
      this.payloadType = payloadType;
    }

    public ActionDefinition<P> execute(List<Class<? extends BatchStep<P>>> stepTypes) {
      return new ActionDefinition<>(payloadType, stepTypes);
    }

    @SafeVarargs
    public final ActionDefinition<P> execute(Class<? extends BatchStep<P>>... stepTypes) {
      return execute(Arrays.asList(stepTypes));
    }
  }

  class ActionDefinition<P> {
    private final Class<P> payloadType;
    private final List<Class<? extends BatchStep<P>>> stepTypes;

    ActionDefinition(Class<P> payloadType, Collection<Class<? extends BatchStep<P>>> stepTypes) {
      if (stepTypes == null || stepTypes.isEmpty()) {
        throw new IllegalArgumentException("At least one step type is required");
      }
      this.payloadType = payloadType;
      this.stepTypes = List.copyOf(stepTypes);
    }

    Class<P> payloadType() {
      return payloadType;
    }

    List<Class<? extends BatchStep<P>>> stepTypes() {
      return stepTypes;
    }
  }

  static <P> ActionBuilder<P> with(Class<P> payloadType) {
    return new ActionBuilder<>(payloadType);
  }

  static <P> Actions byDefault(ActionDefinition<P> definition) {
    return new Actions().byDefault(definition);
  }

  @SuppressWarnings("unused")
  static <P> Actions on(String name, ActionDefinition<P> definition) {
    return new Actions().on(name, definition);
  }

  String getName();

  BatchStatus start();

  <P> boolean execute(BatchContext<P> context) throws Exception;

  BatchStatus stop();

  BatchStatus status();
}
