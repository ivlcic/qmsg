package com.example.batch.common;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class Action<P> implements Iterable<BatchStep<P>> {
  private final String name;
  private final Class<P> payloadType;
  private final List<Class<? extends BatchStep<P>>> stepTypes;
  private final List<BatchStep<P>> steps = new ArrayList<>();

  public Action(String name, Class<P> payloadType, Collection<Class<? extends BatchStep<P>>> stepTypes) {
    if (name == null || name.isBlank()) {
      throw new IllegalArgumentException("Action name is required");
    }
    if (payloadType == null) {
      throw new IllegalArgumentException("Payload type is required for action " + name);
    }
    if (stepTypes == null || stepTypes.isEmpty()) {
      throw new IllegalArgumentException("At least one step type is required for action " + name);
    }
    this.name = name;
    this.payloadType = payloadType;
    this.stepTypes = List.copyOf(stepTypes);
  }

  public Action(String name, BatchService.ActionDefinition<P> definition) {
    this(name, definition.payloadType(), definition.stepTypes());
  }

  public String name() {
    return name;
  }

  public Class<P> payloadType() {
    return payloadType;
  }

  public List<Class<? extends BatchStep<P>>> stepTypes() {
    return stepTypes;
  }

  void addStep(BatchStep<P> step) {
    steps.add(step);
  }

  public boolean isEmpty() {
    return steps.isEmpty();
  }

  @Override
  public Iterator<BatchStep<P>> iterator() {
    return steps.iterator();
  }
}
