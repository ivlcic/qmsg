package com.example.batch.common;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

/**
 * @author Nikola Ivačič <nikola.ivacic@dropchop.com> on 03. 06. 2026.
 */
public class StepTypes<P> implements Iterable<Class<? extends BatchStep<P>>> {

  private final List<Class<? extends BatchStep<P>>> steps;

  public StepTypes(List<Class<? extends BatchStep<P>>> steps) {
    this.steps = steps;
  }

  public List<Class<? extends BatchStep<P>>> get() {
    return steps;
  }

  public void addStep(Class<? extends BatchStep<P>> step) {
    this.steps.add(step);
  }

  @Override
  @SuppressWarnings("NullableProblems")
  public Iterator<Class<? extends BatchStep<P>>> iterator() {
    return steps.iterator();
  }

  public Stream<Class<? extends BatchStep<P>>> stream() {
    return steps.stream();
  }
}
