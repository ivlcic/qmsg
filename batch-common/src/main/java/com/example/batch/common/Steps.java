package com.example.batch.common;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Nikola Ivačič <nikola.ivacic@dropchop.com> on 03. 06. 2026.
 */
public class Steps<P> implements Iterable<BatchStep<P>> {

  private final List<BatchStep<P>> steps = new LinkedList<>();
  private final List<Class<? extends BatchStep<P>>> types;

  public Steps(Collection<Class<? extends BatchStep<P>>> types) {
    this.types = new LinkedList<>(types);
  }

  void add(BatchStep<?> step) {
    //noinspection unchecked
    this.steps.add((BatchStep<P>) step);
  }

  public List<Class<? extends BatchStep<P>>> types() {
    return types;
  }

  public boolean isEmpty() {
    return steps.isEmpty();
  }

  @Override
  public Iterator<BatchStep<P>> iterator() {
    return steps.iterator();
  }
}
