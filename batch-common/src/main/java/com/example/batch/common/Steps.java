package com.example.batch.common;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

/**
 * @author Nikola Ivačič <nikola.ivacic@dropchop.com> on 03. 06. 2026.
 */
public class Steps<P> implements Iterable<BatchStep<P>> {

  private final List<BatchStep<P>> steps;

  public Steps(List<BatchStep<P>> steps) {
    this.steps = steps;
  }

  public List<BatchStep<P>> get() {
    return steps;
  }

  public boolean isEmpty() {
    return steps.isEmpty();
  }

  @Override
  public Iterator<BatchStep<P>> iterator() {
    return steps.iterator();
  }

  public Stream<BatchStep<P>> stream() {
    return steps.stream();
  }
}
