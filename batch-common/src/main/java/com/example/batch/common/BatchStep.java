package com.example.batch.common;

/**
 * @author Nikola Ivačič <nikola.ivacic@dropchop.com> on 29. 06. 2026.
 */
public interface BatchStep<P> {
  void execute(BatchContext<P> context) throws Exception;
}
