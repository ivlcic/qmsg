package com.example.batch.common.message;

/**
 * @author Nikola Ivacic <nikola.ivacic@dropchop.com> on 05. 11. 2026.
 */
@FunctionalInterface
public interface EmitterProducer {
  Emitter forService(String serviceName);
}
