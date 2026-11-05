package com.example.batch.common.message.rabbitmq;

import com.example.batch.common.message.EmitterProducer;
import io.quarkiverse.rabbitmqclient.RabbitMQClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/**
 * @author Nikola Ivacic <nikola.ivacic@dropchop.com> on 05. 11. 2026.
 */
@ApplicationScoped
public class RabbitMQEmitterProducer implements EmitterProducer {
  @Inject
  @SuppressWarnings("CdiInjectionPointsInspection")
  RabbitMQClient rabbitMQClient;

  @Override
  public RabbitMQEmitter forService(String serviceName) {
    return new RabbitMQEmitter(serviceName, rabbitMQClient);
  }
}
