package com.example.batch.common;

import io.quarkiverse.rabbitmqclient.RabbitMQClient;
import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.inject.Produces;
import jakarta.enterprise.inject.spi.InjectionPoint;

@Dependent
public class BatchClientEmitterProducer {

  @Produces
  @Dependent
  @ForBatchService
  BatchClientEmitter create(InjectionPoint injectionPoint, RabbitMQClient rabbitMQClient) {
    ForBatchService qualifier = injectionPoint.getAnnotated().getAnnotation(ForBatchService.class);
    if (qualifier == null || BatchService.class.equals(qualifier.value())) {
      throw new IllegalStateException("BatchClientEmitter injection point must declare @ForBatchService");
    }
    return new BatchClientEmitter(qualifier.value().getSimpleName(), rabbitMQClient);
  }
}
