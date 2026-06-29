package com.example.batch.a;

import com.example.batch.common.AbstractRabbitBatchPublisher;

import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class BatchAPublisher extends AbstractRabbitBatchPublisher {
    @Override
    protected String queueName() {
        return "queue." + BatchAService.class.getSimpleName();
    }
}
