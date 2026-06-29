package com.example.batch.b;

import com.example.batch.common.AbstractRabbitBatchPublisher;

import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class BatchBPublisher extends AbstractRabbitBatchPublisher {
    @Override
    protected String queueName() {
        return "queue." + BatchBService.class.getSimpleName();
    }
}
