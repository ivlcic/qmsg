package com.example.batch.a.steps;

import org.jboss.logging.Logger;

import com.example.batch.a.BatchAData;
import com.example.batch.common.BatchContext;
import com.example.batch.common.BatchStep;

import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class BatchAReadPayloadStep implements BatchStep<BatchAData> {
    private static final Logger LOG = Logger.getLogger(BatchAReadPayloadStep.class);

    @Override
    public String action() {
        return "default";
    }

    @Override
    public int order() {
        return 10;
    }

    @Override
    public void execute(BatchContext<BatchAData> context) {
        BatchAData payload = context.payload();
        LOG.infof("BatchA read payload id=%s name=%s amount=%d", payload.id(), payload.name(), payload.amount());
        context.put("payloadId", payload.id());
    }
}
