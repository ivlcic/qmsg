package com.example.batch.a.steps;

import org.jboss.logging.Logger;

import com.example.batch.a.BatchAData;
import com.example.batch.common.BatchContext;
import com.example.batch.common.BatchStep;

import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class BatchACompleteStep implements BatchStep<BatchAData> {
    private static final Logger LOG = Logger.getLogger(BatchACompleteStep.class);

    @Override
    public String action() {
        return "default";
    }

    @Override
    public int order() {
        return 20;
    }

    @Override
    public void execute(BatchContext<BatchAData> context) {
        String payloadId = context.get("payloadId", String.class);
        LOG.infof("BatchA completed default action for payload id=%s", payloadId);
    }
}
