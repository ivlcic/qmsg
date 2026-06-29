package com.example.batch.b.steps;

import org.jboss.logging.Logger;

import com.example.batch.b.BatchBData;
import com.example.batch.common.BatchContext;
import com.example.batch.common.BatchStep;

import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class BatchBDefaultStep implements BatchStep<BatchBData> {
    private static final Logger LOG = Logger.getLogger(BatchBDefaultStep.class);

    @Override
    public String action() {
        return "default";
    }

    @Override
    public int order() {
        return 10;
    }

    @Override
    public void execute(BatchContext<BatchBData> context) {
        BatchBData payload = context.payload();
        LOG.infof("BatchB processed default action id=%s category=%s active=%s",
                payload.id(), payload.category(), payload.active());
    }
}
