package com.example.batch.b.steps;

import org.jboss.logging.Logger;

import com.example.batch.b.BatchBData;
import com.example.batch.common.BatchContext;
import com.example.batch.common.BatchStep;

import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class BatchBArchiveStep implements BatchStep<BatchBData> {
    private static final Logger LOG = Logger.getLogger(BatchBArchiveStep.class);

    @Override
    public String action() {
        return "archive";
    }

    @Override
    public int order() {
        return 10;
    }

    @Override
    public void execute(BatchContext<BatchBData> context) {
        LOG.infof("BatchB archived payload id=%s", context.payload().id());
    }
}
