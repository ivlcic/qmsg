package com.example.batch.b.steps.common;

import org.jboss.logging.Logger;

import com.example.batch.b.BatchBData1;
import com.example.batch.common.BatchContext;
import com.example.batch.common.BatchStep;

import jakarta.enterprise.context.Dependent;

@Dependent
public class BatchBDefaultStep implements BatchStep<BatchBData1> {
  private static final Logger LOG = Logger.getLogger(BatchBDefaultStep.class);

  @Override
  public void execute(BatchContext<BatchBData1> context) {
    BatchBData1 payload = context.payload();
    LOG.infof("BatchB processed default action id=%s category=%s active=%s",
        payload.id(), payload.category(), payload.active());
  }
}
