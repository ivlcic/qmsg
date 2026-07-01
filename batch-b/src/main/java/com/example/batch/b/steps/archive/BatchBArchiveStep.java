package com.example.batch.b.steps.archive;

import org.jboss.logging.Logger;

import com.example.batch.b.BatchBData1;
import com.example.batch.common.BatchContext;
import com.example.batch.common.BatchStep;

import jakarta.enterprise.context.Dependent;

@Dependent
public class BatchBArchiveStep implements BatchStep<BatchBData1> {
  private static final Logger LOG = Logger.getLogger(BatchBArchiveStep.class);

  @Override
  public void execute(BatchContext<BatchBData1> context) {
    LOG.infof("BatchB archive payload id=%s", context.payload().id());
  }
}
