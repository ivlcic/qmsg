package com.example.batch.b.steps.archive;

import com.example.batch.b.BatchBData1;
import com.example.batch.common.BatchContext;
import com.example.batch.common.BatchStep;
import jakarta.enterprise.context.Dependent;
import org.jboss.logging.Logger;

@Dependent
public class BatchBPrepStep implements BatchStep<BatchBData1> {
  private static final Logger LOG = Logger.getLogger(BatchBPrepStep.class);

  @Override
  public void execute(BatchContext<BatchBData1> context) {
    LOG.infof("BatchB archive prep payload id=%s", context.payload().id());
  }
}
