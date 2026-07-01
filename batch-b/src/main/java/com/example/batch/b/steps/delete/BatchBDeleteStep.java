package com.example.batch.b.steps.delete;

import com.example.batch.b.BatchBData2;
import com.example.batch.common.BatchContext;
import com.example.batch.common.BatchStep;
import jakarta.enterprise.context.Dependent;
import org.jboss.logging.Logger;

@Dependent
public class BatchBDeleteStep implements BatchStep<BatchBData2> {
  private static final Logger LOG = Logger.getLogger(BatchBDeleteStep.class);

  @Override
  public void execute(BatchContext<BatchBData2> context) {
    LOG.infof("BatchB delete payload id=%s", context.payload().id());
  }
}
