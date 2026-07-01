package com.example.batch.b.steps.publish;

import com.example.batch.b.BatchBData1;
import com.example.batch.common.BatchContext;
import com.example.batch.common.BatchStep;
import jakarta.enterprise.context.Dependent;
import org.jboss.logging.Logger;

@Dependent
public class BatchBPublishStep implements BatchStep<BatchBData1> {
  private static final Logger LOG = Logger.getLogger(BatchBPublishStep.class);

  @Override
  public void execute(BatchContext<BatchBData1> context) {
    LOG.infof("BatchB publish payload id=%s", context.payload().id());
  }
}
