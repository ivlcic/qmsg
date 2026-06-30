package com.example.batch.a.steps;

import com.example.batch.a.BatchAData;
import com.example.batch.common.BatchContext;
import com.example.batch.common.BatchStep;
import jakarta.enterprise.context.Dependent;
import org.jboss.logging.Logger;

@Dependent
public class BatchACompleteStep implements BatchStep<BatchAData> {
  private static final Logger LOG = Logger.getLogger(BatchACompleteStep.class);

  @Override
  public void execute(BatchContext<BatchAData> context) {
    String payloadId = context.get("payloadId", String.class);
    LOG.infof("BatchA completed default action for payload id=%s", payloadId);
  }
}
