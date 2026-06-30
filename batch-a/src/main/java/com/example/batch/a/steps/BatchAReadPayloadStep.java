package com.example.batch.a.steps;

import com.example.batch.a.BatchAData;
import com.example.batch.common.BatchContext;
import com.example.batch.common.BatchStep;
import jakarta.enterprise.context.Dependent;
import org.jboss.logging.Logger;

@Dependent
public class BatchAReadPayloadStep implements BatchStep<BatchAData> {
  private static final Logger LOG = Logger.getLogger(BatchAReadPayloadStep.class);

  @Override
  public void execute(BatchContext<BatchAData> context) {
    BatchAData payload = context.payload();
    LOG.infof("BatchA read payload id=%s name=%s amount=%d", payload.id(), payload.name(), payload.amount());
    context.put("payloadId", payload.id());
  }
}
