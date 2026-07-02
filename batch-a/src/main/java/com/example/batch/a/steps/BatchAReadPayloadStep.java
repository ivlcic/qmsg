package com.example.batch.a.steps;

import com.example.batch.a.BatchAData;
import com.example.batch.common.BatchContext;
import com.example.batch.common.BatchStep;
import jakarta.enterprise.context.Dependent;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Nikola Ivačič <nikola.ivacic@dropchop.com> on 29. 06. 2026.
 */
@Slf4j
@Dependent
public class BatchAReadPayloadStep implements BatchStep<BatchAData> {

  @Override
  public void execute(BatchContext<BatchAData> context) {
    BatchAData payload = context.getPayload();
    log.info("BatchA read payload id=[{}] name=[{}] amount=[{}]", payload.id(), payload.name(), payload.amount());
    context.put("payloadId", payload.id());
  }
}
