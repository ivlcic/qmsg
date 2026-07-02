package com.example.batch.a.steps;

import com.example.batch.a.BatchAData;
import com.example.batch.common.BatchContext;
import com.example.batch.common.BatchStep;
import jakarta.enterprise.context.Dependent;
import lombok.extern.slf4j.Slf4j;

import static com.example.batch.common.BatchStep.proceed;

/**
 * @author Nikola Ivačič <nikola.ivacic@dropchop.com> on 29. 06. 2026.
 */
@Slf4j
@Dependent
public class BatchACompleteStep implements BatchStep<BatchAData> {

  @Override
  public Result execute(BatchContext<BatchAData> context) {
    String payloadId = context.get("payloadId", String.class);
    log.info("BatchB complete step payload id=[{}]", payloadId);
    return proceed();
  }
}
