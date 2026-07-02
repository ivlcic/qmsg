package com.example.batch.b.steps.publish;

import com.example.batch.b.BatchBData1;
import com.example.batch.common.BatchContext;
import com.example.batch.common.BatchStep;
import jakarta.enterprise.context.Dependent;
import lombok.extern.slf4j.Slf4j;

import static com.example.batch.common.BatchStep.proceed;

/**
 * @author Nikola Ivačič <nikola.ivacic@dropchop.com> on 01. 07. 2026.
 */
@Slf4j
@Dependent
public class BatchBPublishStep implements BatchStep<BatchBData1> {

  @Override
  public Result execute(BatchContext<BatchBData1> context) {
    log.info("BatchB publish payload id=[{}]", context.getPayload().id());
    return proceed();
  }
}
