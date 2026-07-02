package com.example.batch.b.steps.common;

import com.example.batch.b.BatchBData1;
import com.example.batch.common.BatchContext;
import com.example.batch.common.BatchStep;
import jakarta.enterprise.context.Dependent;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Nikola Ivačič <nikola.ivacic@dropchop.com> on 29. 06. 2026.
 */
@Slf4j
@Dependent
public class BatchBDefaultStep implements BatchStep<BatchBData1> {

  @Override
  public void execute(BatchContext<BatchBData1> context) {
    BatchBData1 payload = context.getPayload();
    log.info(
        "BatchB processed default action id=[{}] category=[{}] active=[{}]",
        payload.id(), payload.category(), payload.active()
    );
  }
}
