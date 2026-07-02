package com.example.batch.b.steps.archive;

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
public class BatchBArchiveStep implements BatchStep<BatchBData1> {

  @Override
  public void execute(BatchContext<BatchBData1> context) {
    log.info("BatchB archive payload id=[{}]", context.getPayload().id());
  }
}
