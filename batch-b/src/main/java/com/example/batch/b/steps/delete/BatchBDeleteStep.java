package com.example.batch.b.steps.delete;

import com.example.batch.b.BatchBData2;
import com.example.batch.common.BatchContext;
import com.example.batch.common.BatchStep;
import jakarta.enterprise.context.Dependent;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Nikola Ivačič <nikola.ivacic@dropchop.com> on 01. 07. 2026.
 */
@Slf4j
@Dependent
public class BatchBDeleteStep implements BatchStep<BatchBData2> {

  @Override
  public void execute(BatchContext<BatchBData2> context) {
    log.info("BatchB delete payload id=[{}]", context.getPayload().id());
  }
}
