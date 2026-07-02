package com.example.batch.b.steps.delete;

import com.dropchop.recyclone.base.api.model.utils.ProfileTimer;
import com.example.batch.b.BatchBData2;
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
public class BatchBDeleteStep implements BatchStep<BatchBData2> {

  @Override
  public Result execute(BatchContext<BatchBData2> context) {
    ProfileTimer timer = new ProfileTimer();
    log.info("BatchB delete payload id=[{}]", context.getPayload().id());
    return proceed("The step consumed [{}]", timer.stop());
  }
}
