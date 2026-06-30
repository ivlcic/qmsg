package com.example.batch.b;

import com.example.batch.b.steps.BatchBArchiveStep;
import com.example.batch.b.steps.BatchBDefaultStep;
import com.example.batch.common.AbstractBatchService;
import com.example.batch.common.ActionSteps;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class BatchBService extends AbstractBatchService {

  public BatchBService() {
    super(
        new ActionSteps()
            .onDefault(
                BatchBDefaultStep.class
            )
            .on(
                "archive", BatchBArchiveStep.class
            )
    );
  }
}
