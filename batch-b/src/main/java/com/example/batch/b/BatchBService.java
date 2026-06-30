package com.example.batch.b;

import com.example.batch.b.steps.BatchBArchiveStep;
import com.example.batch.b.steps.BatchBDefaultStep;
import com.example.batch.common.AbstractBatchService;
import jakarta.enterprise.context.ApplicationScoped;

import static com.example.batch.common.BatchService.byDefault;
import static com.example.batch.common.BatchService.with;

@ApplicationScoped
public class BatchBService extends AbstractBatchService {

  public BatchBService() {
    super(
        byDefault(
            with(BatchBData.class)
                .execute(BatchBDefaultStep.class)
        ).on(
            "archive",
            with(BatchBData.class)
                .execute(BatchBArchiveStep.class)
        )
    );
  }
}
