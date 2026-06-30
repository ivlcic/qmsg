package com.example.batch.b;

import static com.example.batch.common.BatchService.byDefault;

import com.example.batch.b.steps.BatchBArchiveStep;
import com.example.batch.b.steps.BatchBDefaultStep;
import com.example.batch.common.AbstractBatchService;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class BatchBService extends AbstractBatchService {

  public BatchBService() {
    super(
        byDefault(
            BatchBData.class,
            BatchBDefaultStep.class
        ).on(
            "archive", BatchBData.class, BatchBArchiveStep.class
        )
    );
  }
}
