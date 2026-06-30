package com.example.batch.b;

import static com.example.batch.common.Actions.onDefault;

import com.example.batch.b.steps.BatchBArchiveStep;
import com.example.batch.b.steps.BatchBDefaultStep;
import com.example.batch.common.AbstractBatchService;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class BatchBService extends AbstractBatchService {

  public BatchBService() {
    super(
        onDefault(
            BatchBData.class,
            BatchBDefaultStep.class
        ).then(
            "archive", BatchBData.class, BatchBArchiveStep.class
        )
    );
  }
}
