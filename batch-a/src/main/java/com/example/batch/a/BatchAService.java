package com.example.batch.a;

import com.example.batch.a.steps.BatchACompleteStep;
import com.example.batch.a.steps.BatchAReadPayloadStep;
import com.example.batch.common.AbstractBatchService;
import jakarta.enterprise.context.ApplicationScoped;

import static com.example.batch.common.BatchService.byDefault;
import static com.example.batch.common.BatchService.with;

@ApplicationScoped
public class BatchAService extends AbstractBatchService {

  public BatchAService() {
    super(
        byDefault(
            with(BatchAData.class)
                .execute(
                    BatchAReadPayloadStep.class,
                    BatchACompleteStep.class
                )
        )
    );
  }
}
