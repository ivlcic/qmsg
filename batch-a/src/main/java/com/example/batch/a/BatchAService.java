package com.example.batch.a;

import com.example.batch.a.steps.BatchACompleteStep;
import com.example.batch.a.steps.BatchAReadPayloadStep;
import com.example.batch.common.AbstractBatchService;
import com.example.batch.common.ActionSteps;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class BatchAService extends AbstractBatchService {

    public BatchAService() {
        super(
            new ActionSteps()
                .onDefault(
                    BatchAReadPayloadStep.class, BatchACompleteStep.class
                )
        );
    }
}
