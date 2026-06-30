package com.example.batch.a;

import com.example.batch.a.steps.BatchACompleteStep;
import com.example.batch.a.steps.BatchAReadPayloadStep;
import com.example.batch.common.AbstractBatchService;
import com.example.batch.common.ActionStepTypes;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class BatchAService extends AbstractBatchService<BatchAData> {

    public BatchAService() {
        super(
            new ActionStepTypes<BatchAData>()
                .onDefault(
                    BatchAReadPayloadStep.class, BatchACompleteStep.class
                )
        );
    }
}
