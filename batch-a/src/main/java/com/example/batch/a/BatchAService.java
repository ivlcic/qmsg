package com.example.batch.a;

import static com.example.batch.common.Actions.onDefault;

import com.example.batch.a.steps.BatchACompleteStep;
import com.example.batch.a.steps.BatchAReadPayloadStep;
import com.example.batch.common.AbstractBatchService;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class BatchAService extends AbstractBatchService {

    public BatchAService() {
        super(
            onDefault(
                BatchAData.class,
                BatchAReadPayloadStep.class, BatchACompleteStep.class
            )
        );
    }
}
