package com.example.batch.b;

import com.example.batch.b.steps.BatchBArchiveStep;
import com.example.batch.b.steps.BatchBDefaultStep;
import com.example.batch.common.AbstractBatchService;
import com.example.batch.common.ActionStepTypes;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class BatchBService extends AbstractBatchService<BatchBData> {

    @Override
    protected ActionStepTypes<BatchBData> actionStepTypes() {
        return new ActionStepTypes<BatchBData>()
            .onDefault(
                BatchBDefaultStep.class
            )
            .on(
                "archive", BatchBArchiveStep.class
            );
    }
}
