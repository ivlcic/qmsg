package com.example.batch.b;

import com.example.batch.b.steps.BatchBArchiveStep;
import com.example.batch.b.steps.BatchBDefaultStep;
import com.example.batch.common.AbstractBatchService;
import com.example.batch.common.ActionStepTypes;
import com.example.batch.common.BatchStep;
import io.quarkus.arc.All;
import jakarta.enterprise.context.ApplicationScoped;

import java.util.List;

@ApplicationScoped
public class BatchBService extends AbstractBatchService<BatchBData> {

    @SuppressWarnings("CdiInjectionPointsInspection")
    public BatchBService(@All List<BatchStep<BatchBData>> availableSteps) {
        super(
            availableSteps,
            new ActionStepTypes<BatchBData>()
                .onDefault(
                    BatchBDefaultStep.class
                )
                .on(
                    "archive", BatchBArchiveStep.class
                )
        );
    }
}
