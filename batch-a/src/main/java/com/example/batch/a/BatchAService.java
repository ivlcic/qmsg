package com.example.batch.a;

import com.example.batch.a.steps.BatchACompleteStep;
import com.example.batch.a.steps.BatchAReadPayloadStep;
import com.example.batch.common.AbstractBatchService;
import com.example.batch.common.ActionStepTypes;
import com.example.batch.common.BatchStep;
import io.quarkus.arc.All;
import jakarta.enterprise.context.ApplicationScoped;

import java.util.List;

@ApplicationScoped
public class BatchAService extends AbstractBatchService<BatchAData> {

    @SuppressWarnings("CdiInjectionPointsInspection")
    public BatchAService(@All List<BatchStep<BatchAData>> availableSteps) {
        super(
            availableSteps,
            new ActionStepTypes<BatchAData>()
                .onDefault(
                    BatchAReadPayloadStep.class, BatchACompleteStep.class
                )
        );
    }
}
