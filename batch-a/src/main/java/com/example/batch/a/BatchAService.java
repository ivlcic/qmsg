package com.example.batch.a;

import com.example.batch.a.steps.BatchACompleteStep;
import com.example.batch.a.steps.BatchAReadPayloadStep;
import com.example.batch.common.AbstractBatchService;
import jakarta.enterprise.context.ApplicationScoped;

import static com.example.batch.common.BatchService.byDefault;
import static com.example.batch.common.BatchService.with;
import static com.example.batch.common.BatchService.withRetryPolicy;
import static java.time.Duration.ofHours;
import static java.time.Duration.ofMinutes;
import static java.time.Duration.ofSeconds;

/**
 * @author Nikola Ivačič <nikola.ivacic@dropchop.com> on 29. 06. 2026.
 */
@ApplicationScoped
public class BatchAService extends AbstractBatchService {

  public BatchAService() {
    super(
        byDefault(
            with(BatchAData.class)
                .execute(
                    BatchAReadPayloadStep.class,
                    BatchACompleteStep.class
                ),
            withRetryPolicy()
                .maxRetries(7)
                .delays(ofSeconds(5), ofSeconds(15), ofSeconds(45),
                    ofMinutes(2), ofMinutes(5), ofMinutes(15), ofHours(1))
                .build()
        )
    );
  }
}
