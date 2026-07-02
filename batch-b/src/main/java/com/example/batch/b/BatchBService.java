package com.example.batch.b;

import com.example.batch.b.steps.archive.BatchBArchiveStep;
import com.example.batch.b.steps.common.BatchBDefaultStep;
import com.example.batch.b.steps.delete.BatchBDeleteStep;
import com.example.batch.b.steps.publish.BatchBPublishStep;
import com.example.batch.common.AbstractBatchService;
import jakarta.enterprise.context.ApplicationScoped;

import static com.example.batch.common.BatchService.byDefault;
import static com.example.batch.common.BatchService.with;

/**
 * @author Nikola Ivačič <nikola.ivacic@dropchop.com> on 29. 06. 2026.
 */
@ApplicationScoped
public class BatchBService extends AbstractBatchService {

  public BatchBService() {
    super(
        byDefault(
            with(BatchBData1.class)
                .execute(
                    BatchBDefaultStep.class,
                    BatchBPublishStep.class
                )
        ).on(
            "archive",
            with(BatchBData1.class)
                .execute(
                    BatchBArchiveStep.class
                )
        ).on(
            "delete",
            with(BatchBData2.class)
                .execute(
                    BatchBDeleteStep.class
                )
        )
    );
  }
}
