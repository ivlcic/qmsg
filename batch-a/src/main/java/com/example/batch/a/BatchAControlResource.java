package com.example.batch.a;

import com.example.batch.common.AbstractBatchControlResource;
import com.example.batch.common.AbstractBatchService;

import jakarta.inject.Inject;
import jakarta.ws.rs.Path;

@Path("/batch-a/control")
public class BatchAControlResource extends AbstractBatchControlResource {
  @Inject
  BatchAService service;

  @Override
  protected AbstractBatchService service() {
    return service;
  }
}
