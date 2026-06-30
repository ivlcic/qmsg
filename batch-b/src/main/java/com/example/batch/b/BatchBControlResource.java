package com.example.batch.b;

import com.example.batch.common.AbstractBatchControlResource;
import com.example.batch.common.AbstractBatchService;

import jakarta.inject.Inject;
import jakarta.ws.rs.Path;

@Path("/batch-b/control")
public class BatchBControlResource extends AbstractBatchControlResource {
  @Inject
  BatchBService service;

  @Override
  protected AbstractBatchService service() {
    return service;
  }
}
