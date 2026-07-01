package com.example.batch.a;

import com.example.batch.common.BatchStatusResource;
import com.example.batch.common.Message;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;

@Path("/batch-a/messages")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class BatchAResource implements BatchStatusResource {

  @Inject
  BatchAService service;

  public BatchAService getService() {
    return service;
  }

  @POST
  public Message<BatchAData> send(@QueryParam("action") String action, BatchAData payload) throws Exception {
    return service.emit(action, payload);
  }
}
