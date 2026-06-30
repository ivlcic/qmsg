package com.example.batch.a;

import com.example.batch.common.Message;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;

@Path("/batch-a/messages")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class BatchAEmitResource {
  @Inject
  BatchAPublisher publisher;

  @POST
  public Message<BatchAData> emit(@QueryParam("action") String action, BatchAData payload) {
    return publisher.publish(action, payload);
  }
}
