package com.example.batch.b;

import com.example.batch.common.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import lombok.Getter;

@Path("/batch-b/messages")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class BatchBEmitResource extends AbstractBatchMessageResource {

  @Getter
  @Inject
  @ForBatchService(BatchBService.class)
  BatchClientEmitter emitter;

  @Getter
  @Inject
  ObjectMapper objectMapper;

  @POST
  public Message<BatchBData> send(@QueryParam("action") String action, BatchBData payload) throws Exception {
    return super.send(action, payload, emitter, new DefaultSerializer(objectMapper));
  }
}
