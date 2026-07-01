package com.example.batch.b;

import com.example.batch.common.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;

import java.util.Optional;

@Path("/batch-b")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class BatchBResource implements BatchStatusResource {

  @Inject
  BatchBService service;

  @Inject
  ObjectMapper objectMapper;

  @Override
  public BatchBService getService() {
    return service;
  }

  @POST
  public Message<BatchBData1> async(@QueryParam("action") String action, BatchBData1 payload) throws Exception {
    return service.emit(action, payload, new DefaultSerializer(objectMapper));
  }

  @POST
  public void sync(@QueryParam("action") String action, BatchBData1 payload) throws Exception {
    service.execute(action, payload, Optional.empty());
  }

  @POST
  public Message<BatchBData2> async(@QueryParam("action") String action, BatchBData2 payload) throws Exception {
    return service.emit(action, payload, new DefaultSerializer(objectMapper));
  }

  @POST
  public void sync(@QueryParam("action") String action, BatchBData2 payload) throws Exception {
    service.execute(action, payload, Optional.empty());
  }
}
