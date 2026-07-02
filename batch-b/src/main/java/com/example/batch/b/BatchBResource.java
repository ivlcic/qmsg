package com.example.batch.b;

import com.dropchop.recyclone.base.api.model.utils.Uuid;
import com.example.batch.common.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import lombok.Getter;

/**
 * @author Nikola Ivačič <nikola.ivacic@dropchop.com> on 01. 07. 2026.
 */
@Getter
@Path("/batch-b")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class BatchBResource implements BatchStatusResource {

  @Inject
  BatchBService service;

  @Inject
  ObjectMapper objectMapper;

  @POST
  @Path("/exec_async")
  public Message<BatchBData1> async(BatchBData1 payload) throws Exception {
    return service.emit(null, payload, new DefaultSerializer(objectMapper));
  }

  @POST
  @Path("/exec_async/{action : \\w{3,}}")
  public Message<BatchBData1> async(@PathParam("action") String action, BatchBData1 payload) throws Exception {
    return service.emit(action, payload, new DefaultSerializer(objectMapper));
  }

  @POST
  @Path("/exec/{action : \\w{3,}}")
  public void sync(@PathParam("action") String action, BatchBData2 payload) throws Exception {
    service.execute(action, Uuid.getTimeBased().toString(), payload);
  }
}
