package com.example.batch.a;

import com.example.batch.common.BatchClientEmitter;
import com.example.batch.common.ForBatchService;
import com.example.batch.common.Message;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;

import java.nio.charset.StandardCharsets;

@Path("/batch-a/messages")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class BatchAEmitResource {

  @Inject
  @ForBatchService(BatchAService.class)
  BatchClientEmitter emitter;

  @Inject
  ObjectMapper objectMapper;

  private Message.Serializer serializer() {
    return value -> objectMapper.writeValueAsString(value).getBytes(StandardCharsets.UTF_8);
  }

  @POST
  public Message<BatchAData> send(@QueryParam("action") String action, BatchAData payload) {
    Message<BatchAData> message = new Message<>();
    message.setAction(action);
    message.setPayload(payload);
    emitter.emit(message, serializer());
    return message;
  }
}
