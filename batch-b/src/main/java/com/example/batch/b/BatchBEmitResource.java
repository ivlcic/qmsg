package com.example.batch.b;

import com.example.batch.common.Message;

import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;

@Path("/batch-b/messages")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class BatchBEmitResource {
    @Inject
    BatchBPublisher publisher;

    @POST
    public Message<BatchBData> emit(@QueryParam("action") String action, BatchBData payload) {
        return publisher.publish(action, payload);
    }
}
