package com.example.batch.common;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

@Produces(MediaType.APPLICATION_JSON)
public abstract class AbstractBatchControlResource<P> {
    protected abstract AbstractBatchService<P> service();

    @POST
    @Path("/start")
    public BatchStatus start() {
        return service().start();
    }

    @POST
    @Path("/stop")
    public BatchStatus stop() {
        return service().stop();
    }

    @GET
    @Path("/status")
    public BatchStatus status() {
        return service().status();
    }
}
