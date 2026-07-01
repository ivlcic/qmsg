package com.example.batch.common;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;

public interface BatchControlResource extends BatchResource {

  @POST
  @Path("/start")
  default BatchStatus start() {
    return getService().start();
  }

  @POST
  @Path("/stop")
  default BatchStatus stop() {
    return getService().stop();
  }

  @GET
  @Path("/status")
  default BatchStatus status() {
    return getService().status();
  }
}
