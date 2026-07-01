package com.example.batch.common;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.Response;

/**
 * @author Nikola Ivačič <nikola.ivacic@dropchop.com> on 01. 07. 2026.
 */
public interface BatchStatusResource extends BatchResource {

  @GET
  @Path("/status")
  default BatchStatus status() {
    return getService().status();
  }

  @GET
  @Path("/healtz")
  default BatchStatus healtz() {
    return getService().status();
  }

  @GET
  @Path("/readyz")
  default Response readyz() {
    BatchStatus status = getService().status();
    if (status.state() == BatchServiceState.started) {
      return Response.ok(status).build();
    }
    return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity(status).build();
  }
}
