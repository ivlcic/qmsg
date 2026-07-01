package com.example.batch.common;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;

/**
 * @author Nikola Ivačič <nikola.ivacic@dropchop.com> on 01. 07. 2026.
 */
public interface BatchStatusResource extends BatchResource {

  @GET
  @Path("/status")
  default BatchStatus status() {
    return getService().status();
  }
}
