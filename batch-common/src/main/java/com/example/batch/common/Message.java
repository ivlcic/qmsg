package com.example.batch.common;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class Message<P> {
  private String action;
  private P payload;

  public Message() {
  }

  public String getAction() {
    return action;
  }

  public void setAction(String action) {
    this.action = action;
  }

  public P getPayload() {
    return payload;
  }

  public void setPayload(P payload) {
    this.payload = payload;
  }
}
