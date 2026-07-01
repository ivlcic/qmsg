package com.example.batch.common;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class Message<P> {

  @FunctionalInterface
  public interface Reader<M extends Message<P>, P> {
    M read(byte[] body) throws Exception;
  }

  @FunctionalInterface
  public interface Processor<M extends Message<P>, P> {
    boolean process(M message) throws Exception;
  }

  @FunctionalInterface
  public interface Writer {
    void write(byte[] body) throws Exception;
  }

  @FunctionalInterface
  public interface Serializer {
    byte[] serialize(Message<?> message) throws Exception;
  }

  @FunctionalInterface
  public interface Deserializer<P> {
    Message<P> deserialize(byte[] body) throws Exception;
  }

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
