package com.example.batch.common;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Getter;
import lombok.Setter;

/**
 * @author Nikola Ivačič <nikola.ivacic@dropchop.com> on 29. 06. 2026.
 */
@Getter
@Setter
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

  private String id;
  private String action;
  private P payload;

  public Message() {
  }
}
