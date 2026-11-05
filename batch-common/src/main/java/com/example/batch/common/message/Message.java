package com.example.batch.common.message;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Getter;
import lombok.Setter;

import java.util.Objects;

/**
 * @author Nikola Ivačič <nikola.ivacic@dropchop.com> on 29. 06. 2026.
 */
@Getter
@Setter
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Message<P> {
  public record RetryMetadata(int retryCount, long totalDeferredMs, long lastDeferredMs) {
    public RetryMetadata {
      if (retryCount < 0 || lastDeferredMs < 0 || lastDeferredMs > totalDeferredMs) {
        throw new IllegalArgumentException("Invalid retry metadata");
      }
    }

    public RetryMetadata next(long delayMs) {
      if (delayMs <= 0) {
        throw new IllegalArgumentException("Retry delay must be positive");
      }
      return new RetryMetadata(Math.incrementExact(retryCount), Math.addExact(totalDeferredMs, delayMs), delayMs);
    }

  }

  public enum Disposition {
    ACK,
    RETRY,
    REJECT
  }

  public record Result(Disposition disposition, long deferMs, Throwable cause) {
    public Result {
      Objects.requireNonNull(disposition, "Message disposition is required");
      if (deferMs < 0) {
        throw new IllegalArgumentException("Message retry delay must not be negative");
      }
    }

    public static Result acknowledged() {
      return new Result(Disposition.ACK, 0, null);
    }

    public static Result retry(long deferMs, Throwable cause) {
      return new Result(Disposition.RETRY, deferMs, cause);
    }

    public static Result rejected(Throwable cause) {
      return new Result(Disposition.REJECT, 0, cause);
    }
  }

  @FunctionalInterface
  public interface Reader<M extends Message<P>, P> {
    M read(byte[] body) throws Exception;
  }

  @FunctionalInterface
  public interface Processor<M extends Message<P>, P> {
    Result process(M message) throws Exception;
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

  @JsonIgnore
  private RetryMetadata retryMetadata = new RetryMetadata(0, 0, 0);

  public Message() {
  }

  @JsonIgnore
  public int getRetryCount() {
    return retryMetadata.retryCount();
  }

  @JsonIgnore
  public long getTotalDeferredMs() {
    return retryMetadata.totalDeferredMs();
  }

  @JsonIgnore
  public long getLastDeferredMs() {
    return retryMetadata.lastDeferredMs();
  }
}
