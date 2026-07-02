package com.example.batch.common;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import org.slf4j.event.Level;

import java.util.Arrays;

/**
 * @author Nikola Ivačič <nikola.ivacic@dropchop.com> on 29. 06. 2026.
 */
@SuppressWarnings("unused")
public interface BatchStep<P> {

  @Getter
  @Builder
  @AllArgsConstructor(access = AccessLevel.PRIVATE)
  class Result {
    public enum Process {
      STOP,
      CONTINUE
    }

    @Builder.Default
    private final Process process = Process.CONTINUE;
    private final Level logLevel;
    private final String logMessage;
    private final Object[] logArgs;
    private final Throwable cause;
    @Builder.Default
    private final boolean messageRetry = false;
    @Builder.Default
    private final long messageDeferMs = 0L;
  }

  private static Result.ResultBuilder resultBuilder(Result.Process process, boolean messageRetry) {
    return Result.builder()
        .process(process)
        .messageRetry(messageRetry);
  }

  private static Throwable trailingCause(Object[] logArgs) {
    if (logArgs == null || logArgs.length == 0) {
      return null;
    }
    Object last = logArgs[logArgs.length - 1];
    if (last instanceof Throwable throwable) {
      return throwable;
    }
    return null;
  }

  private static Object[] sanitizeLogArgs(Object[] logArgs) {
    if (logArgs == null || logArgs.length == 0) {
      return logArgs;
    }
    if (trailingCause(logArgs) == null) {
      return logArgs;
    }
    return Arrays.copyOf(logArgs, logArgs.length - 1);
  }

  private static Result.ResultBuilder resultBuilder(
      Result.Process process, boolean messageRetry, Level logLevel, String logMessage, Object... logArgs) {
    Throwable cause = trailingCause(logArgs);
    return resultBuilder(process, messageRetry)
        .logLevel(logLevel)
        .logMessage(logMessage)
        .logArgs(sanitizeLogArgs(logArgs))
        .cause(cause);
  }

  static Result.ResultBuilder proceeder() {
    return resultBuilder(Result.Process.CONTINUE, false);
  }

  static Result.ResultBuilder proceeder(String logMessage) {
    return proceeder(Level.INFO, logMessage);
  }

  static Result.ResultBuilder proceeder(String logMessage, Object... logArgs) {
    return proceeder(Level.INFO, logMessage, logArgs);
  }

  static Result.ResultBuilder proceeder(Level logLevel, String logMessage) {
    return proceeder(logLevel, logMessage, new Object[0]);
  }

  static Result.ResultBuilder proceeder(Level logLevel, String logMessage, Object... logArgs) {
    return resultBuilder(Result.Process.CONTINUE, false, logLevel, logMessage, logArgs);
  }

  static Result proceed() {
    return proceeder().build();
  }

  static Result proceed(String logMessage) {
    return proceeder(logMessage).build();
  }

  static Result proceed(String logMessage, Object... logArgs) {
    return proceeder(logMessage, logArgs).build();
  }

  static Result proceed(Level logLevel, String logMessage) {
    return proceeder(logLevel, logMessage).build();
  }

  static Result proceed(Level logLevel, String logMessage, Object... logArgs) {
    return proceeder(logLevel, logMessage, logArgs).build();
  }

  static Result.ResultBuilder stopper() {
    return resultBuilder(Result.Process.STOP, false);
  }

  static Result.ResultBuilder stopper(String logMessage, Object... logArgs) {
    return stopper(Level.WARN, logMessage, logArgs);
  }

  static Result.ResultBuilder stopper(Level logLevel, String logMessage, Object... logArgs) {
    return resultBuilder(Result.Process.STOP, false, logLevel, logMessage, logArgs);
  }

  static Result stop() {
    return stopper().build();
  }

  static Result stop(String logMessage) {
    return stopper(logMessage).build();
  }

  static Result stop(String logMessage, Object... logArgs) {
    return stopper(logMessage, logArgs).build();
  }

  static Result stop(Level logLevel, String logMessage) {
    return stopper(logLevel, logMessage).build();
  }

  static Result stop(Level logLevel, String logMessage, Object... logArgs) {
    return stopper(logLevel, logMessage, logArgs).build();
  }

  static Result.ResultBuilder retryer() {
    return resultBuilder(Result.Process.STOP, true);
  }

  static Result.ResultBuilder retryer(String logMessage) {
    return retryer(Level.WARN, logMessage);
  }

  static Result.ResultBuilder retryer(String logMessage, Object... logArgs) {
    return retryer(Level.WARN, logMessage, logArgs);
  }

  static Result.ResultBuilder retryer(Level logLevel, String logMessage) {
    return retryer(logLevel, logMessage, new Object[0]);
  }

  static Result.ResultBuilder retryer(Level logLevel, String logMessage, Object... logArgs) {
    return resultBuilder(Result.Process.STOP, true, logLevel, logMessage, logArgs);
  }

  static Result retry() {
    return retryer().build();
  }

  static Result retry(String logMessage) {
    return retryer(logMessage).build();
  }

  static Result retry(String logMessage, Object... logArgs) {
    return retryer(logMessage, logArgs).build();
  }

  static Result retry(Level logLevel, String logMessage) {
    return retryer(logLevel, logMessage).build();
  }

  static Result retry(Level logLevel, String logMessage, Object... logArgs) {
    return retryer(logLevel, logMessage, logArgs).build();
  }

  Result execute(BatchContext<P> context) throws Exception;
}
