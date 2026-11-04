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
      CONTINUE,
      STOP,
      RETRY,
      FAIL
    }

    @Builder.Default
    private final Process process = Process.CONTINUE;
    private final Level logLevel;
    private final String logMessage;
    private final Object[] logArgs;
    private final Throwable cause;
    @Builder.Default
    private final long messageDeferMs = 0L;
  }

  private static Result.ResultBuilder resultBuilder(
      Result.Process process, Level logLevel, String logMessage, Object... logArgs) {
    Throwable cause = null;
    if (logArgs != null && logArgs.length > 0 && logArgs[logArgs.length - 1] instanceof Throwable throwable) {
      cause = throwable;
      logArgs = Arrays.copyOf(logArgs, logArgs.length - 1);
    }
    return Result.builder()
        .process(process)
        .logLevel(logLevel)
        .logMessage(logMessage)
        .logArgs(logArgs)
        .cause(cause);
  }

  static Result.ResultBuilder proceeder(Level logLevel, String logMessage, Object... logArgs) {
    return resultBuilder(Result.Process.CONTINUE, logLevel, logMessage, logArgs);
  }

  static Result.ResultBuilder proceeder(String logMessage, Object... logArgs) {
    return proceeder(Level.INFO, logMessage, logArgs);
  }

  static Result.ResultBuilder proceeder() {
    return proceeder(Level.INFO, null);
  }

  static Result proceed(Level logLevel, String logMessage, Object... logArgs) {
    return proceeder(logLevel, logMessage, logArgs).build();
  }

  static Result proceed(String logMessage, Object... logArgs) {
    return proceeder(logMessage, logArgs).build();
  }

  static Result proceed() {
    return proceeder().build();
  }

  static Result.ResultBuilder stopper(Level logLevel, String logMessage, Object... logArgs) {
    return resultBuilder(Result.Process.STOP, logLevel, logMessage, logArgs);
  }

  static Result.ResultBuilder stopper(String logMessage, Object... logArgs) {
    return stopper(Level.WARN, logMessage, logArgs);
  }

  static Result.ResultBuilder stopper() {
    return stopper(Level.WARN, null);
  }

  static Result stop(Level logLevel, String logMessage, Object... logArgs) {
    return stopper(logLevel, logMessage, logArgs).build();
  }

  static Result stop(String logMessage, Object... logArgs) {
    return stopper(logMessage, logArgs).build();
  }

  static Result stop() {
    return stopper().build();
  }

  static Result.ResultBuilder retryer(Level logLevel, String logMessage, Object... logArgs) {
    return resultBuilder(Result.Process.RETRY, logLevel, logMessage, logArgs);
  }

  static Result.ResultBuilder retryer(String logMessage, Object... logArgs) {
    return retryer(Level.WARN, logMessage, logArgs);
  }

  static Result.ResultBuilder retryer() {
    return retryer(Level.WARN, null);
  }

  static Result retry(Level logLevel, String logMessage, Object... logArgs) {
    return retryer(logLevel, logMessage, logArgs).build();
  }

  static Result retry(String logMessage, Object... logArgs) {
    return retryer(logMessage, logArgs).build();
  }

  static Result retry() {
    return retryer().build();
  }

  static Result.ResultBuilder failer(Level logLevel, String logMessage, Object... logArgs) {
    return resultBuilder(Result.Process.FAIL, logLevel, logMessage, logArgs);
  }

  static Result.ResultBuilder failer(String logMessage, Object... logArgs) {
    return failer(Level.ERROR, logMessage, logArgs);
  }

  static Result.ResultBuilder failer() {
    return failer(Level.ERROR, null);
  }

  static Result fail(Level logLevel, String logMessage, Object... logArgs) {
    return failer(logLevel, logMessage, logArgs).build();
  }

  static Result fail(String logMessage, Object... logArgs) {
    return failer(logMessage, logArgs).build();
  }

  static Result fail() {
    return failer().build();
  }

  Result execute(BatchContext<P> context) throws Exception;
}
