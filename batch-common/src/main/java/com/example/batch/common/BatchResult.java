package com.example.batch.common;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import org.slf4j.event.Level;

import java.util.Arrays;

/**
 * @author Nikola Ivacic <nikola.ivacic@dropchop.com> on 05. 11. 2026.
 */
@Getter
@Builder
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public final class BatchResult {
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

  private static BatchResultBuilder resultBuilder(
      Process process, Level logLevel, String logMessage, Object... logArgs) {
    Throwable cause = null;
    if (logArgs != null && logArgs.length > 0 && logArgs[logArgs.length - 1] instanceof Throwable throwable) {
      cause = throwable;
      logArgs = Arrays.copyOf(logArgs, logArgs.length - 1);
    }
    return builder()
        .process(process)
        .logLevel(logLevel)
        .logMessage(logMessage)
        .logArgs(logArgs)
        .cause(cause);
  }

  public static BatchResultBuilder proceeder(Level logLevel, String logMessage, Object... logArgs) {
    return resultBuilder(Process.CONTINUE, logLevel, logMessage, logArgs);
  }

  public static BatchResultBuilder proceeder(String logMessage, Object... logArgs) {
    return proceeder(Level.INFO, logMessage, logArgs);
  }

  public static BatchResultBuilder proceeder() {
    return proceeder(Level.INFO, null);
  }

  public static BatchResultBuilder stopper(Level logLevel, String logMessage, Object... logArgs) {
    return resultBuilder(Process.STOP, logLevel, logMessage, logArgs);
  }

  public static BatchResultBuilder stopper(String logMessage, Object... logArgs) {
    return stopper(Level.WARN, logMessage, logArgs);
  }

  public static BatchResultBuilder stopper() {
    return stopper(Level.WARN, null);
  }

  public static BatchResultBuilder retryer(Level logLevel, String logMessage, Object... logArgs) {
    return resultBuilder(Process.RETRY, logLevel, logMessage, logArgs);
  }

  public static BatchResultBuilder retryer(String logMessage, Object... logArgs) {
    return retryer(Level.WARN, logMessage, logArgs);
  }

  public static BatchResultBuilder retryer() {
    return retryer(Level.WARN, null);
  }

  public static BatchResultBuilder failer(Level logLevel, String logMessage, Object... logArgs) {
    return resultBuilder(Process.FAIL, logLevel, logMessage, logArgs);
  }

  public static BatchResultBuilder failer(String logMessage, Object... logArgs) {
    return failer(Level.ERROR, logMessage, logArgs);
  }

  public static BatchResultBuilder failer() {
    return failer(Level.ERROR, null);
  }
}
