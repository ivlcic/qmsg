package com.example.batch.common;

import org.slf4j.event.Level;

import static com.example.batch.common.BatchResult.failer;
import static com.example.batch.common.BatchResult.proceeder;
import static com.example.batch.common.BatchResult.retryer;
import static com.example.batch.common.BatchResult.stopper;

/**
 * @author Nikola Ivačič <nikola.ivacic@dropchop.com> on 29. 06. 2026.
 */
@SuppressWarnings("unused")
public interface BatchStep<P> {

  static BatchResult proceed(Level logLevel, String logMessage, Object... logArgs) {
    return proceeder(logLevel, logMessage, logArgs).build();
  }

  static BatchResult proceed(String logMessage, Object... logArgs) {
    return proceeder(logMessage, logArgs).build();
  }

  static BatchResult proceed() {
    return proceeder().build();
  }

  static BatchResult stop(Level logLevel, String logMessage, Object... logArgs) {
    return stopper(logLevel, logMessage, logArgs).build();
  }

  static BatchResult stop(String logMessage, Object... logArgs) {
    return stopper(logMessage, logArgs).build();
  }

  static BatchResult stop() {
    return stopper().build();
  }

  static BatchResult retry(Level logLevel, String logMessage, Object... logArgs) {
    return retryer(logLevel, logMessage, logArgs).build();
  }

  static BatchResult retry(String logMessage, Object... logArgs) {
    return retryer(logMessage, logArgs).build();
  }

  static BatchResult retry() {
    return retryer().build();
  }

  static BatchResult fail(Level logLevel, String logMessage, Object... logArgs) {
    return failer(logLevel, logMessage, logArgs).build();
  }

  static BatchResult fail(String logMessage, Object... logArgs) {
    return failer(logMessage, logArgs).build();
  }

  static BatchResult fail() {
    return failer().build();
  }

  BatchResult execute(BatchContext<P> context) throws Exception;
}
