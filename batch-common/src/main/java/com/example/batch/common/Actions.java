package com.example.batch.common;

import lombok.Getter;

import java.util.LinkedHashMap;
import java.util.Objects;

import static com.example.batch.common.BatchService.DEFAULT_ACTION;
import static com.example.batch.common.RetryPolicy.DEFAULT;

/**
 * @author Nikola Ivačič <nikola.ivacic@dropchop.com> on 30. 06. 2026.
 */
@Getter
public class Actions extends LinkedHashMap<String, Action<?>> {
  private RetryPolicy retryPolicy = DEFAULT;

  Actions withRetryPolicy(RetryPolicy retryPolicy) {
    this.retryPolicy = Objects.requireNonNull(retryPolicy, "Retry policy is required");
    return this;
  }

  public <P> Actions on(String name, BatchService.ActionDefinition<P> definition) {
    Action<P> action = new Action<>(name, definition);
    put(action.getName(), action);
    return this;
  }

  public <P> Actions byDefault(BatchService.ActionDefinition<P> definition) {
    return on(DEFAULT_ACTION, definition);
  }
}
