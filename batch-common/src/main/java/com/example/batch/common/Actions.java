package com.example.batch.common;

import static com.example.batch.common.BatchService.DEFAULT_ACTION;

import java.util.LinkedHashMap;

public class Actions extends LinkedHashMap<String, Action<?>> {

  public <P> Actions byDefault(BatchService.ActionDefinition<P> definition) {
    return on(DEFAULT_ACTION, definition);
  }

  public <P> Actions on(String name, BatchService.ActionDefinition<P> definition) {
    Action<P> action = new Action<>(name, definition);
    put(action.name(), action);
    return this;
  }
}
