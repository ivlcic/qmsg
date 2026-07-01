package com.example.batch.common;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class BatchContext<P> {
  private final String action;
  private final P payload;
  private final Instant created;
  private final Map<String, Object> attributes = new HashMap<>();

  public BatchContext(String action, P payload) {
    this.action = action;
    this.payload = payload;
    this.created = Instant.now();
  }

  public String action() {
    return action;
  }

  public P payload() {
    return payload;
  }

  public Instant created() {
    return created;
  }

  public Map<String, Object> attributes() {
    return attributes;
  }

  public void put(String key, Object value) {
    attributes.put(key, value);
  }

  @SuppressWarnings("unchecked")
  public <T> T get(String key, Class<T> type) {
    Object value = attributes.get(key);
    if (value == null) {
      return null;
    }
    return (T) value;
  }
}
