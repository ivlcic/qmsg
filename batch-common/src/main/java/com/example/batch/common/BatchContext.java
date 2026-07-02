package com.example.batch.common;

import lombok.Getter;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Nikola Ivačič <nikola.ivacic@dropchop.com> on 29. 06. 2026.
 */
@Getter
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

  public void put(String key, Object value) {
    attributes.put(key, value);
  }

  @SuppressWarnings("unchecked")
  public <T> T get(String key, Class<T> type) {
    Object value = attributes.get(key);
    if (value == null) {
      return null;
    }
    if (type != null && type.isInstance(value)) {
      return (T) value;
    }
    throw new IllegalArgumentException(
        "Wrong attribute value [" + key + "] type [" + value.getClass() + "] for [" + type + "]"
    );
  }
}
