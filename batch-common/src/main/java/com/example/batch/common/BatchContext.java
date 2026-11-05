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
  private final String traceId;
  private final P payload;
  private final boolean async;
  private final int retryCount;
  private final long totalDeferredMs;
  private final long lastDeferredMs;
  private final Instant created;
  private final Map<String, Object> attributes = new HashMap<>();

  public BatchContext(String action, String traceId, P payload, boolean async,
                      int retryCount, long totalDeferredMs, long lastDeferredMs) {
    if (retryCount < 0 || totalDeferredMs < 0 || lastDeferredMs < 0 || lastDeferredMs > totalDeferredMs) {
      throw new IllegalArgumentException("Invalid retry metadata");
    }
    this.action = action;
    this.traceId = traceId;
    this.payload = payload;
    this.async = async;
    this.retryCount = retryCount;
    this.totalDeferredMs = totalDeferredMs;
    this.lastDeferredMs = lastDeferredMs;
    this.created = Instant.now();
  }

  public BatchContext(String action, String traceId, P payload, boolean async) {
    this(action, traceId, payload, async, 0, 0, 0);
  }

  public BatchContext(String action, String traceId, P payload) {
    this(action, traceId, payload, false);
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
