package com.example.batch.common;

import com.rabbitmq.client.AMQP;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class BatchContext<P> {
  private final String action;
  private final P payload;
  private final Optional<byte[]> rawBody;
  private final AMQP.BasicProperties properties;
  private final Instant receivedAt;
  private final Map<String, Object> attributes = new HashMap<>();

  public BatchContext(String action, P payload, Optional<byte[]> rawBody, AMQP.BasicProperties properties) {
    this.action = action;
    this.payload = payload;
    this.rawBody = rawBody;
    this.properties = properties;
    this.receivedAt = Instant.now();
  }

  public String action() {
    return action;
  }

  public P payload() {
    return payload;
  }

  public Optional<byte[]> rawBody() {
    return rawBody;
  }

  public String rawBodyAsString() {
    return rawBody
        .map(bytes -> new String(bytes, StandardCharsets.UTF_8))
        .orElse(null);
  }

  public AMQP.BasicProperties properties() {
    return properties;
  }

  public Instant receivedAt() {
    return receivedAt;
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
