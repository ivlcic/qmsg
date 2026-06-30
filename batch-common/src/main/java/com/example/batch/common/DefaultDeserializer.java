package com.example.batch.common;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author Nikola Ivačič <nikola.ivacic@dropchop.com> on 30. 06. 2026.
 */
public class DefaultDeserializer<P> extends DefaultMapper implements Message.Deserializer<P> {

  public DefaultDeserializer(ObjectMapper objectMapper) {
    super(objectMapper);
  }

  @Override
  public Message<P> deserialize(byte[] body) throws Exception {
    return objectMapper.readValue(
        body,
        new TypeReference<>() {
        }
    );
  }
}
