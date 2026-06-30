package com.example.batch.common;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;

/**
 * @author Nikola Ivačič <nikola.ivacic@dropchop.com> on 30. 06. 2026.
 */
public class DefaultSerializer extends DefaultMapper implements Message.Serializer {

  public DefaultSerializer(ObjectMapper objectMapper) {
    super(objectMapper);
  }

  @Override
  public byte[] serialize(Message<?> message) throws Exception {
    return objectMapper.writeValueAsString(message).getBytes(StandardCharsets.UTF_8);
  }
}
