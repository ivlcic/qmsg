package com.example.batch.common;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author Nikola Ivačič <nikola.ivacic@dropchop.com> on 30. 06. 2026.
 */
public class DefaultMapper {
  final ObjectMapper objectMapper;

  public DefaultMapper(ObjectMapper objectMapper) {
    this.objectMapper = objectMapper;
  }
}
