package com.example.batch.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Nikola Ivačič <nikola.ivacic@dropchop.com> on 16. 07. 2026.
 */
class MessageCodecTest {

  private final ObjectMapper objectMapper = new ObjectMapper();
  private final DefaultSerializer serializer = new DefaultSerializer(objectMapper);
  private final DefaultDeserializer<Object> deserializer = new DefaultDeserializer<>(objectMapper);

  @Test
  void serializedEnvelopeOmitsNullFields() throws Exception {
    Message<Map<String, Object>> message = new Message<>();
    message.setAction("archive");
    message.setPayload(Map.of("name", "test"));

    String json = new String(serializer.serialize(message), StandardCharsets.UTF_8);
    assertTrue(json.contains("\"action\":\"archive\""));
    assertTrue(json.contains("\"payload\""));
    assertFalse(json.contains("\"id\""), "null id must be omitted: " + json);
  }

  @Test
  void envelopeRoundTripsThroughSerializerAndDeserializer() throws Exception {
    Message<Map<String, Object>> message = new Message<>();
    message.setId("msg-1");
    message.setAction("archive");
    message.setPayload(Map.of("name", "test", "amount", 3));

    Message<Object> read = deserializer.deserialize(serializer.serialize(message));
    assertEquals("msg-1", read.getId());
    assertEquals("archive", read.getAction());
    assertInstanceOf(Map.class, read.getPayload());
    Map<?, ?> payload = (Map<?, ?>) read.getPayload();
    assertEquals("test", payload.get("name"));
    assertEquals(3, payload.get("amount"));
  }

  @Test
  void deserializeToleratesMissingActionAndPayload() throws Exception {
    Message<Object> read = deserializer.deserialize("{}".getBytes(StandardCharsets.UTF_8));
    assertNull(read.getAction());
    assertNull(read.getPayload());
    assertNull(read.getId());
  }

  @Test
  void deserializeRejectsInvalidJson() {
    assertThrows(Exception.class,
        () -> deserializer.deserialize("not-json".getBytes(StandardCharsets.UTF_8)));
  }
}
