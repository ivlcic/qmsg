package com.example.batch.common.message.rabbitmq;

import com.example.batch.common.message.Message;
import com.example.batch.common.message.rabbitmq.RabbitMQReceiver.RetryHeaders;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Nikola Ivacic <nikola.ivacic@dropchop.com> on 05. 11. 2026.
 */
class RabbitMQRetryHeadersTest {
  @Test
  void absentHeadersMeanInitialDelivery() {
    assertEquals(new Message.RetryMetadata(0, 0, 0), RetryHeaders.read(null));
    assertEquals(new Message.RetryMetadata(0, 0, 0), RetryHeaders.read(Map.of()));
  }

  @Test
  void metadataRoundTripsThroughHeadersWithoutChangingOtherHeaders() {
    Map<String, Object> headers = new HashMap<>(Map.of("custom", "retained"));
    Message.RetryMetadata metadata = new Message.RetryMetadata(2, 20000, 15000);
    RetryHeaders.write(metadata, headers);
    assertEquals(metadata, RetryHeaders.read(headers));
    assertEquals("retained", headers.get("custom"));
    assertInstanceOf(Integer.class, headers.get(RetryHeaders.COUNT_HEADER));
    assertInstanceOf(Long.class, headers.get(RetryHeaders.TOTAL_DEFERRED_HEADER));
    assertInstanceOf(Long.class, headers.get(RetryHeaders.LAST_DEFERRED_HEADER));
  }

  @Test
  void malformedOrNegativeHeadersCannotResetTheRetryBudget() {
    assertThrows(IllegalArgumentException.class,
        () -> RetryHeaders.read(Map.of(RetryHeaders.COUNT_HEADER, "2")));
    assertThrows(IllegalArgumentException.class,
        () -> RetryHeaders.read(Map.of(RetryHeaders.COUNT_HEADER, 1.5)));
    assertThrows(IllegalArgumentException.class,
        () -> RetryHeaders.read(Map.of(RetryHeaders.TOTAL_DEFERRED_HEADER, -1L)));
    assertThrows(ArithmeticException.class,
        () -> RetryHeaders.read(Map.of(RetryHeaders.COUNT_HEADER, Long.MAX_VALUE)));
  }

  @Test
  void integralAmqpHeaderTypesAreAccepted() {
    for (Number count : new Number[]{(byte) 2, (short) 2, 2, 2L}) {
      assertEquals(new Message.RetryMetadata(2, 20000, 15000), RetryHeaders.read(Map.of(
          RetryHeaders.COUNT_HEADER, count,
          RetryHeaders.TOTAL_DEFERRED_HEADER, 20000,
          RetryHeaders.LAST_DEFERRED_HEADER, (short) 15000)));
    }
  }

  @Test
  void inconsistentDeferredTimesAreRejected() {
    assertThrows(IllegalArgumentException.class, () -> RetryHeaders.read(Map.of(
        RetryHeaders.COUNT_HEADER, 1,
        RetryHeaders.TOTAL_DEFERRED_HEADER, 100L,
        RetryHeaders.LAST_DEFERRED_HEADER, 200L)));
  }
}
