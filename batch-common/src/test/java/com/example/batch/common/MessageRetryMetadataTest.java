package com.example.batch.common;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class MessageRetryMetadataTest {
  @Test
  void absentHeadersMeanInitialDelivery() {
    assertEquals(new Message.RetryMetadata(0, 0, 0), Message.RetryMetadata.fromHeaders(null));
    assertEquals(new Message.RetryMetadata(0, 0, 0), Message.RetryMetadata.fromHeaders(Map.of()));
  }

  @Test
  void scheduledRetriesRoundTripThroughHeadersAndAccumulateDelays() {
    Message.RetryMetadata first = new Message.RetryMetadata(0, 0, 0).next(5000);
    Map<String, Object> headers = new HashMap<>();
    first.writeHeaders(headers);
    Message.RetryMetadata second = Message.RetryMetadata.fromHeaders(headers).next(15000);
    second.writeHeaders(headers);
    assertEquals(new Message.RetryMetadata(2, 20000, 15000), Message.RetryMetadata.fromHeaders(headers));
    assertInstanceOf(Long.class, headers.get(Message.RetryMetadata.TOTAL_DEFERRED_HEADER));
    assertInstanceOf(Long.class, headers.get(Message.RetryMetadata.LAST_DEFERRED_HEADER));
  }

  @Test
  void malformedOrNegativeHeadersCannotResetTheRetryBudget() {
    assertThrows(IllegalArgumentException.class,
        () -> Message.RetryMetadata.fromHeaders(Map.of(Message.RetryMetadata.COUNT_HEADER, "2")));
    assertThrows(IllegalArgumentException.class,
        () -> Message.RetryMetadata.fromHeaders(Map.of(Message.RetryMetadata.COUNT_HEADER, 1.5)));
    assertThrows(IllegalArgumentException.class,
        () -> Message.RetryMetadata.fromHeaders(Map.of(Message.RetryMetadata.TOTAL_DEFERRED_HEADER, -1L)));
    assertThrows(ArithmeticException.class,
        () -> Message.RetryMetadata.fromHeaders(Map.of(Message.RetryMetadata.COUNT_HEADER, Long.MAX_VALUE)));
  }

  @Test
  void deferredTimeAndCountOverflowCannotWrapAround() {
    assertThrows(ArithmeticException.class, () -> new Message.RetryMetadata(1, Long.MAX_VALUE, 1).next(1));
    assertThrows(ArithmeticException.class, () -> new Message.RetryMetadata(Integer.MAX_VALUE, 1, 1).next(1));
    assertThrows(IllegalArgumentException.class, () -> new Message.RetryMetadata(1, 1, 1).next(0));
  }
}
