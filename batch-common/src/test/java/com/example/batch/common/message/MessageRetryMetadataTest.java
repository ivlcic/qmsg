package com.example.batch.common.message;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Nikola Ivacic <nikola.ivacic@dropchop.com> on 05. 11. 2026.
 */
class MessageRetryMetadataTest {
  @Test
  void messagesStartWithZeroRetryMetadata() {
    assertEquals(new Message.RetryMetadata(0, 0, 0), new Message<>().getRetryMetadata());
  }

  @Test
  void scheduledRetriesAccumulateDelaysWithoutChangingThePreviousValue() {
    Message.RetryMetadata first = new Message.RetryMetadata(0, 0, 0).next(5000);
    Message.RetryMetadata second = first.next(15000);
    assertEquals(new Message.RetryMetadata(1, 5000, 5000), first);
    assertEquals(new Message.RetryMetadata(2, 20000, 15000), second);
  }

  @Test
  void invalidValuesAreRejectedWithoutAnyTransportHeaders() {
    assertThrows(IllegalArgumentException.class, () -> new Message.RetryMetadata(-1, 0, 0));
    assertThrows(IllegalArgumentException.class, () -> new Message.RetryMetadata(0, -1, 0));
    assertThrows(IllegalArgumentException.class, () -> new Message.RetryMetadata(0, 0, -1));
    assertThrows(IllegalArgumentException.class, () -> new Message.RetryMetadata(1, 100, 200));
  }

  @Test
  void deferredTimeAndCountOverflowCannotWrapAround() {
    assertThrows(ArithmeticException.class, () -> new Message.RetryMetadata(1, Long.MAX_VALUE, 1).next(1));
    assertThrows(ArithmeticException.class, () -> new Message.RetryMetadata(Integer.MAX_VALUE, 1, 1).next(1));
    assertThrows(IllegalArgumentException.class, () -> new Message.RetryMetadata(1, 1, 1).next(0));
  }
}
