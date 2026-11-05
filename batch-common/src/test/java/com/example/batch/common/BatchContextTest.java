package com.example.batch.common;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Kristijan Sečan <kristijan.secan@dropchop.com> on 16. 07. 2026.
 */
@QuarkusTest
class BatchContextTest {

  @Test
  void constructorCapturesActionTracePayloadAndAsyncFlag() {
    BatchContext<String> context = new BatchContext<>("archive", "trace-1", "data", true);
    assertEquals("archive", context.getAction());
    assertEquals("trace-1", context.getTraceId());
    assertEquals("data", context.getPayload());
    assertTrue(context.isAsync());
    assertNotNull(context.getCreated());
    assertEquals(0, context.getRetryCount());
    assertEquals(0, context.getTotalDeferredMs());
    assertEquals(0, context.getLastDeferredMs());
  }

  @Test
  void convenienceConstructorDefaultsToSynchronous() {
    BatchContext<String> context = new BatchContext<>("archive", "trace-1", "data");
    assertFalse(context.isAsync());
    assertEquals(0, context.getRetryCount());
    assertEquals(0, context.getTotalDeferredMs());
    assertEquals(0, context.getLastDeferredMs());
  }

  @Test
  void constructorCapturesRetryCountAndBothDeferredTimes() {
    BatchContext<String> context = new BatchContext<>("archive", "trace-1", "data", true, 2, 20000, 15000);
    assertEquals(2, context.getRetryCount());
    assertEquals(20000, context.getTotalDeferredMs());
    assertEquals(15000, context.getLastDeferredMs());
  }

  @Test
  void invalidRetryMetadataIsRejected() {
    assertThrows(IllegalArgumentException.class,
        () -> new BatchContext<>("archive", "trace-1", "data", true, -1, 0, 0));
    assertThrows(IllegalArgumentException.class,
        () -> new BatchContext<>("archive", "trace-1", "data", true, 1, 100, 200));
  }

  @Test
  void attributesRoundTripWithTypedGet() {
    BatchContext<String> context = new BatchContext<>(null, null, null);
    context.put("count", 7);
    assertEquals(7, context.get("count", Integer.class));
  }

  @Test
  void missingAttributeReturnsNull() {
    BatchContext<String> context = new BatchContext<>(null, null, null);
    assertNull(context.get("absent", String.class));
  }

  @Test
  void wrongAttributeTypeThrows() {
    BatchContext<String> context = new BatchContext<>(null, null, null);
    context.put("count", 7);
    assertThrows(IllegalArgumentException.class, () -> context.get("count", String.class));
  }
}
