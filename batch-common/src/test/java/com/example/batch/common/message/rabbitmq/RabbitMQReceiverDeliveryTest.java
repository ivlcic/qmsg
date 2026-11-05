package com.example.batch.common.message.rabbitmq;

import com.example.batch.common.BatchServiceException;
import com.example.batch.common.RetryPolicy;
import com.example.batch.common.message.Emitter;
import com.example.batch.common.message.Message;
import com.example.batch.common.message.Receiver;
import com.example.batch.common.message.rabbitmq.RabbitMQReceiver.RetryHeaders;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.Envelope;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Nikola Ivacic <nikola.ivacic@dropchop.com> on 05. 11. 2026.
 */
class RabbitMQReceiverDeliveryTest {
  private final List<String> events = new ArrayList<>();
  private final Message<Object> message = new Message<>();
  private final RetryPolicy retryPolicy = new RetryPolicy(2, List.of(100L, 200L));
  private Delivery delivery = new Delivery(
      new Envelope(7, false, "", "queue.TestService"),
      new AMQP.BasicProperties.Builder().correlationId("request-1").build(),
      "unreadable".getBytes(StandardCharsets.UTF_8)
  );

  private class DeadLetters extends RabbitMQDeliveryApprover {
    Delivery forwarded;
    Throwable cause;
    Exception failure;
    long requestedDelayMs;

    @Override
    public synchronized void close() {
    }

    @Override
    public synchronized void deadLetter(String queue, Delivery incoming, Throwable error) throws Exception {
      assertEquals("queue.TestService", queue);
      events.add("forward");
      if (failure != null) {
        throw failure;
      }
      forwarded = incoming;
      cause = error;
    }

    @Override
    public synchronized void retry(String queue, Delivery incoming, RetryPolicy policy,
                                   long delayMs, Throwable error) throws Exception {
      assertEquals("queue.TestService", queue);
      assertSame(retryPolicy, policy, "retry policy must come from the receiver controller");
      events.add("retry");
      if (failure != null) {
        throw failure;
      }
      forwarded = incoming;
      cause = error;
      requestedDelayMs = delayMs;
    }
  }

  private final RabbitMQReceiver receiver = new RabbitMQReceiver();
  private final DeadLetters deadLetters = new DeadLetters();
  private Channel channel;

  private static void set(Object target, String name, Object value) throws Exception {
    Field field = target.getClass().getDeclaredField(name);
    field.setAccessible(true);
    field.set(target, value);
  }

  private Channel channel(List<String> operations) {
    return (Channel) Proxy.newProxyInstance(Channel.class.getClassLoader(), new Class<?>[]{Channel.class},
        (proxy, method, args) -> {
          switch (method.getName()) {
            case "isOpen": return true;
            case "basicAck":
              assertEquals(7L, args[0]);
              operations.add("ack");
              return null;
            case "basicNack":
              operations.add("nack:" + args[2]);
              return null;
            case "close": return null;
            default: throw new UnsupportedOperationException(method.getName());
          }
        });
  }

  @BeforeEach
  void setUp() throws Exception {
    channel = channel(events);
    receiver.deliveryApprover = deadLetters;
    set(receiver, "channel", channel);
    set(receiver, "controller", new Receiver.Controller() {
      public String queueName() { return "queue.TestService"; }
      public RetryPolicy retryPolicy() { return retryPolicy; }
      public boolean isRetryForbidden() { return false; }
      public void clearConsumerTag() { }
      public void retryPending() { events.add("recover"); }
      public void retrySuccess() { }
      public void attemptStart() { }
    });
  }

  private void deliver(Message.Reader<Message<Object>, Object> reader,
                       Message.Processor<Message<Object>, Object> processor) throws Exception {
    Method method = RabbitMQReceiver.class.getDeclaredMethod(
        "handleDelivery", Channel.class, Delivery.class, Message.Reader.class, Message.Processor.class);
    method.setAccessible(true);
    method.invoke(receiver, channel, delivery, reader, processor);
  }

  @AfterEach
  void tearDown() {
    receiver.shutdownRetrier();
  }

  @Test
  void refusesAnEmitterFromAnotherTransportBeforeOpeningAnything() {
    Emitter emitter = (Emitter) Proxy.newProxyInstance(
        Emitter.class.getClassLoader(), new Class<?>[]{Emitter.class}, (proxy, method, args) -> {
          throw new AssertionError("incompatible emitter must not be used");
        });
    IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
        () -> receiver.open(null, emitter));
    assertTrue(failure.getMessage().contains("RabbitMQ emitter"));
    assertTrue(events.isEmpty());
  }

  @Test
  void successOnlyAcknowledges() throws Exception {
    deliver(body -> message, msg -> Message.Result.acknowledged());
    assertEquals(List.of("ack"), events);
    assertNull(deadLetters.forwarded);
  }

  @Test
  void unreadableMessageIsForwardedBeforeAcknowledgement() throws Exception {
    IOException failure = new IOException("bad encoding");
    deliver(body -> { throw failure; }, msg -> { throw new AssertionError("must not process"); });
    assertEquals(List.of("forward", "ack"), events);
    assertSame(delivery, deadLetters.forwarded);
    assertSame(failure, deadLetters.cause);
  }

  @Test
  void nullReadIsDeadLetteredWithoutCallingProcessor() throws Exception {
    deliver(body -> null, msg -> { throw new AssertionError("must not process"); });
    assertEquals(List.of("forward", "ack"), events);
    assertSame(delivery, deadLetters.forwarded);
  }

  @Test
  void unexpectedExceptionUsesTheSameDeadLetterFlowAsRejection() throws Exception {
    IllegalStateException failure = new IllegalStateException("unexpected processing failure");
    deliver(body -> message, msg -> { throw failure; });
    assertEquals(List.of("forward", "ack"), events);
    assertSame(failure, deadLetters.cause);
  }

  @Test
  void rejectedResultIsDeadLettered() throws Exception {
    RuntimeException failure = new IllegalArgumentException("invalid payload");
    deliver(body -> message, msg -> Message.Result.rejected(failure));
    assertEquals(List.of("forward", "ack"), events);
    assertSame(failure, deadLetters.cause);
  }

  @Test
  void nullProcessorResultIsPermanent() throws Exception {
    deliver(body -> message, msg -> null);
    assertEquals(List.of("forward", "ack"), events);
    assertInstanceOf(BatchServiceException.class, deadLetters.cause);
    assertTrue(deadLetters.cause.getMessage().contains("Message processor returned no result"));
  }

  @Test
  void rejectionWithoutCauseIsStillDeadLettered() throws Exception {
    deliver(body -> message, msg -> Message.Result.rejected(null));
    assertEquals(List.of("forward", "ack"), events);
    assertSame(delivery, deadLetters.forwarded);
    assertNull(deadLetters.cause);
  }

  @Test
  void checkedProcessingExceptionIsDeadLettered() throws Exception {
    IOException failure = new IOException("unexpected checked failure");
    deliver(body -> message, msg -> { throw failure; });
    assertEquals(List.of("forward", "ack"), events);
    assertSame(failure, deadLetters.cause);
  }

  @Test
  void explicitRetryRepublishesBeforeAcknowledgingOriginal() throws Exception {
    deliver(body -> message, msg -> Message.Result.retry(5000, null));
    assertEquals(List.of("retry", "ack"), events);
    assertSame(delivery, deadLetters.forwarded);
    assertEquals(5000, deadLetters.requestedDelayMs);
  }

  @Test
  void failedRetryPublishDoesNotSettleOriginalAndSchedulesRecovery() throws Exception {
    deadLetters.failure = new IOException("broker unavailable");
    deliver(body -> message, msg -> Message.Result.retry(5000, null));
    assertEquals(List.of("retry", "recover"), events);
    assertNull(deadLetters.forwarded);
  }

  @Test
  void processorReceivesAllRetryMetadataFromDeliveryHeaders() throws Exception {
    delivery = new Delivery(delivery.getEnvelope(), new AMQP.BasicProperties.Builder().headers(Map.of(
        RetryHeaders.COUNT_HEADER, 2,
        RetryHeaders.TOTAL_DEFERRED_HEADER, 20000L,
        RetryHeaders.LAST_DEFERRED_HEADER, 15000L
    )).build(), delivery.getBody());
    deliver(body -> message, msg -> {
      assertEquals(2, msg.getRetryCount());
      assertEquals(20000, msg.getTotalDeferredMs());
      assertEquals(15000, msg.getLastDeferredMs());
      return Message.Result.acknowledged();
    });
    assertEquals(List.of("ack"), events);
  }

  @Test
  void invalidRetryMetadataBypassesProcessorAndGoesToFinalDlq() throws Exception {
    delivery = new Delivery(delivery.getEnvelope(), new AMQP.BasicProperties.Builder().headers(Map.of(
        RetryHeaders.COUNT_HEADER, -1
    )).build(), delivery.getBody());
    deliver(body -> message, msg -> { throw new AssertionError("must not process invalid metadata"); });
    assertEquals(List.of("forward", "ack"), events);
    assertInstanceOf(IllegalArgumentException.class, deadLetters.cause);
  }

  @Test
  void failedDlqPublishDoesNotSettleOriginalAndSchedulesRecovery() throws Exception {
    deadLetters.failure = new IOException("broker unavailable");
    deliver(body -> null, msg -> Message.Result.acknowledged());
    assertEquals(List.of("forward", "recover"), events);
    assertNull(deadLetters.forwarded);
  }

  @Test
  void deliveryIsAcknowledgedOnItsOriginalChannel() throws Exception {
    List<String> replacementEvents = new ArrayList<>();
    set(receiver, "channel", channel(replacementEvents));
    deliver(body -> message, msg -> Message.Result.acknowledged());
    assertEquals(List.of("ack"), events);
    assertTrue(replacementEvents.isEmpty());
  }
}
