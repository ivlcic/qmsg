package com.example.batch.common;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.Return;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class BatchDeadLetterPublisherTest {
  private final BatchDeadLetterPublisher publisher = new BatchDeadLetterPublisher();
  private final List<String> events = new ArrayList<>();
  private final Delivery delivery = new Delivery(
      new Envelope(1, false, "", "queue.TestService"),
      new AMQP.BasicProperties.Builder()
          .contentType("application/json")
          .correlationId("request-1")
          .messageId("message-1")
          .expiration("1000")
          .userId("original-publisher")
          .headers(Map.of("custom", "retained"))
          .build(),
      new byte[]{0, 1, 2, 3}
  );
  private AMQP.BasicProperties publishedProperties;
  private byte[] publishedBody;
  private boolean returnMessage;
  private boolean failConfirmation;

  private static void set(Object target, String name, Object value) throws Exception {
    Field field = target.getClass().getDeclaredField(name);
    field.setAccessible(true);
    field.set(target, value);
  }

  @SuppressWarnings("unchecked")
  private AtomicReference<Return> returned() throws Exception {
    Field field = BatchDeadLetterPublisher.class.getDeclaredField("returned");
    field.setAccessible(true);
    return (AtomicReference<Return>) field.get(publisher);
  }

  @BeforeEach
  void setUp() throws Exception {
    Connection connection = (Connection) Proxy.newProxyInstance(
        Connection.class.getClassLoader(), new Class<?>[]{Connection.class}, (proxy, method, args) -> {
          if (method.getName().equals("isOpen")) {
            return true;
          }
          if (method.getName().equals("close")) {
            return null;
          }
          throw new UnsupportedOperationException(method.getName());
        });
    Channel channel = (Channel) Proxy.newProxyInstance(
        Channel.class.getClassLoader(), new Class<?>[]{Channel.class}, (proxy, method, args) -> {
          switch (method.getName()) {
            case "isOpen": return true;
            case "queueDeclare":
              assertEquals("queue.TestService.dlq", args[0]);
              assertEquals(true, args[1]);
              assertNull(args[4]);
              events.add("declare");
              return null;
            case "basicPublish":
              assertEquals("", args[0]);
              assertEquals("queue.TestService.dlq", args[1]);
              assertEquals(true, args[2], "publishing must be mandatory");
              publishedProperties = (AMQP.BasicProperties) args[3];
              publishedBody = (byte[]) args[4];
              events.add("publish");
              return null;
            case "waitForConfirmsOrDie":
              events.add("confirm");
              if (failConfirmation) {
                throw new TimeoutException("confirmation timeout");
              }
              if (returnMessage) {
                returned().set(new Return(312, "NO_ROUTE", "", "queue.TestService.dlq",
                    publishedProperties, publishedBody));
              }
              return null;
            case "close": return null;
            default: throw new UnsupportedOperationException(method.getName());
          }
        });
    set(publisher, "connection", connection);
    set(publisher, "channel", channel);
  }

  @Test
  void preservesRawPayloadAndMetadataAndWaitsForConfirmation() throws Exception {
    publisher.publish("queue.TestService", delivery, new IllegalArgumentException("invalid"));
    assertEquals(List.of("declare", "publish", "confirm"), events);
    assertSame(delivery.getBody(), publishedBody);
    assertEquals("request-1", publishedProperties.getCorrelationId());
    assertEquals("message-1", publishedProperties.getMessageId());
    assertEquals("application/json", publishedProperties.getContentType());
    assertEquals(2, publishedProperties.getDeliveryMode());
    assertEquals("retained", publishedProperties.getHeaders().get("custom"));
    assertEquals("queue.TestService", publishedProperties.getHeaders().get("x-batch-source-queue"));
    assertEquals("invalid", publishedProperties.getHeaders().get("x-batch-failure-message"));
    assertNull(publishedProperties.getExpiration());
    assertNull(publishedProperties.getUserId());
    assertEquals(Map.of("custom", "retained"), delivery.getProperties().getHeaders());
  }

  @Test
  void confirmationTimeoutIsPropagated() {
    failConfirmation = true;
    assertThrows(TimeoutException.class, () -> publisher.publish("queue.TestService", delivery, null));
  }

  @Test
  void returnedMessageIsNotReportedAsSuccessfullyForwarded() {
    returnMessage = true;
    assertThrows(IllegalStateException.class, () -> publisher.publish("queue.TestService", delivery, null));
  }
}
