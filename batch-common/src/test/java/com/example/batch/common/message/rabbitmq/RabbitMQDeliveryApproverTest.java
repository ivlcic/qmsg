package com.example.batch.common.message.rabbitmq;

import com.example.batch.common.RetryPolicy;
import com.example.batch.common.message.Message;
import com.example.batch.common.message.rabbitmq.RabbitMQReceiver.RetryHeaders;
import com.rabbitmq.client.*;
import io.quarkiverse.rabbitmqclient.RabbitMQClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static com.example.batch.common.RetryPolicy.DEFAULT;
import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Nikola Ivacic <nikola.ivacic@dropchop.com> on 05. 11. 2026.
 */
class RabbitMQDeliveryApproverTest {
  private final RabbitMQDeliveryApprover publisher = new RabbitMQDeliveryApprover();
  private final RetryPolicy retryPolicy = DEFAULT;
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
  private String destination = "queue.TestService.dlq";
  private String serverVersion = "4.4.0";
  private RabbitMQEmitter emitter;

  @SuppressWarnings("unchecked")
  private AtomicReference<Return> returned() throws Exception {
    Field field = RabbitMQDeliveryApprover.class.getDeclaredField("returned");
    field.setAccessible(true);
    return (AtomicReference<Return>) field.get(publisher);
  }

  @BeforeEach
  void setUp() {
    AtomicReference<Channel> publishingChannel = new AtomicReference<>();
    Connection connection = (Connection) Proxy.newProxyInstance(
        Connection.class.getClassLoader(),
        new Class<?>[]{Connection.class},
        (
            proxy, method, args) -> switch (method.getName()) {
              case "isOpen" -> true;
              case "close" -> null;
              case "getServerProperties" -> Map.of("version", serverVersion);
              case "createChannel" -> publishingChannel.get();
              default -> throw new UnsupportedOperationException(method.getName());
            }
        );
    Channel channel = (Channel) Proxy.newProxyInstance(
        Channel.class.getClassLoader(), new Class<?>[]{Channel.class}, (proxy, method, args) -> {
          switch (method.getName()) {
            case "isOpen": return true;
            case "getConnection": return connection;
            case "confirmSelect":
            case "addReturnListener": return null;
            case "queueDeclare":
              assertEquals(destination, args[0]);
              assertEquals(true, args[1]);
              if (destination.endsWith(".dlq")) {
                assertNull(args[4]);
              } else {
                assertEquals(Map.of("x-queue-type", "quorum"), args[4]);
              }
              events.add("declare");
              return null;
            case "basicPublish":
              assertEquals("", args[0]);
              assertEquals(destination, args[1]);
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
                returned().set(new Return(312, "NO_ROUTE", "", destination,
                    publishedProperties, publishedBody));
              }
              return null;
            case "close": return null;
            default: throw new UnsupportedOperationException(method.getName());
          }
        });
    publishingChannel.set(channel);
    RabbitMQEmitterProducer emitterProducer = new RabbitMQEmitterProducer();
    emitterProducer.rabbitMQClient = (RabbitMQClient) Proxy.newProxyInstance(
        RabbitMQClient.class.getClassLoader(), new Class<?>[]{RabbitMQClient.class},
        (proxy, method, args) -> {
          if (method.getName().equals("connect")) {
            return connection;
          }
          throw new UnsupportedOperationException(method.getName());
        });
    emitter = emitterProducer.forService("TestService");
    publisher.useEmitter(emitter);
  }

  @AfterEach
  void tearDown() {
    publisher.close();
    emitter.close();
  }

  @Test
  void preservesRawPayloadAndMetadataAndWaitsForConfirmation() throws Exception {
    publisher.deadLetter("queue.TestService", delivery, new IllegalArgumentException("invalid"));
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
    assertThrows(TimeoutException.class, () -> publisher.deadLetter("queue.TestService", delivery, null));
  }

  @Test
  void returnedMessageIsNotReportedAsSuccessfullyForwarded() {
    returnMessage = true;
    assertThrows(IllegalStateException.class, () -> publisher.deadLetter("queue.TestService", delivery, null));
  }

  private Delivery withMetadata(Message.RetryMetadata metadata) {
    Map<String, Object> headers = new HashMap<>(delivery.getProperties().getHeaders());
    RetryHeaders.write(metadata, headers);
    return new Delivery(delivery.getEnvelope(), delivery.getProperties().builder().headers(headers).build(), delivery.getBody());
  }

  @Test
  void retrySchedulesOriginalBodyInSameQuorumQueueAndUpdatesAllMetadata() throws Exception {
    destination = "queue.TestService";
    long before = System.currentTimeMillis();
    publisher.retry(destination, delivery, retryPolicy, 0, new IllegalStateException("temporary"));
    long after = System.currentTimeMillis();
    Map<String, Object> headers = publishedProperties.getHeaders();
    long readyAt = assertInstanceOf(Long.class, headers.get(RetryHeaders.DELIVERY_TIME_HEADER));
    assertTrue(readyAt >= before + 5000);
    assertTrue(readyAt <= after + 5000);
    assertEquals(new Message.RetryMetadata(1, 5000, 5000), RetryHeaders.read(headers));
    assertSame(delivery.getBody(), publishedBody);
    assertEquals("message-1", publishedProperties.getMessageId());
    assertEquals("request-1", publishedProperties.getCorrelationId());
    assertEquals(List.of("declare", "publish", "confirm"), events);
    assertEquals(Map.of("custom", "retained"), delivery.getProperties().getHeaders());
  }

  @Test
  void subsequentRetryAccumulatesDelayAndReplacesLastDelay() throws Exception {
    destination = "queue.TestService";
    publisher.retry(destination, withMetadata(new Message.RetryMetadata(1, 5000, 5000)), retryPolicy, 0, null);
    assertEquals(new Message.RetryMetadata(2, 20000, 15000),
        RetryHeaders.read(publishedProperties.getHeaders()));
  }

  @Test
  void explicitDelayOverridesDefaultSchedule() throws Exception {
    destination = "queue.TestService";
    publisher.retry(destination, withMetadata(new Message.RetryMetadata(2, 20000, 15000)), retryPolicy, 1234, null);
    assertEquals(new Message.RetryMetadata(3, 21234, 1234),
        RetryHeaders.read(publishedProperties.getHeaders()));
  }

  @Test
  void exhaustedRetriesGoToFinalDlqWithoutSchedulingAnotherDelivery() throws Exception {
    Delivery exhausted = withMetadata(new Message.RetryMetadata(7, 4985000, 3600000));
    publisher.retry("queue.TestService", exhausted, retryPolicy, 0, null);
    assertEquals(new Message.RetryMetadata(7, 4985000, 3600000),
        RetryHeaders.read(publishedProperties.getHeaders()));
    assertFalse(publishedProperties.getHeaders().containsKey(RetryHeaders.DELIVERY_TIME_HEADER));
    assertSame(delivery.getBody(), publishedBody);
  }

  @Test
  void finalDlqRemovesAnyPreviousDeliveryTime() throws Exception {
    Map<String, Object> headers = new HashMap<>(delivery.getProperties().getHeaders());
    headers.put(RetryHeaders.DELIVERY_TIME_HEADER, Long.MAX_VALUE);
    Delivery scheduled = new Delivery(delivery.getEnvelope(), delivery.getProperties().builder().headers(headers).build(), delivery.getBody());
    publisher.deadLetter("queue.TestService", scheduled, null);
    assertFalse(publishedProperties.getHeaders().containsKey(RetryHeaders.DELIVERY_TIME_HEADER));
  }

  @Test
  void deferredTotalOverflowIsDeadLetteredInsteadOfRepeatedlyRetried() throws Exception {
    publisher.retry("queue.TestService", withMetadata(new Message.RetryMetadata(1, Long.MAX_VALUE, 1)), retryPolicy, 5000, null);
    assertEquals(ArithmeticException.class.getName(), publishedProperties.getHeaders().get("x-batch-failure-type"));
    assertFalse(publishedProperties.getHeaders().containsKey(RetryHeaders.DELIVERY_TIME_HEADER));
  }

  @Test
  void retryConfirmationTimeoutPropagates() {
    destination = "queue.TestService";
    failConfirmation = true;
    assertThrows(TimeoutException.class, () -> publisher.retry(destination, delivery, retryPolicy, 5000, null));
  }

  @Test
  void unroutableRetryIsNotReportedAsSuccessful() {
    destination = "queue.TestService";
    returnMessage = true;
    assertThrows(IllegalStateException.class, () -> publisher.retry(destination, delivery, retryPolicy, 5000, null));
  }

  @Test
  void olderBrokerCannotSilentlyIgnoreScheduledDelivery() {
    destination = "queue.TestService";
    serverVersion = "4.3.9";
    IllegalStateException failure = assertThrows(IllegalStateException.class,
        () -> publisher.retry(destination, delivery, retryPolicy, 5000, null));
    assertTrue(failure.getMessage().contains("RabbitMQ 4.4+"));
    assertNull(publishedBody);
  }

  @Test
  void zeroRetriesInTheServicePolicyGoStraightToTheFinalDlq() throws Exception {
    publisher.retry("queue.TestService", delivery, new RetryPolicy(0, List.of(100L)), 0, null);
    assertFalse(publishedProperties.getHeaders().containsKey(RetryHeaders.DELIVERY_TIME_HEADER));
    assertEquals(new Message.RetryMetadata(0, 0, 0),
        RetryHeaders.read(publishedProperties.getHeaders()));
  }

  @Test
  void fallbackDelayComesFromTheSuppliedServicePolicy() throws Exception {
    destination = "queue.TestService";
    publisher.retry(destination, withMetadata(new Message.RetryMetadata(1, 100, 100)),
        new RetryPolicy(2, List.of(25L, 75L)), 0, null);
    assertEquals(new Message.RetryMetadata(2, 175, 75),
        RetryHeaders.read(publishedProperties.getHeaders()));
  }
}
